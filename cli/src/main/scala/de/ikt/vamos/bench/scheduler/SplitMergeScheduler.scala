package main.scala.de.ikt.vamos.bench.scheduler

import java.util.concurrent.{Callable, ForkJoinTask, FutureTask, TimeUnit}

import breeze.linalg.max
import com.ibm.sparktc.sparkbench.utils.SparkBenchException
import com.ibm.sparktc.sparkbench.workload.{Suite, Workload}
import de.ikt.vamos.bench.distribution.DistributionBase
import main.scala.de.ikt.vamos.bench.utils.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorRemoved}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class SplitMergeScheduler(val distribution: DistributionBase) extends SparkListener with SchedulerBase {
  val MAX_NUMBER_OF_DATA_CREATION_TRIES = 5
  // Number of milliseconds to create one slice.
  // This is necessary to create the same number of slices on each executor.
  val TIME_TO_CREATE_EMPTY_SLICE = 100
  val threadsInPool = 1
  // This values are object members to make it possible to call run and execute only a subset
  // of wanted repetitions. This is helpful to prevent out of memory errors on many repetitions.
  var lastArrivalTime: Long = 0L
  var totalInterarrivalTime: Long = 0
  var interarrivalTime: Long = 1000
  var shouldArriveTime: Long = System.currentTimeMillis() + 10000
  var completedRepetitions = 0
  val forkJoinPool = new java.util.concurrent.ForkJoinPool(threadsInPool)
  var tasks: scala.collection.mutable.ListBuffer[Seq[DataFrame]] =
    scala.collection.mutable.ListBuffer.empty[Seq[DataFrame]]
  var executorLost = false
  val DATA_SCHEMA = new StructType()
    .add(StructField("id", IntegerType, true)).add(StructField("hostname", StringType, true))
  val e =  scala.concurrent.ExecutionContext.Implicits.global
  var sparkSession: Option[SparkSession] = None // Needed to kill jobs on executor failures

  override def run(suite: Suite, spark: SparkSession): Seq[DataFrame] = {
    sparkSession = Some(spark)
    println(s"Should run SplitMergeScheduler with ${suite.repeat} repetitions")
    SparkUtils.waitForExecutors(spark)
    spark.sparkContext.addSparkListener(this)
    executorLost = false
    val numOfRepetitions = if(suite.repeatBuf == -1) suite.repeat else
      Math.min(suite.repeatBuf, suite.repeat - completedRepetitions)

    runRepetitions(numOfRepetitions, suite, getWorkloadConfigs(suite), spark)

    spark.sparkContext.removeSparkListener(this)
    getResultsOfFinishedTasks(suite)
  }


  //    val dfSeqFromOneRun: ForkJoinTask[Seq[DataFrame]] = ForkJoinTask.adapt(
  //      new java.util.concurrent.Callable[Seq[DataFrame]]() {
  //        def call(): Seq[DataFrame] = {
  //          val runNum = tmpRun
  //          print(s"Starting run $runNum")
  //          val thisInterarrivalTime = tmpInterarrivalTime
  //          val thisArrivalTime = tmpThisArrivalTime
  //          val out = runWorkloads(suite.parallel, workloads, spark, inDf = inDf ).map(_._1).map(res => res.withColumn
  //          ("run", lit(runNum)).withColumn("interarrivalTime", lit(thisInterarrivalTime))
  //            .withColumn("shouldArrive", lit(thisArrivalTime)))
  //          print(s"Ending run $runNum")
  //          out
  //        }
  //      })
  //  forkJoinPool.execute(dfSeqFromOneRun)
  def runRepetitions(numOfRepetitions: Int, suite: Suite, workloads: Seq[Workload], spark: SparkSession): Unit = {
    var inDf: Option[DataFrame] = createDataFrameForLocality(spark, suite.slices)
    inDf = if(suite.forceDistr) inDf else None
    print(s"Created the dataframe")
    if (shouldArriveTime < System.currentTimeMillis()) {
      println("Arrival time first task in history. Setting new time.")
      shouldArriveTime = System.currentTimeMillis() + interarrivalTime
      // This can add a small delay depending on the os and the utilization of the CPU.
      // To reduce the delay active blocking could be used.
      Thread.sleep(max(0L, System.currentTimeMillis() - shouldArriveTime))
    }
    (0 until numOfRepetitions).foreach { i =>
      if(!executorLost) {
        //      lastArrivalTime = System.currentTimeMillis()
        val tmpRun = completedRepetitions
        val tmpInterarrivalTime = interarrivalTime
        val tmpThisArrivalTime = shouldArriveTime //lastArrivalTime

  //      val future = new FutureTask[Seq[DataFrame]](new Callable[Seq[DataFrame]]() {
  //        def call(): Seq[DataFrame] = {
  //
  //        }})

        print(s"Creating new job $i, should sleep ${(shouldArriveTime - System.currentTimeMillis())} while interarrival" +
          s"time is $interarrivalTime ")
        val fut = Future {
          runJobAndGetResult(tmpRun, tmpInterarrivalTime, tmpThisArrivalTime, suite, workloads, spark, inDf)(e)
        }(e)
        val creatingSparkJobTime = System.currentTimeMillis()
        var dfSeqFromOneRun: Option[Seq[DataFrame]] = None
        Try(Await.result(fut, Duration.Inf)) match {
          case Success(value) => dfSeqFromOneRun = Some(value)
          case Failure(exception) => print(s"An error occurred during running spark job. $exception")
        }
        val finishedJobTime = System.currentTimeMillis()
        print(s"Finished job $i in time ${finishedJobTime - creatingSparkJobTime}\n")
  //      val dfSeqFromOneRun: Seq[DataFrame] = Await.result(fut, Duration.Inf)
        if(dfSeqFromOneRun.nonEmpty) {
          tasks += dfSeqFromOneRun.get
          interarrivalTime = distribution.nextSample().toLong
          //      shouldArriveTime = lastArrivalTime + interarrivalTime
          shouldArriveTime += interarrivalTime
          val sleepTime = (shouldArriveTime - System.currentTimeMillis()).toLong
          //      println(s"Should sleep ${sleepTime}ms, after handling job$i")
          Thread.sleep(max(0L, sleepTime))
          // Indicate which run of this suite this was.
          //      dfSeqFromOneRun.map(_._1).map(res => res.withColumn("run", lit(i)))
          completedRepetitions += 1
        }
      }
      // TODO implement with future. Putting a timeout in blocking and check if spark job fails.
//      else {
//        // Lost an executor
//        spark.sparkContext.cancelAllJobs()
//        val numCancelledTasks = removeUnfinishedTasks()
//        spark.sparkContext.cancelAllJobs()
//        println(s"Lost tasks $numCancelledTasks")
//        if(inDf.isDefined)
//          inDf = if(suite.forceDistr) createDataFrameForLocality(spark, suite.slices) else None
//      }
    }
  }

  def runJobAndGetResult(runNum: Int, thisInterarrivalTime: Long, thisArrivalTime: Long, suite: Suite,
                         workloads: Seq[Workload], spark: SparkSession, inDf: Option[DataFrame])(implicit e: ExecutionContext): Seq[DataFrame] = {
//    val runNum = tmpRun
//    print(s"Starting run $runNum\n")
//    val thisInterarrivalTime = tmpInterarrivalTime
//    val thisArrivalTime = tmpThisArrivalTime
    val out = runWorkloads(suite.parallel, workloads, spark, inDf = inDf ).map(_._1).map(res => res.withColumn
    ("run", lit(runNum)).withColumn("interarrivalTime", lit(thisInterarrivalTime))
      .withColumn("shouldArrive", lit(thisArrivalTime)))
//    print(s"Ending run $runNum\n")
    out
  }

//  def removeUnfinishedTasks(): Int = {
//    var cancelledTasks = 0
////    val fjPoolRunning = forkJoinPool.getQueuedTaskCount
//    var firstTaskToCancel = -1
//    for(taskIdx <- tasks.indices) {
//      if (!tasks(taskIdx).isDone) {
//        if (firstTaskToCancel == -1)
//          firstTaskToCancel = taskIdx
//        val cancelled = tasks(taskIdx).cancel(true)
//        // TODO: figure out what to do if a task cannot get cancelled. Maybe throw an error?
//        if(!cancelled)
//          println("Could not cancel a task")
//        cancelledTasks += 1
//      }
//    }
//    tasks.remove(firstTaskToCancel, cancelledTasks)
//    cancelledTasks
//  }

  def createDataFrameForLocality(spark: SparkSession, numOfTasks: Int): Option[DataFrame] = {
    var numOfTries = 0
    val numOfInstances = spark.conf.get("spark.executor.instances").toInt
    val tasksPerExecutor = numOfTasks / numOfInstances
    while (numOfTries < MAX_NUMBER_OF_DATA_CREATION_TRIES) {
      val rdd = spark.sparkContext.parallelize(0 until numOfTasks, numOfTasks).map(i => {
        val startTime = java.lang.System.currentTimeMillis()
        val targetStopTime = startTime + 100 * (numOfTries +1) // TIME_TO_CREATE_EMPTY_SLICE
        var x = 0
        val hostname = java.net.InetAddress.getLocalHost.getHostName
        while (java.lang.System.currentTimeMillis() < targetStopTime) {
          x += 1
        }
        Row(i, hostname)
      })
      val hostnames = rdd.persist.collect
      val slicesPerHost = hostnames.groupBy(_(1)).mapValues(_.length)
      // print(hostnames)
      if (slicesPerHost.values.exists(_ != tasksPerExecutor)) {
        rdd.unpersist()
        numOfTries += 1
      } else
        return Some(spark.createDataFrame(rdd, DATA_SCHEMA))
    }
    throw SparkBenchException(s"Could not create data on all nodes after $numOfTries tries.")
  }

  def checkIfNumOfInstancesIsAvailable(spark: SparkSession): Boolean = {
    val numOfInstances = spark.conf.get("spark.executor.instances").toInt
    spark.sparkContext.getExecutorMemoryStatus.toList.length - 1
    true
  }

  /**
    * Returns the results of already finished jobs if there are unscheduled jobs left.
    * This means that scheduled jobs which did not already finish are ignored and the
    * result must be fetched later.
    * If all jobs are scheduled this function blocks until all they are finished and
    * returns the result.
    * @param suite
    * @return
    */
  def getResultsOfFinishedTasks(suite: Suite): Seq[DataFrame] = {
    var outRows = scala.collection.mutable.ListBuffer.empty[DataFrame]
//    // The job to store the results is also a spark job so this function can also block
//    forkJoinPool.awaitQuiescence(0, TimeUnit.DAYS)
//    for (task <- tasks) {
//      outRows += task.get()
//    }
    if (suite.repeatBuf == -1 || completedRepetitions >= suite.repeat) {
      completed = true
    }
//    else {
//      // TODO: Move this functionality in the base class of schedulers. (Only for schedulers
//      //  with arrival times)
//      // Removes some of the already finished tasks to reduce the used memory. It's not
//      // guaranteed that all finished tasks are removed because this queuing teqchnique allows
//      // finishing not in placed.
//      // At this point there is no waiting until all tasks in queue have finished because
//      // this could cause wrong arrival times when running the next repetitions.
//      var unfinishedTaskFound = false
//      while (!unfinishedTaskFound && tasks.nonEmpty) {
//        val task = tasks.head
//        if (task.isDone) {
//          outRows += task.get()
//          tasks.remove(0)
//        } else
//          unfinishedTaskFound = true
//      }
//    }
//    outRows.map(res => res.head)
    outRows = tasks.map(res => res.head)
    tasks.clear()
    outRows
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    executorLost = true
    print(s"The executor with id ${executorRemoved.executorId} got removed with reason ${executorRemoved.reason}.\n")
    if(sparkSession.nonEmpty) {
      sparkSession.get.sparkContext.cancelAllJobs()
      print(s"Cancelling all jobs and restart with a new executor\n")
    }
    else {
      print(s"Could not cancel spark jobs. No spark session is set.\n")
    }
  }


//  class MyThread extends Runnable
//  {
//    override def run()
//    {
//      // Displaying the thread that is running
//      println("Thread " + Thread.currentThread().getName() +
//        " is running.")
//    }
//  }
}
