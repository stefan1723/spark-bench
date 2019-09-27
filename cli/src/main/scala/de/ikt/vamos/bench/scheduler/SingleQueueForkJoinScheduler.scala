package main.scala.de.ikt.vamos.bench.scheduler

import java.util.concurrent.{ForkJoinTask, TimeUnit}

import breeze.linalg.max
import com.ibm.sparktc.sparkbench.workload.{Suite, Workload}
import de.ikt.vamos.bench.distribution.DistributionBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.ForkJoinTaskSupport

class SingleQueueForkJoinScheduler(val distribution: DistributionBase) extends SchedulerBase {
  val threadsInPool = 50
  // This values are object members to make it possible to call run and execute only a subset
  // of wanted repetitions. This is helpful to prevent out of memory errors on many repetitions.
  var lastArrivalTime = 0L
  var totalInterarrivalTime = 0.0
  var interarrivalTime = 0.0
  var shouldArriveTime = 0.0
  var completedRepetitions = 0
  val forkJoinPool = new java.util.concurrent.ForkJoinPool(threadsInPool)
  var tasks: scala.collection.mutable.ListBuffer[ForkJoinTask[Seq[DataFrame]]] =
    scala.collection.mutable.ListBuffer.empty[ForkJoinTask[Seq[DataFrame]]]


  override def run(suite: Suite, spark: SparkSession): Seq[DataFrame] = {
    println(s"Should run SingleQueueForkJoinScheduler with ${suite.repeat} repetitions")

    val workloads = getWorkloadConfigs(suite)

    val numOfRepetitions = if(suite.repeatBuf == -1) suite.repeat else
      Math.min(suite.repeatBuf, suite.repeat - completedRepetitions)
    (0 until numOfRepetitions).foreach { i =>
      lastArrivalTime = System.currentTimeMillis()
      val tmpRun = completedRepetitions
      val tmpinterarrivalTime = interarrivalTime
      val tmpThisArrivalTime = lastArrivalTime
      val dfSeqFromOneRun: ForkJoinTask[Seq[DataFrame]] = ForkJoinTask.adapt(
            new java.util.concurrent.Callable[Seq[DataFrame]]() {
        def call(): Seq[DataFrame] = {
          val runNum = tmpRun
          val thisInterarrivalTime = tmpinterarrivalTime
          val thisArrivalTime = tmpThisArrivalTime
          runWorkloads(suite.parallel, workloads, spark).map(_._1).map(res => res.withColumn
          ("run", lit(runNum)).withColumn("interarrivalTime", lit(thisInterarrivalTime))
          .withColumn("shouldArrive", lit(thisArrivalTime)))
        }
      })

      forkJoinPool.execute(dfSeqFromOneRun)
      tasks += dfSeqFromOneRun
      interarrivalTime = distribution.nextSample()
      shouldArriveTime = lastArrivalTime + interarrivalTime
      val sleepTime = (shouldArriveTime - System.currentTimeMillis()).toLong
      println(s"Should sleep ${sleepTime}ms, after handling job${i}")
      Thread.sleep(max(0L, sleepTime))
      // Indicate which run of this suite this was.
//      dfSeqFromOneRun.map(_._1).map(res => res.withColumn("run", lit(i)))
      completedRepetitions += 1
    }

    var outRows = scala.collection.mutable.ListBuffer.empty[Seq[DataFrame]]
    if (suite.repeatBuf == -1 || completedRepetitions >= suite.repeat) {
      forkJoinPool.awaitQuiescence(0, TimeUnit.DAYS)
      for (task <- tasks) {
        outRows += task.get()
      }
      completed = true
    } else {
      // TODO: Move this functionallity in the base class of schedulers. (Only for schedulers
      //  with arrival times)
      // Removes some of the already finished tasks to reduce the used memory. It's not
      // guaranteed that all finished tasks are removed because this queuing teqchnique allows
      // finishing not in placed.
      // At this point there is no waiting until all tasks in queue have finished because
      // this could cause wrong arrival times when running the next repetitions.
      var unfinishedTaskFound = false
      while (!unfinishedTaskFound && tasks.nonEmpty) {
        val task = tasks.head
        if (task.isDone) {
          outRows += task.get()
          tasks.remove(0)
        } else
          unfinishedTaskFound = true
      }
    }
    outRows.toSeq.map(res => res.head)
  }
}
