package main.scala.de.ikt.vamos.bench.scheduler

import com.ibm.sparktc.sparkbench.utils.SparkBenchException
import com.ibm.sparktc.sparkbench.workload.{ConfigCreator, Suite, Workload}
import de.ikt.vamos.bench.distribution.DistributionBase
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.parallel.ForkJoinTaskSupport

/**
  * TODO: Change structure to store workload configs directly or make the run function static
  * because the multiple classes and objects doesn't make sense in current structure.
  * A possible refactorization would be to store the suite in the scheduler.
  */
trait SchedulerBase {
  val MAX_NUMBER_OF_DATA_CREATION_TRIES = 5
  var completed = false
  def run(suite: Suite, spark: SparkSession): Seq[DataFrame]

  def getWorkloadConfigs(suite: Suite): Seq[Workload] = suite.workloadConfigs.map(ConfigCreator
    .mapToConf)

  def isCompleted: Boolean = completed

  protected def runParallel(workloadConfigs: Seq[Workload], spark: SparkSession, inDf: Option[DataFrame]):
  Seq[(DataFrame, Option[RDD[_]])] = {
    val confSeqPar = workloadConfigs.par
    confSeqPar.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(confSeqPar.size))
    confSeqPar.map(_.run(spark, inDf=inDf)).seq
  }

  protected def runSerially(workloadConfigs: Seq[Workload], spark: SparkSession, inDf: Option[DataFrame]):
  Seq[(DataFrame, Option[RDD[_]])] = {
    workloadConfigs.map(_.run(spark, inDf=inDf))
  }

  protected def runWorkloads(parallel: Boolean, workloads: Seq[Workload],
                             spark: SparkSession, inDf: Option[DataFrame] = None): Seq[(DataFrame, Option[RDD[_]])] = {
    if (parallel) runParallel(workloads, spark, inDf=inDf)
    else runSerially(workloads, spark, inDf=inDf)
  }

  // This function pushes the code to the executors. In order to do so it creates a task on each executor which returns
  // their executor id. Only if the number of different executor ids equals the number of executors we expect that
  // each executor got the code.
  // TODO Change to use ip and port because there can be more than one executor running on the same host.
  def pushCodeToExecutors(spark: SparkSession): Unit = {
    var numOfTries = 0
    val numOfInstances = spark.conf.get("spark.executor.instances").toInt
    val tasksPerExecutor = 1
    while (numOfTries < MAX_NUMBER_OF_DATA_CREATION_TRIES) {
      val rdd = spark.sparkContext.parallelize(0 until numOfInstances, numOfInstances).map(i => {
        val startTime = java.lang.System.currentTimeMillis()
        val targetStopTime = startTime + 100 * (numOfTries +1) // TIME_TO_CREATE_EMPTY_SLICE
        var x = 0
        val executorIds = SparkEnv.get.executorId
        while (java.lang.System.currentTimeMillis() < targetStopTime) {
          x += 1
        }
        Row(i, executorIds)
      })
      val executorIds = rdd.persist.collect
      val slicesPerHost = executorIds.groupBy(_(1)).mapValues(_.length)
      // print(hostnames)
      if (slicesPerHost.values.exists(_ != tasksPerExecutor)) {
        rdd.unpersist()
        numOfTries += 1
      } else
        return
    }
    throw SparkBenchException(s"Could not create data on all nodes after $numOfTries tries.")
  }
}

//trait SchedulerDefaults {
//  val name: String
//}

object SchedulerBase {
  def apply(schedulerName: String, m: Map[String, Any]): SchedulerBase = {
    schedulerName match {
      case "serial" => new SerialScheduler
      case "parallel" => new ParallelScheduler
      case "split-merge" => new SplitMergeScheduler(DistributionBase.createDistribution(m))
      case "fork-join" => new ForkJoinScheduler(DistributionBase.createDistribution(m))
      case "single-queue-fork-join" => new SingleQueueForkJoinScheduler(DistributionBase.createDistribution(m))
    }
  }
}
