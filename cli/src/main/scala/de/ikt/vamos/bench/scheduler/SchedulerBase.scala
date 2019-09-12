package main.scala.de.ikt.vamos.bench.scheduler

import com.ibm.sparktc.sparkbench.workload.{ConfigCreator, Suite, Workload}
import de.ikt.vamos.bench.distribution.DistributionBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.ForkJoinTaskSupport

/**
  * TODO: Change structure to store workload configs directly or make the run function static
  * because the multiple classes and objects doesn't make sense in current structure.
  * A possible refactorization would be to store the suite in the scheduler.
  */
trait SchedulerBase {
  var completed = false
  def run(suite: Suite, spark: SparkSession): Seq[DataFrame]

  def getWorkloadConfigs(suite: Suite): Seq[Workload] = suite.workloadConfigs.map(ConfigCreator
    .mapToConf)

  def isCompleted: Boolean = completed

  protected def runParallel(workloadConfigs: Seq[Workload], spark: SparkSession):
  Seq[(DataFrame, Option[RDD[_]])] = {
    val confSeqPar = workloadConfigs.par
    confSeqPar.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(confSeqPar.size))
    confSeqPar.map(_.run(spark, None)).seq
  }

  protected def runSerially(workloadConfigs: Seq[Workload], spark: SparkSession):
  Seq[(DataFrame, Option[RDD[_]])] = {
    workloadConfigs.map(_.run(spark, None))
  }

  protected def runWorkloads(parallel: Boolean, workloads: Seq[Workload],
                             spark: SparkSession): Seq[(DataFrame, Option[RDD[_]])] = {
    if (parallel) runParallel(workloads, spark)
    else runSerially(workloads, spark)
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
