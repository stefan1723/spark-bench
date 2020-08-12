package main.scala.de.ikt.vamos.bench.scheduler

import com.ibm.sparktc.sparkbench.workload.{Suite, Workload}
import de.ikt.vamos.bench.distribution.DistributionBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.ForkJoinTaskSupport

class ForkJoinScheduler(val distribution: DistributionBase) extends SchedulerBase {
  override def run(suite: Suite, spark: SparkSession): Seq[DataFrame] = {
    completed = true
    println(s"Should run ForkJoinScheduler ${distribution.toString}")
    println(s"WARNING: This scheduler is not yet implemented!")
    val workloads = getWorkloadConfigs(suite)
    Seq.empty[DataFrame]
  }
}
