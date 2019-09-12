package main.scala.de.ikt.vamos.bench.scheduler

import com.ibm.sparktc.sparkbench.workload.{Suite, Workload}
import de.ikt.vamos.bench.distribution.DistributionBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.ForkJoinTaskSupport

class SplitMergeScheduler(val distribution: DistributionBase) extends SchedulerBase {
  override def run(suite: Suite, spark: SparkSession): Seq[DataFrame] = {
    completed = true
    println(s"Should run SplitMergeScheduler ${distribution.toString}")
    println(s"WARNING: This scheduler is not yet implemented!")
    val workloads = getWorkloadConfigs(suite)
    (0 until suite.repeat).flatMap { i =>
      // This will produce one DataFrame of one row for each workload in the sequence.
      // We're going to produce one coherent DF later from these
      val dfSeqFromOneRun: Seq[(DataFrame, Option[RDD[_]])] = {
        runWorkloads(suite.parallel, workloads, spark)
      }
      // Indicate which run of this suite this was.
      dfSeqFromOneRun.map(_._1).map(res => res.withColumn("run", lit(i)))
    }
  }
}
