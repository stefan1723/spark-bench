package main.scala.de.ikt.vamos.bench.scheduler

import com.ibm.sparktc.sparkbench.workload.{Suite, Workload}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.ForkJoinTaskSupport

class ParallelScheduler extends SchedulerBase {
  override def run(suite: Suite, spark: SparkSession): Seq[DataFrame] = {
    completed = true
    println("Should run ParallelScheduler")
    println(s"WARNING: This scheduler is not yet implemented!")
    val workloads = getWorkloadConfigs(suite)
    val confSeqPar = (0 until suite.repeat).par
    confSeqPar.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(confSeqPar.size))
    confSeqPar.flatMap(i => {
      val dfSeqFromOneRun: Seq[(DataFrame, Option[RDD[_]])] = {
        runWorkloads(suite.parallel, workloads, spark)
      }
      // Indicate which run of this suite this was.
      dfSeqFromOneRun.map(_._1).map(res => res.withColumn("run", lit(i)))
    }).seq
  }
}
