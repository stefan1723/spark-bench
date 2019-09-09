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
  override def run(suite: Suite, spark: SparkSession): Seq[DataFrame] = {
    println(s"Should run SingleQueueForkJoinScheduler with ${suite.repeat} repetitions")
    println(s"WARNING: This scheduler is not yet implemented!")
    val forkJoinPool = new java.util.concurrent.ForkJoinPool(threadsInPool)

    val workloads = getWorkloadConfigs(suite)

    var outRows = scala.collection.mutable.ListBuffer.empty[Seq[DataFrame]]
    var tasks = scala.collection.mutable.ListBuffer.empty[ForkJoinTask[Seq[DataFrame]]]
    var lastArrivalTime = 0L
    var totalInterarrivalTime = 0.0
    var interarrivalTime = 0.0
    var shouldArriveTime = 0.0

    (0 until suite.repeat).foreach { i =>
      lastArrivalTime = System.currentTimeMillis()

      val dfSeqFromOneRun: ForkJoinTask[Seq[DataFrame]] = ForkJoinTask.adapt(
            new java.util.concurrent.Callable[Seq[DataFrame]]() {
        def call(): Seq[DataFrame] = {
          val runNum = i
          val thisInterarrivalTime = interarrivalTime
          val thisArrivalTime = lastArrivalTime
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
    }
    forkJoinPool.awaitQuiescence(0, TimeUnit.DAYS)
    for (task <- tasks) {
      outRows += task.get()
    }
    outRows.toSeq.map(res => res.head)
  }
}
