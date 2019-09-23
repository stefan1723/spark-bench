package main.scala.de.ikt.vamos.bench.logging

import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.scheduler._

class LogSparkStatistics extends SparkListener {
  val stageCount: AtomicLong = new AtomicLong()
  val taskCount: AtomicLong = new AtomicLong()
  val jobCount: AtomicLong = new AtomicLong()
  val executorAddCount: AtomicLong = new AtomicLong()
  val executorRemoveCount: AtomicLong = new AtomicLong()

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = stageCount.incrementAndGet()

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = taskCount.incrementAndGet()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = jobCount.incrementAndGet()

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = executorAddCount.incrementAndGet()

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = executorRemoveCount.incrementAndGet()

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("**** MetricsSparkListener ****")
    println(s"stageCount=${stageCount.get()}")
    println(s"taskCount=${taskCount.get()}")
    println(s"jobCount=${jobCount.get()}")
    println(s"executorAddCount=${executorAddCount.get()}")
    println(s"executorRemoveCount=${executorRemoveCount.get()}")
  }
}
