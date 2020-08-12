package main.scala.de.ikt.vamos.bench.logging

import java.io.{File, PrintWriter}

import org.apache.spark.scheduler._


/**
  * This class can be added as listener to a SparkContext
  * and will track the metrics of the executed tasks.
  * To use it in a spark-bench experiment you can add
  * "spark.extraListeners" = "main.scala.de.ikt.vamos.bench.logging.CsvWritingLogListener"
  * to the spark-submit-config configuration.
  */
class CsvWritingLogListener extends LogSparkStatistics {
  var filepath: String = s"spark_log_${System.currentTimeMillis()}.csv"
  var pw: PrintWriter = _


  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val currStage = stageIdToStage.get(stageId)
    if (currStage.isEmpty)
      println("Stage id unknown in onStageCompleted. Should be set at this position.")
    else {
      val stage = currStage.get
      stage.stageInfo = Some(stageCompleted.stageInfo)
      writeStageToFile(stage)
    }
  }

  def removeStage(stage: LogStage): Unit = {
    stageIdToStage.remove(stage.stageId)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobIdsToJobs.remove(jobEnd.jobId)
    jobIdToStageIds.remove(jobEnd.jobId)
  }

  def writeStageToFile(stage: LogStage): Unit = {
    val executionStats = this.executionTimeAccumulator.fold(Map.empty[String, Long])(_.value.stats)
    for (task <- stage.tasks) {
      if (task.taskInfo.nonEmpty) {
        val runTime = executionStats.getOrElse(task.taskInfo.fold(-1L)(_.taskId).toString, 0L)
        val extendedTask = ExtendedFlatTask(task, stage, runTime)
        pw.write(s"${extendedTask.toJson()}\n")
      }
    }
    removeStage(stage)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    val currStage = stageIdToStage.get(taskEnd.stageId)
    if (currStage.isEmpty) {
      writeTaskToFile(taskEnd)
    } else {
      super.onTaskEnd(taskEnd)
    }
  }

  /**
    * Writes the data of SparkListenerEnd to the csv file. This function should only get called if a
    * task ends which belong to a stage which is not available anymore. This usually happens if tasks can run
    * speculative and the same tasks runs more than once.
    *
    * @param taskEnd
    */
  def writeTaskToFile(taskEnd: SparkListenerTaskEnd): Unit = {
    val executionStats = this.executionTimeAccumulator.fold(Map.empty[String, Long])(_.value.stats)
    val runTime = executionStats.getOrElse(taskEnd.taskInfo.taskId.toString, 0L)
    val task = LogTask(taskEnd.stageId)
    task.taskInfo = Option(taskEnd.taskInfo)
    task.taskMetrics = Option(taskEnd.taskMetrics)
    task.endReason = s""""${taskEnd.reason.toString}""""
    // No job data is set in the stage here. Only tasks which results are used have job data set.
    val stage = LogStage(taskEnd.stageId)
    val extendedTask = ExtendedFlatTask(task, stage, runTime)
    pw.write(s"${extendedTask.toJson()}\n")
  }

  def flush(): Unit = {
    pw.flush()
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    pw = new PrintWriter(new File(filepath))
    pw.write(getCsvLabelLine())
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    pw.close()
  }
}