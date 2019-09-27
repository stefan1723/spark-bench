package main.scala.de.ikt.vamos.bench.logging

import java.io.{File, PrintWriter}

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerStageCompleted}


/**
  * This class should be added as listener to a SparkContext
  * and will track the metrics of executed tasks.
  */
class CsvWritingLogListener extends LogSparkStatistics {
  var filepath: String = s"spark_log_${System.currentTimeMillis()}.csv"
  var pw: PrintWriter = _


  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val currStage = stageIdToStage.get(stageId)
    if(currStage.isEmpty)
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
    for((taskIndex,task) <- stage.tasks) {
      if(task.taskInfo.nonEmpty) {
        val runTime = executionStats.getOrElse(task.taskInfo.fold(-1L)(_.taskId).toString, 0L)
        val extendedTask =  ExtendedFlatTask(task, stage, runTime)
        pw.write(s"${extendedTask.toJson()}\n")
      }
    }
    removeStage(stage)
  }

  def flush(): Unit = {
    pw.flush()
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
//    super.onApplicationStart(applicationStart)
    pw = new PrintWriter(new File(filepath))
    pw.write(getCsvLabelLine())
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
//    super.onApplicationEnd(applicationEnd)
    pw.close()
  }
}

//object CsvWritingLogListener {
//  def apply(filepath: String): CsvWritingLogListener = {
//    val csvWriter = new CsvWritingLogListener
//    csvWriter.filepath = filepath
//    csvWriter.pw = new PrintWriter(new File(filepath))
//    csvWriter.pw.write(csvWriter.getCsvLabelLine())
//    csvWriter
//  }
//}