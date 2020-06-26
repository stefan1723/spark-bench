package main.scala.de.ikt.vamos.bench.logging

//import org.apache.spark.executor.{InputReadData, TaskMetrics}
//import org.apache.spark.executor.InputReadData
import org.apache.spark.scheduler._
import org.apache.spark.executor.TaskMetrics


case class FlatTask(taskIndex:Long, taskId:Long, stageId:Long, host:String, local:Boolean,
                    sojournTime:Double, waitingTime:Double, serviceTime:Double,
                    deserializationTime:Double, schedulerOverhead:Double,
                    runtime:Double, readTime:Double, locationExecId:String,
                    readType:String, cachedPartition:Boolean) {
  override def toString:String = {
    ("taskIndex:"+taskIndex+" taskId:"+taskId+" stageId:"+stageId+" host:"+host+" local:"+local+" sojournTime:"+sojournTime
      +" waitingTime:"+waitingTime+" serviceTime:"+serviceTime+" deserializationTime:"+deserializationTime
      +" schedulerOverhead:"+schedulerOverhead+"runtime:"+runtime)
  }
}

case class ExtendedFlatTask(stageSubmissionTime: Long, launchTime:Long, finishTime: Long,
                            jobId: Long,
                            status: String,
                            stageId:Long, name: String, taskId:Long,
                            taskIndex:Long, attempt: Int, executorId: String, duration: Double, durationNs: Double,
                            sojournTime: Double, waitingTime:Double, taskLocality: String,
                            executorDeserializationTime:Double, executorRunTime:Double,
                            executorCpuTime: Double, executorDeserializeCpuTime: Double,
                            resultSize: Long, gettingResultTime: Double, jvmGcTime: Long,
                            resultSerializationTime: Double, memoryBytesSpilled: Long,
                            diskBytesSpilled: Double, peakExecutionMemory:Long, bytesRead:Long,
                            recordsRead: Long, readTime: Double, locationExecId: String,
                            readMethod: String, cachedBlock: Boolean, bytesWritten: Long,
                            recordsWritten: Long, shuffleRemoteBlocksFetched: Long,
                            shuffleLocalBlocksFetched: Long, shuffleFetchWaitTime:Long,
                            remoteBytesRead: Long, shuffleRemoteBytesReadToDisk: Long,
                            shuffleLocalBytesRead:Long, shuffleRecordsRead: Long,
                            shuffleBytesWritten: Long, shuffleWriteTime:Long,
                            shuffleRecordsWritten: Long,stageCompletionTime: Double,
                            measuredRunTime: Long, endReason: String) {

  def toJson(): String = {
    this.productIterator.map{
      case Some(value) => value
      case None => ""
      case rest => rest
    }.mkString(",")
  }
}

object ExtendedFlatTask {
  def apply(task: LogTask, stage: LogStage, runTime: Long): ExtendedFlatTask = {
    ExtendedFlatTask(
      task.submissionTime.getOrElse(0L),
      task.taskInfo.fold(0L)(_.launchTime),
      task.taskInfo.fold(0L)(_.finishTime),
      stage.job.fold(-1)(_.jobId),
      "-", stage.stageId, stage.stageInfo.map(_.name).getOrElse("Not defined"),
      task.taskInfo.fold(-1L)(_.taskId), task.taskInfo.fold(-1L)(_.index),
      stage.stageInfo.fold(-1)(_.attemptNumber()),
      task.taskInfo.fold("-1")(_.executorId),
      task.taskInfo.fold(0L)(_.duration),
      task.taskInfo.fold(0L)(_.duration),
      stage.stageInfo.fold(0L)(_.completionTime.getOrElse(0L)) - task.submissionTime.getOrElse(0L),
      task.taskInfo.fold(0L)(_.launchTime) - task.submissionTime.getOrElse(0L),
      task.taskInfo.fold("Not set")(_.taskLocality.toString),
      task.taskMetrics.fold(0L)(_.executorDeserializeTime),
      task.taskMetrics.fold(0L)(_.executorRunTime),
      task.taskMetrics.fold(0L)(_.executorCpuTime),
      task.taskMetrics.fold(0L)(_.executorDeserializeCpuTime),
      task.taskMetrics.fold(0L)(_.resultSize),
      task.taskInfo.fold(0L)(_.gettingResultTime),
      0L,
      task.taskMetrics.fold(0L)(_.resultSerializationTime),
      task.taskMetrics.fold(0L)(_.memoryBytesSpilled),
      task.taskMetrics.fold(0L)(_.diskBytesSpilled),
      task.taskMetrics.fold(0L)(_.peakExecutionMemory),
      task.taskMetrics.fold(0L)(_.inputMetrics.bytesRead),
      task.taskMetrics.fold(0L)(_.inputMetrics.recordsRead),
      0.0,
      "-1",
      "-1",
      true,
      task.taskMetrics.fold(0L)(_.outputMetrics.bytesWritten),
      task.taskMetrics.fold(0L)(_.outputMetrics.recordsWritten),
      task.taskMetrics.fold(0L)(_.shuffleReadMetrics.remoteBlocksFetched),
      task.taskMetrics.fold(0L)(_.shuffleReadMetrics.localBlocksFetched),
      task.taskMetrics.fold(0L)(_.shuffleReadMetrics.fetchWaitTime),
      task.taskMetrics.fold(0L)(_.shuffleReadMetrics.remoteBytesRead),
      task.taskMetrics.fold(0L)(_.shuffleReadMetrics.remoteBytesReadToDisk),
      task.taskMetrics.fold(0L)(_.shuffleReadMetrics.localBytesRead),
      task.taskMetrics.fold(0L)(_.shuffleReadMetrics.recordsRead),
      task.taskMetrics.fold(0L)(_.shuffleWriteMetrics.bytesWritten),
      task.taskMetrics.fold(0L)(_.shuffleWriteMetrics.writeTime),
      task.taskMetrics.fold(0L)(_.shuffleWriteMetrics.recordsWritten),
      stage.stageInfo.fold(-1.0)(_.completionTime.fold(-1.0)(_.toDouble)),
      runTime,
      task.endReason
    )
  }
}

// More detailed FlatTask class
case class FlatTaskDetail(stageId:Long, taskId:Long, duration:Long, durationNs:Long, executorId:String, finishTime:Long, gettingResultTime:Long, id:String, index:Int,
                          launchTime:Long, taskLocality:String, diskBytesSpilled:Long, executorCpuTime:Long, executorDeserializeCpuTime:Long,
                          executorDeserializeTime:Long, executorRunTime:Long, memoryBytesSpilled:Long, peakExecutionMemory:Long, resultSerializationTime:Long, resultSize:Long)

// Following inner classes keeps track of jobs, stages and tasks.
case class LogJob(jobId:Int) extends Serializable {
  var submissionTime: Option[Long] = None
  var numStages: Int = 0
  var stageIds:Seq[Int] = Seq.empty[Int]
  var stageInfos:Seq[StageInfo] = Seq.empty[StageInfo]
  // time in SparkListenerJobEnd. TODO check what time is meant
  var time: Option[Long] = None
  override def toString:String = { "LogJob("+submissionTime.getOrElse("None")+", "+numStages+", "+stageIds+", "+stageInfos+", "+time.getOrElse("None")+")" }
}

case class LogTask(stageId: Int) extends Serializable {
  var index: Int = -1
  var taskInfo: Option[TaskInfo] = None
  var taskMetrics: Option[TaskMetrics] = None
  var submissionTime: Option[Long] = None // Represents the time the stage is submitted
  var jobEnd: Option[Long] = None
  var endReason: String = ""
  override def toString:String = { "LogTask("+taskInfo.getOrElse("None")+", "+taskMetrics.getOrElse("None")+", "+submissionTime.getOrElse("None")+", "+jobEnd.getOrElse("None")+")" }
}

case class LogStage(stageId: Int) extends Serializable {
  var stageInfo: Option[StageInfo] = None
  var job: Option[LogJob] = None
//  var tasks: scala.collection.mutable.Map[Int, LogTask] = scala.collection.mutable.Map.empty[Int, LogTask]
  var tasks: scala.collection.mutable.ListBuffer[LogTask] = scala.collection.mutable.ListBuffer.empty[LogTask]
  override def toString:String = { "LogStage("+stageInfo.getOrElse("None")+", "+job.getOrElse("None")+", "+tasks+")" }
}
