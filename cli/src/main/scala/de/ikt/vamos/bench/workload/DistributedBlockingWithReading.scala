package main.scala.de.ikt.vamos.bench.workload

import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.utils.SaveModes
import com.ibm.sparktc.sparkbench.utils.SparkFuncs.load
import de.ikt.vamos.bench.distribution.{DistributionBase, ExponentialDistribution}
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.random

/**
  * This workload should extend the DistributedBlocking workload by adding loading before blocking.
  * Therefore it should be possible to use a previouse created RDD or passing a path which should get used
  * for loading data.
  * The purpose is to create a distributed workload which includes overhead caused by data transfers.
  * TODO: Implement it correctly. The current version is only a draft and should not be used.
  */
object DistributedBlockingWithReading extends WorkloadDefaults {
  val name = "distributed-blocking-with-reading"

  override def apply(m: Map[String, Any]): DistributedBlockingWithReading = {
    val distStr = getOrThrow(m, "distribution").asInstanceOf[String]
    val numSlices: Int = m.get("slices").asInstanceOf[Option[Int]].getOrElse(10)
    val dataPath: String = m.get("data-path").asInstanceOf[Option[String]].getOrElse("-")
    new DistributedBlockingWithReading(
      distTest = ExponentialDistribution.apply(m), numSlices = numSlices,
      dataPath = dataPath
      // Only for testing
    )
  }
}
case class DistributedBlockingWithReadingResult(
                                name: String,
                                stageId: Long,
                                timestamp: Long,
                                endTimestamp: Long,
                                generatedTimes: String
                              )

case class DistributedBlockingWithReading(input: Option[String] = None, output: Option[String] = None,
                               saveMode: String = SaveModes.error, distTest: DistributionBase,
                               numSlices: Int, dataPath: String)
  extends Workload {
  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession, prevRDD: Option[RDD[_]]): (DataFrame,
    Option[RDD[_]]) = {
    val timestamp = System.currentTimeMillis()
//    val serviceDistribution = for (_ <- 1 to 10) yield distTest.nextSample()
    val serviceDistribution = distTest.sample(numSlices)
    val (generateTime, stageId): (Long, Long) = time {
      dataPath match {
        case "prevRDD" => runSlices(prevRDD.asInstanceOf[RDD[(Double, _)]],
          serviceDistribution, 1)
        case "-" => runSlices(spark.sparkContext.parallelize(1 to serviceDistribution.size,
          serviceDistribution.size),
          serviceDistribution, 1)
        case _ => val rdd = spark.sparkContext.textFile(dataPath, minPartitions = numSlices)
          runSlices(rdd,
            serviceDistribution, 1)
      }

    }

    val endTime = System.currentTimeMillis()

    (spark.createDataFrame(Seq(DistributedBlockingWithReadingResult
    (DistributedBlockingWithReading.name, stageId, timestamp,
      endTime, serviceDistribution.mkString(",")))), None)

  }
  /**
    * Run s slices on the Spark cluster, with service times drawn
    * from the given serviceProcess.
    *
    * Within each slice we just generate random numbers for the specified amount of time.
    * This is from the SparkPi demo program, generating random numbers in a square.
    * Each slice returns 1, and we do a count() to force Spark to execute the slices.
    * Therefore the shuffle/reduce step is trivial.
    *
    * Note: the stdout produced from these println() will appear on the stdout of the workers,
    *       not the driver.  I could just as well remove it.
    *
    *  Note: I would like to pass in the serviceProcess instead of a list of serviceTimes, but since
    *        this is parallelized I ran into the problem that in some cases we would be passing
    *        identical RNGs to the workers, and generating identical service times.
    */
  def runSlices(slices: RDD[_], serviceTimes: IndexedSeq[Double], jobId: Int)
  : Long = {
    //println("serviceTimes = "+serviceTimes)

    slices.zipWithIndex.map({ case (it, i) =>
      if(i < serviceTimes.size) {
        val taskId = i
        val jobLength = serviceTimes(i.toInt)
        val startTime = java.lang.System.currentTimeMillis()
        val targetStopTime = startTime + jobLength
        println(s"    +++ TASK $jobId.$taskId START: $startTime")
        while (java.lang.System.currentTimeMillis() < targetStopTime) {
          val x = random * 2 - 1
          val y = random * 2 - 1
        }

        val stopTime = java.lang.System.currentTimeMillis()
        println("    --- TASK $jobId.$taskId STOP: $stopTime")
        println("    === TASK $jobId.$taskId ELAPSED: ${stopTime-startTime}")
      }
      TaskContext.get.stageId
    }).collect()(0)
  }

  def loadFromDisk(spark: SparkSession, input: String): (Long, DataFrame) = time {
    val df = load(spark, input)
    df
  }
}