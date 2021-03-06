package de.ikt.vamos.bench.workload

import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.utils.SaveModes
import de.ikt.vamos.bench.distribution.{ConstantDistribution, DistributionBase, ExponentialDistribution}
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.math.random

object DistributedBlocking extends WorkloadDefaults {
  val name = "distributed-blocking"

  override def apply(m: Map[String, Any]): DistributedBlocking = {
    val distStr = getOrThrow(m, "distribution").asInstanceOf[String]
    //    getOrThrow(m, "mu").asInstanceOf[Double]
//    val mu: Option[Double] = m.get("mu").asInstanceOf[Option[Double]]
//    val multiplier: Double = m.get("multiplier").asInstanceOf[Option[Double]].getOrElse(1.0)
    val numSlices: Int = m.get("slices").asInstanceOf[Option[Int]].getOrElse(10)
    val dist = distStr match {
      case "constant" => ConstantDistribution.apply(m)
      case "exponential" => ExponentialDistribution.apply(m)
    }
    new DistributedBlocking(
      distTest = dist, numSlices = numSlices
      // Only for testing
    )
  }

  def waitAndGetStageId(taskId: Int, serviceTimes: IndexedSeq[Double]): Int = {
    val jobLength = serviceTimes(taskId)
    val startTime = java.lang.System.currentTimeMillis()
    val targetStopTime = startTime + jobLength
    while (java.lang.System.currentTimeMillis() < targetStopTime) {
      val x = random * 2 - 1
      val y = random * 2 - 1
    }
    val stopTime = java.lang.System.currentTimeMillis()
    TaskContext.get.stageId
  }
}
case class DistributedBlockingResult(
                                name: String,
                                stageId: Long,
                                timestamp: Long,
                                endTimestamp: Long,
                                generatedTimes: String
                              )



case class DistributedBlocking(input: Option[String] = None, output: Option[String] = None,
                               saveMode: String = SaveModes.error, distTest: DistributionBase,
                               numSlices: Int)
  extends Workload {
  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession, prevRDD: Option[RDD[_]]): (DataFrame,
    Option[RDD[_]]) = {
    val timestamp = System.currentTimeMillis()
//    val serviceDistribution = for (_ <- 1 to 10) yield distTest.nextSample()
    val serviceDistribution = distTest.sample(numSlices)
    val (generateTime, stageId): (Long, Long) = time {
      runEmptySlices(spark, serviceDistribution.size, serviceDistribution, 1, df)
    }

    val endTime = System.currentTimeMillis()

    (spark.createDataFrame(Seq(DistributedBlockingResult("distributed-blocking", stageId, timestamp,
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
  def runEmptySlices(spark:SparkSession, slices: Int, serviceTimes: IndexedSeq[Double], jobId: Int, df: Option[DataFrame] = None)
  : Long = {
    df match {
      case None => spark.sparkContext.parallelize(0 until slices, slices).map { i =>
          DistributedBlocking.waitAndGetStageId(i, serviceTimes)
        }.collect()(0)
      case Some(x) => x.rdd.map{ vals: Row =>
          DistributedBlocking.waitAndGetStageId(vals(0).asInstanceOf[Int], serviceTimes)
        }.collect()(0)
    }
  }


}