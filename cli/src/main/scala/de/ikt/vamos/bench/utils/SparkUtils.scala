package main.scala.de.ikt.vamos.bench.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def waitForExecutors(spark: SparkSession): Unit = {
    val requestedExecutors = spark.sparkContext.getConf.getInt("spark.executor.instances", 1)
    var activeExecutors = numActiveExecutors(spark.sparkContext)
    while (activeExecutors < requestedExecutors) {
      Thread.sleep(1000)
      println(s"Waiting for executors. $activeExecutors/$requestedExecutors")
      activeExecutors = numActiveExecutors(spark.sparkContext)
    }
    println(s"Got $activeExecutors/$requestedExecutors executors.")
  }

  def numActiveExecutors(sc: SparkContext): Int = {
    sc.getExecutorMemoryStatus.toList.length - 1
  }
}
