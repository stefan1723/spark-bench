package main.scala.de.ikt.vamos.bench.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def waitForExecutors(spark: SparkSession): Unit = {
    val requestedExecutors = spark.sparkContext.getConf.getInt("spark.executor.instances", 1)
    var activeExecutors = numActiveExecutors(spark.sparkContext)
    while (activeExecutors < requestedExecutors) {
      Thread.sleep(1000)
      print(s"Waiting for executors. $activeExecutors/$requestedExecutors\n")
      activeExecutors = numActiveExecutors(spark.sparkContext)
    }
    print(s"Got $activeExecutors/$requestedExecutors executors.\n")
  }

  def numActiveExecutors(sc: SparkContext): Int = {
    sc.getExecutorMemoryStatus.toList.length - 1
  }
}
