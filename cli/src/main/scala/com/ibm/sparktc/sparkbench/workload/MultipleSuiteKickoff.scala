/**
  * (C) Copyright IBM Corp. 2015 - 2017
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */

package com.ibm.sparktc.sparkbench.workload

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.parallel.ForkJoinTaskSupport

object MultipleSuiteKickoff {
  def run(seq: Seq[MultiSuiteRunConfig]): Unit = seq.foreach { contextConf =>
    val spark = createSparkContext(seq)
    waitForExecutors(spark)
    if (contextConf.suitesParallel) runSuitesInParallel(contextConf.suites, spark)
    else runSuitesSerially(contextConf.suites, spark)
  }

  private def runSuitesInParallel(suiteSeq: Seq[Suite], spark: SparkSession): Unit = {
    val parallelSeq = suiteSeq.par
    parallelSeq.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parallelSeq.size))
    parallelSeq.foreach(suite => SuiteKickoff.run(suite, spark))
  }

  private def runSuitesSerially(suiteSeq: Seq[Suite], spark: SparkSession): Unit =
    suiteSeq.foreach(SuiteKickoff.run(_, spark))

  private def createSparkContext(configs: Seq[MultiSuiteRunConfig]): SparkSession = {
    val builder = SparkSession.builder
    // if any configs have hive enabled, enable it for all
    val builderWithHive = if (configs.exists(_.enableHive)) builder.enableHiveSupport else builder
    builderWithHive.getOrCreate
  }

  private def waitForExecutors(spark: SparkSession): Unit = {
    val requestedExecutors = spark.sparkContext.getConf.getInt("spark.executor.instances", 1)
    var activeExecutors = numActiveExecutors(spark.sparkContext)
    while (activeExecutors < requestedExecutors) {
      Thread.sleep(1000)
      println(s"Waiting for executors. $activeExecutors/$requestedExecutors")
      activeExecutors = numActiveExecutors(spark.sparkContext)
    }
    println(s"Got $activeExecutors/$requestedExecutors executors. Waiting additional 5 seconds.")
    Thread.sleep(5000)
  }

  private def numActiveExecutors(sc: SparkContext): Int = {
    sc.getExecutorMemoryStatus.toList.length - 1
  }
}
