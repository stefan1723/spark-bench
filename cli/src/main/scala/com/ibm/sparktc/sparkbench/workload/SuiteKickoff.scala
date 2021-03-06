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

import com.ibm.sparktc.sparkbench.utils.SparkFuncs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

import scala.collection.parallel.ForkJoinTaskSupport

/**
    Run each workload in the sequence.
    Run the sequence `repeat` times over.
    If my sequence is Seq(A, B, B, C), it's running serially, and repeat is 2, this will run like:
    A
    B
    B
    C
    ---
    A
    B
    B
    C
    ---
    Done

    As opposed to:
    A
    A
    --
    B
    B
    --
    B
    B
    --
    C
    C
    ---
    Done
*/

object SuiteKickoff {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def run(s: Suite, spark: SparkSession): Unit = {
    verifyOutput(s.benchmarkOutput, s.saveMode, spark)
    var i = 0 // Iterations run
    while (!s.scheduler.isCompleted) {
      // Translate the maps into runnable workloads
      val dataframes: Seq[DataFrame] = s.scheduler.run(s, spark)
      // getting the Spark confs so we can output them in the results.
      val strSparkConfs = spark.conf.getAll
      // Ah, see, here's where we're joining that series of one-row DFs
      val singleDF = joinDataFrames(dataframes, spark)
      s.description.foreach(log.info)
      // And now we're going to curry in the results
      val plusSparkConf = addConfToResults(singleDF, strSparkConfs)
      val plusDescription = addConfToResults(plusSparkConf, Map("description" -> s.description)).coalesce(1)
      val saveMode = if (i == 0) s.saveMode else "append"
      i += 1
      // And write to disk. We're done with this suite!
      if(s.benchmarkOutput.nonEmpty) writeToDisk(s.benchmarkOutput.get, saveMode, plusDescription, spark)
    }
  }

  private def runParallel(workloadConfigs: Seq[Workload], spark: SparkSession): Seq[(DataFrame,
    Option[RDD[_]])] = {
    val confSeqPar = workloadConfigs.par
    confSeqPar.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(confSeqPar.size))
    confSeqPar.map(_.run(spark, None)).seq
  }

  private def runSerially(workloadConfigs: Seq[Workload], spark: SparkSession): Seq[(DataFrame,
    Option[RDD[_]])] = {
    workloadConfigs.map(_.run(spark, None))
  }

  private def joinDataFrames(seq: Seq[DataFrame], spark: SparkSession): DataFrame = {
    if (seq.length == 1) seq.head
    else {
      val seqOfColNames = seq.map(_.columns.toSet)
      val allTheColumns = seqOfColNames.foldLeft(Set[String]())(_ ++ _)

      def expr(myCols: Set[String], allCols: Set[String]) = {
        allCols.toList.map {
          case x if myCols.contains(x) => col(x)
          case x => lit(null).as(x)
        }
      }

      val seqFixedDfs = seq.map(df => df.select(expr(df.columns.toSet, allTheColumns): _*))
      if (seqFixedDfs.nonEmpty) {
        val schema = seqFixedDfs.head.schema
        // Folding left across this sequence should be fine because each DF should only have 1 row
        // Nevarr Evarr do this to legit dataframes that are all like big and stuff
        seqFixedDfs.foldLeft(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema))(_
          union _)
      } else
        spark.emptyDataFrame
    }
  }
}
