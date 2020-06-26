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

import com.ibm.sparktc.sparkbench.utils.{SaveModes, SparkBenchException}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.ibm.sparktc.sparkbench.utils.SparkFuncs._
import org.apache.spark.rdd.RDD

trait WorkloadDefaults {
  val name: String
  def apply(m: Map[String, Any]): Workload
}

trait Workload {
  val input: Option[String]
  val output: Option[String]
  val saveMode: String

  /**
    *  Validate that the data set has a correct schema and fix if necessary.
    *  This is to solve issues such as the KMeans load-from-disk pathway returning
    *  a DataFrame with all the rows as StringType instead of DoubleType.
   */
  def reconcileSchema(dataFrame: DataFrame): DataFrame = dataFrame

  /**
    * Actually run the workload.  Takes an optional DataFrame as input if the user
    * supplies an inputDir, and returns the generated results and data DataFrame. The last can
    * be used to create a workload chain.
    */
  def doWorkload(df: Option[DataFrame], sparkSession: SparkSession, prevRDD: Option[RDD[_]]): (DataFrame, Option[RDD[_]])

  def run(spark: SparkSession, inDf: Option[DataFrame]): (DataFrame, Option[RDD[Any]]) = {

    verifyOutput(output, saveMode, spark)
    if(saveMode == SaveModes.append){
      throw SparkBenchException("Save-mode \"append\" not available for workload results. " +
        "Please use \"errorifexists\", \"ignore\", or \"overwrite\" instead.")
    }

    var df = input.map { in =>
      val rawDF = load(spark, in)
      reconcileSchema(rawDF)
    }
    if(df.isEmpty && inDf.nonEmpty) {
      df = Some(reconcileSchema(inDf.get))
    }
    val (res, outData) = doWorkload(df, spark, None)
    (addConfToResults(res.coalesce(1), toMap), None)
  }

  def toMap: Map[String, Any] =
    (Map[String, Any]() /: this.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(this))
    }
}
