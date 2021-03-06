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

package main.scala.de.ikt.vamos.bench.datageneration

import java.util.concurrent.ThreadLocalRandom

import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.utils.SaveModes
import com.ibm.sparktc.sparkbench.utils.SparkFuncs.writeToDisk
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.mllib.util.KMeansDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object RandomAsciiGenerator extends WorkloadDefaults {
  val chars: IndexedSeq[Char] = ('a' to 'z') ++ ('A' to 'Z')
  val name = "random-ascii-generator"
  override def apply(m: Map[String, Any]): RandomAsciiGenerator = new RandomAsciiGenerator(
    partitionSize = getOrThrow(m, "partition-size").asInstanceOf[Int],
    output = Some(getOrThrow(m, "output").asInstanceOf[String]),
    saveMode = getOrDefault[String](m, "save-mode", SaveModes.error),
    numPartitions = getOrDefault[Int](m, "partitions", 1)
  )
}


case class RandomAsciiGenerator(partitionSize: Int,
                                input: Option[String] = None,
                                output: Option[String],
                                saveMode: String,
                                numPartitions: Int) extends Workload {

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession, prevRDD: Option[RDD[_]]): (DataFrame, Option[RDD[_]]) = {
    import spark.implicits._
    val timestamp = System.currentTimeMillis()

    val (generateTime, data) = time {
      spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map { i =>
        i
      }.map( i => (i,Array.fill[String](partitionSize)(RandomAsciiGenerator.chars(scala.util.Random
        .nextInt(52) ).toString).mkString(""))
      ).persist(StorageLevel.MEMORY_AND_DISK)
    }
    val dataSchema = StructType(
      List(
        StructField("out", StringType, nullable = false)
      )
    )

    val (saveTime, _) = time { writeToDisk(output.get, saveMode,
      data.toDF(), spark) }

    val timeResultSchema = StructType(
      List(
        StructField("name", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false),
        StructField("generate-time", LongType, nullable = true),
        StructField("save-time", LongType, nullable = true)
      )
    )
//    data.collect()[0].toString()

    val timeList = spark.sparkContext.parallelize(Seq(Row("random-ascii-generator", timestamp,
      generateTime, saveTime)))

    (spark.createDataFrame(timeList, timeResultSchema), Some(data))
  }
}
