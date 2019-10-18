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

object RandomAsciiGenerator extends WorkloadDefaults {
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

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): (DataFrame, Option[RDD[_]]) = {
    val timestamp = System.currentTimeMillis()

    val (generateTime, data): (Long, RDD[Row]) = time {
      spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map(i => {
        val randomGenerator = ThreadLocalRandom.current()
        Row.fromSeq(Seq.fill[Byte](partitionSize)((randomGenerator.nextInt(256) - 128).toByte))
      })
    }
    val dataSchema = StructType(
      List(
        StructField("bytes", ByteType, nullable = false)
      )
    )
    val (saveTime, _) = time { writeToDisk(output.get, saveMode, spark.createDataFrame(data,
      dataSchema),
      spark) }

    val timeResultSchema = StructType(
      List(
        StructField("name", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false),
        StructField("generate-time", LongType, nullable = true),
        StructField("save-time", LongType, nullable = true)
      )
    )


    val timeList = spark.sparkContext.parallelize(Seq(Row("random-ascii-generator", timestamp, generateTime)))

    (spark.createDataFrame(timeList, timeResultSchema), Some(data))
  }
}
