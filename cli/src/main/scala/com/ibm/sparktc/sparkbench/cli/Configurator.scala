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

package com.ibm.sparktc.sparkbench.cli

import com.ibm.sparktc.sparkbench.utils.{SaveModes, TypesafeAccessories}
import com.ibm.sparktc.sparkbench.utils.TypesafeAccessories.configToMapStringSeqAny

import scala.collection.JavaConverters._
import com.ibm.sparktc.sparkbench.workload.{MultiSuiteRunConfig, Suite}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import main.scala.de.ikt.vamos.bench.scheduler.SchedulerBase
import main.scala.de.ikt.vamos.bench.workload.ArrivalProcess

import scala.util.Try

object Configurator {

  def apply(str: String): Seq[MultiSuiteRunConfig] = {
    val unescaped = StringContext.treatEscapes(str)
    val config: Config = ConfigFactory.parseString(unescaped)
    val sparkBenchConfig = config.getObject("spark-bench").toConfig
    val multiSuiteRunConfig: Seq[MultiSuiteRunConfig] = parseSparkBenchRunConfig(sparkBenchConfig)
    multiSuiteRunConfig
  }

  private def parseSparkBenchRunConfig(config: Config): Seq[MultiSuiteRunConfig] = {
    val sparkContextConfs = getConfigListByName("spark-submit-config", config)
    val workloadConfs = sparkContextConfs.map { sparkContextConf => {
        MultiSuiteRunConfig(
          suitesParallel = Try(sparkContextConf.getBoolean("suites-parallel")).getOrElse(false),
          enableHive = Try(sparkContextConf.getBoolean("enable-hive")).getOrElse(false),
          suites = getConfigListByName("workload-suites", sparkContextConf).map(parseSuite)
        )
      }
    }
    workloadConfs
  }

  private def getConfigListByName(name: String, config: Config): List[Config] = {
    val workloadObjs: Iterable[ConfigObject] = config.getObjectList(name).asScala
    workloadObjs.map(_.toConfig).toList
  }

  def parseSuite(config: Config): Suite = {
    val descr: Option[String] = Try(config.getString("descr")).toOption
    val runMode: String = Try(config.getString("run-mode")).getOrElse("serial")
    val parallel: Boolean = Try(config.getBoolean("parallel")).getOrElse(false)
    val repeat: Int = Try(config.getInt("repeat")).getOrElse(1)
    val repeatBuf: Int = Try(config.getInt("repeatBuf")).getOrElse(-1)
    val output: Option[String] = Try(config.getString("benchmark-output")).toOption
    val saveMode: String = Try(config.getString("save-mode")).getOrElse(SaveModes.error)
    val workloads: Seq[Map[String, Seq[Any]]]  = getConfigListByName("workloads", config).map(configToMapStringSeqAny)
//    if (runMode == "split-merge" || runMode == "fork-join" || runMode == "single-queue-fork-join");
//    val arrivalProcess = config.getConfig("arival-distribution")
    val scheduler = SchedulerBase.apply(runMode, Try{config.getConfig("arrival-distribution")}
  .getOrElse(ConfigFactory.empty()).root().unwrapped().asScala.toMap)

    Suite.build(workloads, descr, repeat, repeatBuf, parallel, scheduler, saveMode, output)
  }

//  def parseArrivalProcess(config: Option[Config]): Option[ArrivalProcess] = {
//    if (config.isEmpty) None
//    val distribution = config.get.getString(distribution)
//    None
//  }
}
