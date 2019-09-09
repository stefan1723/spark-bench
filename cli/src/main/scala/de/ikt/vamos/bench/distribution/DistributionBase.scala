package de.ikt.vamos.bench.distribution

import breeze.stats.distributions.ContinuousDistr
import scala.collection.JavaConverters._
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.getOrThrow
import com.ibm.sparktc.sparkbench.utils.{SparkBenchException}
import de.ikt.vamos.utils.CustomWorkloadParameter

/**
  * Abstract class for distributions. A distribution must publish a function to get the next sample
  * and the configuration string.
  */
abstract class DistributionBase(val multiplier: Double = 1.0) extends CustomWorkloadParameter{
  val distribution: ContinuousDistr[Double]

  def nextSample(): Double = distribution.sample()*multiplier
  def sample(n: Int): IndexedSeq[Double] = IndexedSeq.fill(n)(nextSample())
  override def toString: String = s"${distribution.toString}"
  override def getStringRepresentation: String = s"${distribution.toString}"
}

trait DistributionDefaults {
  val name: String
  def apply(m: Map[String, Any]): DistributionBase
}

object DistributionBase {
//  def createDistribution_(config: Config): DistributionBase = {
////    createDistribution(TypesafeAccessories.configToMapStringSeqAny(config))
//    createDistribution(TypesafeAccessories.configToMapStringSeqAny(config.root().unwrapped()
//      .asScala.asInstanceOf[Map[String, Any]]))
//  }

  def createDistribution(m: Map[String, Any]): DistributionBase = {
    val distrName = getOrThrow(m, "distribution").asInstanceOf[String]
    val (displayName, conf) =
      (distrName, SupportedDistribution.workloads.get(distrName))
    conf match {
      case Some(wk) => wk.apply(m)
      case _ => throw SparkBenchException(s"Could not find distribution $displayName")
    }
  }
}

object SupportedDistribution {
  val workloads: Map[String, DistributionDefaults] = Set(
    ExponentialDistribution,
    ConstantDistribution,
    Erlang,
    UniformDistribution
  ).map(wk => wk.name -> wk).toMap
}