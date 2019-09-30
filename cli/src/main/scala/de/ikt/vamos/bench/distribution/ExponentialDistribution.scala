package de.ikt.vamos.bench.distribution

import breeze.stats.distributions.Exponential
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._

class ExponentialDistribution(val mu: Double, override val multiplier: Double = 1.0) extends
  DistributionBase(multiplier) {
  val distribution: Exponential = Exponential(mu)()
}

object ExponentialDistribution extends DistributionDefaults{
  val name: String = "exponential"

  override def apply(m: Map[String, Any]): ExponentialDistribution = {
    val mu = getOrThrow(m, "mu") match {
      case mu: Int => mu.toDouble
      case mu: Double => mu
    }
    val multiplier = optionallyGet(m, "multiplier") match {
      case multiplier: Option[Int] => multiplier.getOrElse(1).toDouble
      case multiplier: Option[Double] => multiplier.getOrElse(1.0)
    }
//    val multiplier = getOrDefault(m, "multiplier", 1.0)
    new ExponentialDistribution(mu, multiplier)
  }

  def apply(mu: Double, multplier: Double = 1.0): ExponentialDistribution = new
      ExponentialDistribution(mu, multplier)
}