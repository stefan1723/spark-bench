package de.ikt.vamos.bench.distribution

import breeze.stats.distributions.Uniform
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.getOrThrow

class UniformDistribution extends DistributionBase {
  val distribution: Uniform = Uniform(0, 1)
}

object UniformDistribution extends DistributionDefaults {
  val name: String = "uniform"
  def apply(): UniformDistribution = new UniformDistribution()

  override def apply(m: Map[String, Any]): DistributionBase = {
    new UniformDistribution
  }
}