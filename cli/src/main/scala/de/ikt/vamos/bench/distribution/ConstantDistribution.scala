package de.ikt.vamos.bench.distribution

import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.getOrThrow
import main.scala.breezeExtension.ConstantDistr

class ConstantDistribution(val time: Double = 2.0) extends DistributionBase {
  val distribution: ConstantDistr = ConstantDistr(time)
}

object ConstantDistribution extends DistributionDefaults {
  val name: String = "constant"

  override def apply(m: Map[String, Any]): DistributionBase = {
    val time = getOrThrow(m, "time").asInstanceOf[Double]
    new ConstantDistribution(time)
  }

  def apply(time: Double = 2.0): ConstantDistribution = new ConstantDistribution(time)
}