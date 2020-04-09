package de.ikt.vamos.bench.distribution

import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.getOrThrow
import main.scala.breezeExtension.ConstantDistr

class ConstantDistribution(val time: Double = 2.0) extends DistributionBase {
  val distribution: ConstantDistr = ConstantDistr(time)
}

object ConstantDistribution extends DistributionDefaults {
  val name: String = "constant"

  override def apply(m: Map[String, Any]): DistributionBase = {
    val k = getOrThrow(m, "k") match {
      case k: Int => k.toDouble
      case k: Double => k
    }
    new ConstantDistribution(k)
  }

  def apply(k: Double = 2.0): ConstantDistribution = new ConstantDistribution(k)
}