package de.ikt.vamos.bench.distribution

import breeze.stats.distributions.Gamma
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.getOrThrow

class Erlang(shape: Double, scale: Double) extends DistributionBase {
  override val distribution: Gamma = Gamma(shape, scale)()
}

object Erlang extends DistributionDefaults {
  val name: String = "erlang"
  def apply(shape: Double, scale: Double): Erlang = new Erlang(shape, scale)

  override def apply(m: Map[String, Any]): DistributionBase = {
    val shape = getOrThrow(m, "shape").asInstanceOf[Double]
    val scale = getOrThrow(m, "scale").asInstanceOf[Double]
    new Erlang(shape, scale)
  }
}