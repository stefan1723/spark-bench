package main.scala.breezeExtension

import breeze.stats.distributions.ContinuousDistr

/**
  * This "Distribution" always returns the same value but can be helpful if you want to create
  * distributions from ContinuousDistr but also support constant values.
  * @param value
  */
case class ConstantDistr(value: Double) extends ContinuousDistr[Double] {
  override def unnormalizedLogPdf(x: Double): Double = value

  override def logNormalizer: Double = value

  override def draw(): Double = value

  override def toString: String = s"Constant(value:$value)"
}
