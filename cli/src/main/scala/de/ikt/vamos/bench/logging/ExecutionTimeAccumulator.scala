package main.scala.de.ikt.vamos.bench.logging

import org.apache.spark.util.AccumulatorV2

class ExecutionTimeAccumulator(var executionTimeStat: ExecutionTimeStat) extends
  AccumulatorV2[(String, Long), ExecutionTimeStat] {
  def reset(): Unit = {
    executionTimeStat = ExecutionTimeStat()
  }

  def add(input: (String, Long)): Unit = {
    executionTimeStat = executionTimeStat.add(input)
  }

  def value: ExecutionTimeStat = {
    executionTimeStat
  }

  def isZero: Boolean = {
    executionTimeStat.isEmpty()
  }

  def copy(): ExecutionTimeAccumulator = {
    new ExecutionTimeAccumulator(executionTimeStat)
  }

  def merge(other: AccumulatorV2[(String, Long), ExecutionTimeStat]):Unit = {
    executionTimeStat = executionTimeStat.merge(other.value)
  }
}

object ExecutionTimeAccumulator {
  def apply(executionTimeStat: ExecutionTimeStat): ExecutionTimeAccumulator = {
    new ExecutionTimeAccumulator(executionTimeStat)
  }
}


case class ExecutionTimeStat(stats: Map[String, Long] = Map()) extends Serializable {

  // method to define logic for adding metric up during a transformation
  def add(input: (String, Long)): ExecutionTimeStat = {
    val existingCount = stats.getOrElse(input._1, 0L)
    this.copy(stats = stats.filterKeys{key: String => !key.equals(input._1)} ++ Map(input._1 ->
      (input._2 + existingCount)))
  }

  // method to define logic for merging two metric instances during an action
  def merge(other: ExecutionTimeStat) = {
    this.copy(mergeMaps(this.stats, other.stats))
  }

  private def mergeMaps(l: Map[String, Long], r: Map[String, Long]): Map[String, Long] = {
    (l.keySet union r.keySet).map { key =>
      key -> (l.getOrElse(key, 0L) + r.getOrElse(key, 0L))
    }.toMap
  }

  def isEmpty(): Boolean = {
    stats.isEmpty
  }
}