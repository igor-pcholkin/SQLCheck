

package org.random.sqlcheck

trait Value[T] {
  def value: T
  def >(otherValue: String): Boolean
  def <(otherValue: String): Boolean
  override def toString = value.toString
}

case class StringValue(value: String) extends Value[String] {
  override def >(otherValue: String) = value > otherValue
  override def <(otherValue: String) = value < otherValue
}

case class NumberValue(value: Double) extends Value[Double] {
  override def >(otherValue: String) = value > otherValue.toDouble
  override def <(otherValue: String) = value < otherValue.toDouble
}
