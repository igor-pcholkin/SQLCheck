

package org.random.sqlcheck

import SQLParser.{EResultSet, QBoundValues}

trait Value[+T] {
  def value: T
  def apply(ers: EResultSet): (Value[T], Option[QBoundValues]) = (this, None)
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

abstract class BoundVariable extends Value[Any] {
  override def value = throw new RuntimeException(errUnexpectedCallMsg)
  override def >(otherValue: String) = throw new RuntimeException(errUnexpectedCallMsg)
  override def <(otherValue: String) = throw new RuntimeException(errUnexpectedCallMsg)
  val errUnexpectedCallMsg = "You should call apply() to provide value for bound variable"
}

case class QBoundVariable() extends BoundVariable {
  override def apply(ers: EResultSet) = (ers.boundValues.head, Some(ers.boundValues.tail))
}