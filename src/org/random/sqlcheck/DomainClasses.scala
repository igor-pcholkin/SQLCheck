package org.random.sqlcheck

import SQLParser._

trait Value[+T] {
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

case class Row(contents: Map[String, Any]) {
  def apply(key: String) = {
    contents.getOrElse(key, "")
  }
}

case class Field(name: String, alias: Option[String])
case class Table(name: String, alias: Option[String])
case class TableSpec(table: Table, joins: List[Join])
case class Join(table: Table, conditions: Map[Option[String], Seq[EResultSet => EResultSet]])
case class WhereCondition(field: String, value: String)
case class EResultSet(rs: ResultSet, select: Select, db: DB)
case class Order(column: (Option[String], String), asc: Boolean)
case class Select(fields: Seq[Field], tables: Seq[Table], conditions: Map[Option[String], Seq[EResultSet => EResultSet]],
                  orderCols: Seq[Order])

class JoinType
case object InnerJoin extends JoinType
case object LeftJoin extends JoinType
case object RightJoin extends JoinType
case object FullJoin extends JoinType
                  

