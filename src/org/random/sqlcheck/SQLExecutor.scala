

package org.random.sqlcheck

import SQLParser.EResultSet
import scala.util.{Try}

object SQLExecutor {
  import SQLParser._

  def executeSelect(db: DB, select: String, boundValues: Seq[Any]): Seq[Row] = {
    val boundSelect = bindValues(select, boundValues)
    executeSelect(db, boundSelect)
  }

  def executeSelect(db: DB, select: String, boundValues: Map[String, Any]): Seq[Row] = {
    val boundSelect = bindValues(select, boundValues)
    executeSelect(db, boundSelect)
  }
  
  def executeSelect(db: DB, select: String): Seq[Row] = {
    SQLParser.parseAll(SQLParser.select, select) match {
      case SQLParser.Success(sel, _) => doExecute(db, sel)
      case ex@_ =>
        println(ex)
        emptyRS
    }
  }

  private def doExecute(db: DB, select: Select): Seq[Row] = {
    val startTableName = select.tables(0).name
    val startTableAlias = select.tables(0).alias
    val inputResultSet = db(startTableName).map(row => Map(startTableName -> row))
    val sc = select.conditions
    val conditions = sc.withDefault { _ => sc.getOrElse(startTableAlias, sc.getOrElse(None, Nil)) } (Some(startTableName))
    val conditionsCheck = conditions.foldLeft((rs: EResultSet) => rs)(_ compose _)
    val outputResultSet = conditionsCheck(EResultSet(inputResultSet, select, db))
    order(select, project(outputResultSet))
  }  
  
  private def bindValues(select: String, boundValues: Seq[Any]) = {
      boundValues.foldLeft(select)((select, value) => {
      val qIndex = select.indexOf("?")
      s"""${select.substring(0, qIndex)}'${value.toString}'${select.substring(qIndex + 1)}"""
    })
  }

  private def bindValues(select: String, boundValues: Map[String, Any]) = {
      boundValues.foldLeft(select)((select, vpair) => {
      val (name, value) = vpair  
      val nIndex = select.indexOf(name)
      s"""${select.substring(0, nIndex)}'${value.toString}'${select.substring(nIndex + name.length)}"""
    })
  }
  
  private def project(outputResultSet: EResultSet): Seq[Row] = {
    outputResultSet.rs.map { trow =>
      trow.foldLeft(Map[String, Any]())((crow, t1row) => {
        val (table, row) = t1row
        crow ++ row.foldLeft(Map[String, Any]())((r, pair) => {
          val (field,value) = pair
          val oTable: Option[Table] = outputResultSet.select.tables.find(t => t.name == table)
          val talias = oTable.map { t =>
            t.alias.getOrElse(t.name)
          }.getOrElse("")
          val oNewFieldName = getSelectedFieldName(field, talias, outputResultSet.select.fields)
          oNewFieldName match {
            case Some(newFieldName) => r + (newFieldName -> value)
            case None => r
          }
        })
      })
    }
  }
  
  private def order(select: Select, outputResultSet: Seq[Row]): Seq[Row] = {
    outputResultSet.sortWith((row1, row2) => {
      compare(row1, row2, select, select.orderCols)
    })
  }

  private def compare(row1: Row, row2: Row, select: Select, orderCols: Seq[Order]): Boolean = {
    if (orderCols.isEmpty) return true
    val order = orderCols.head
    val (table, column) = order.column
    val field = table match {
      case Some(t) => s"$t.$column"
      case None => column
    }
    compare(rvalue(row1, field, select), rvalue(row2, field, select)) match {
      case -1 => order.asc
      case 0 => compare(row1, row2, select, orderCols.tail)
      case _ => !order.asc
    }
  }
  
  private def compare(value1: Any, value2: Any): Int = {
    Try(compareAsNumbers(value1, value2)) match {
      case scala.util.Success(nCompResult) => nCompResult
      case scala.util.Failure(ex) => compareAsStrings(value1, value2)
    }
  }
  
  private def compareAsNumbers(value1: Any, value2: Any): Int = {
     val num1 = java.lang.Double.valueOf(value1.toString)
     val num2 = java.lang.Double.valueOf(value2.toString)
     if (num1 < num2) -1
     else if (num1 > num2) 1
     else 0
  }
  
  private def compareAsStrings(value1: Any, value2: Any): Int = {
    val s1 = value1.toString
    val s2 = value2.toString
    if (s1 < s2) -1 
    else if (s1 > s2) 1
    else 0
  }

  private def getSelectedFieldName(strField: String, talias: String, fields: Seq[Field]) = {
    val oField: Option[Field] = fields.find(f => f.name == s"$talias.$field" || f.name == strField ||
      f.name == s"$table.$field")
    oField match {
      case Some(field) =>
        field.alias match {
          case Some(alias) => Some(alias)
          case None        =>
            if (strField == "*" || strField.contains("."))
              Some(s"$talias.${field.name}")
            else
              Some(strField)
        }
      case None => fields.find(f => f.name == "*") match {
        case Some(_) => Some(s"$talias.$strField")
        case None    => None
      }
    }
  }
  
  private lazy val emptyRS = Seq[TRow]()
}