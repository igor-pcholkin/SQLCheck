

package org.random.sqlcheck

import SQLParser._
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
    val startTable = getStartTable(select) 
    val startTableName = startTable.name
    val startTableAlias = startTable.alias
    val inputResultSet = db(startTableName).map(mrow => Map(startTableName -> Row(mrow)))
    val sc = select.conditions
    val conditions = sc.withDefault { _ => sc.getOrElse(startTableAlias, sc.getOrElse(None, Nil)) } (Some(startTableName))
    val conditionsCheck = conditions.foldLeft((rs: EResultSet) => rs)(_ compose _)
    val outputResultSet = conditionsCheck(EResultSet(inputResultSet, select, db))
    order(select, project(outputResultSet))
  }  
  
  private def getStartTable(select: Select) = {
    select.primaryTable match {
      case None => select.tables(0) 
      case Some(primaryTableName) => select.tables.find(t => t.name == primaryTableName || t.alias == Some(primaryTableName)).getOrElse(select.tables(0)) 
    }
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
      Row(trow.foldLeft(Map[String, Any]())((crow, t1row) => {
        val (table, row) = t1row
        crow ++ row.contents.foldLeft(Map[String, Any]())((r, pair) => {
          val (dbFieldName,value) = pair
          val oTable: Option[Table] = outputResultSet.select.tables.find(t => t.name == table)
          val talias = oTable.map { t =>
            t.alias.getOrElse(t.name)
          }.getOrElse("")
          val oSelectedFieldName = getFullSelectedFieldName(dbFieldName, table, talias, outputResultSet.select.fields)
          oSelectedFieldName match {
            case Some(selectedFieldName) => r + (selectedFieldName -> value)
            case None => r
          }
        })
      }))
    }
  }
  
  private def order(select: Select, outputResultSet: Seq[Row]): Seq[Row] = {
    outputResultSet.sortWith(compare(_, _, select, select.orderCols))
  }

  private def compare(row1: Row, row2: Row, select: Select, orderCols: Seq[Order]): Boolean = {
    if (orderCols.isEmpty) return true
    val order = orderCols.head
    val (table, column) = order.column
    val field = table match {
      case Some(t) => s"$t.$column"
      case None => column
    }
    val nCompResult = compare(rcvalue(row1.contents, field, select), rcvalue(row2.contents, field, select)) 
    if (nCompResult < 0)
      order.asc
    else if (nCompResult > 0)  
      !order.asc
    else  
      compare(row1, row2, select, orderCols.tail)
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
     num1.compareTo(num2)
  }
  
  private def compareAsStrings(value1: Any, value2: Any): Int = {
    val s1 = value1.toString
    val s2 = value2.toString
    s1.compareTo(s2)
  }

  private def getFullSelectedFieldName(dbFieldName: String, table: String, talias: String, selectedFields: Seq[Field]) = {
    val oSelectedField: Option[Field] = selectedFields.find(sf => sf.name == s"$talias.$dbFieldName" || sf.name == dbFieldName ||
      sf.name == s"$table.$dbFieldName" || (sf.alias.nonEmpty && sf.alias.get == dbFieldName))
    oSelectedField match {
      case Some(field) =>
        field.alias match {
          case Some(alias) => Some(alias)
          case None        =>
            if (field.name == "*" || field.name.contains("."))
              Some(s"${field.name}")
            else
              Some(dbFieldName)
        }
      case None => selectedFields.find(f => f.name == "*") match {
        case Some(_) => Some(s"$talias.$dbFieldName")
        case None    => None
      }
    }
  }
  
  private lazy val emptyRS = Seq[Row]()
}