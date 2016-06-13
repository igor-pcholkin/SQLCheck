

package org.random.sqlcheck

import SQLParser.EResultSet

object SQLExecutor {
  import SQLParser._

  def execute(db: DB, select: Select): Seq[Row] = {
    val startTable = select.tables(0).name
    val inputResultSet = db(startTable).map(row => Map(startTable -> row))
    val conditions = select.conditions.withDefaultValue(select.conditions.getOrElse(None, Nil))(Some(startTable))
    val conditionsCheck = conditions.foldLeft((rs: EResultSet) => rs)(_ compose _)
    val outputResultSet = conditionsCheck(EResultSet(inputResultSet, select, db))
    flatten(outputResultSet)
  }  

  def flatten(outputResultSet: EResultSet): Seq[Row] = {
    outputResultSet.rs.map { trow =>
      trow.foldLeft(Map[String, Any]())((crow, t1row) => {
        val (table, row) = t1row
        crow ++ row.foldLeft(Map[String, Any]())((r, pair) => {
          val (field,value) = pair
          val talias = outputResultSet.select.tables.find(t => t.name == table).map { t => 
            t.alias.getOrElse(t.name)
          }.getOrElse("")
          r + (s"$talias.$field" -> value)
        })
      })
    }
  }
  
  def executeSelect(db: DB, select: String): Seq[Row] = {
    SQLParser.parseAll(SQLParser.select, select) match {
      case SQLParser.Success(sel, _) => execute(db, sel)
      case ex@_ => emptyRS(ex)
    }
  }
  
  def emptyRS(ex: Any) = {
    println(ex)
    Seq[TRow]()
  }
}