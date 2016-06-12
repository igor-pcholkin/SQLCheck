

package org.random.sqlcheck

import SQLParser.EResultSet

object SQLExecutor {
  import SQLParser._

  def execute(db: DB, select: Select) = {
    val startTable = select.tables(0)
    val inputResultSet = db(startTable)
    val conditionsCheck = select.conditions.withDefaultValue(Nil)(startTable).foldLeft((rs: EResultSet) => rs)(_ compose _)
    val outputResultSet = conditionsCheck(EResultSet(inputResultSet, select, db))
    outputResultSet.rs
  }

  def executeSelect(db: DB, select: String) = {
    SQLParser.parseAll(SQLParser.select, select) match {
      case SQLParser.Success(sel, _) => execute(db, sel)
      case ex@_ => emptyRS(ex)
    }
  }
  
  def emptyRS(ex: Any) = {
    println(ex)
    Seq[Row]()
  }
}