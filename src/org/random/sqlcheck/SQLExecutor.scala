

package org.random.sqlcheck

import SQLParser.EResultSet

object SQLExecutor {
  import SQLParser._

  def execute(db: DB, select: Select): Seq[Row] = {
    val startTableName = select.tables(0).name
    val startTableAlias = select.tables(0).alias
    val inputResultSet = db(startTableName).map(row => Map(startTableName -> row))
    val sc = select.conditions
    val conditions = sc.withDefault { _ => sc.getOrElse(startTableAlias, sc.getOrElse(None, Nil)) } (Some(startTableName))
    val conditionsCheck = conditions.foldLeft((rs: EResultSet) => rs)(_ compose _)
    val outputResultSet = conditionsCheck(EResultSet(inputResultSet, select, db))
    project(outputResultSet)
  }  

  def project(outputResultSet: EResultSet): Seq[Row] = {
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

  def getSelectedFieldName(strField: String, talias: String, fields: Seq[Field]) = {
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