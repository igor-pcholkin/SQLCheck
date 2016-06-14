package org.random.sqlcheck

trait SQLParserHelpers { 
  import SQLParser._
  
  def rvalue(row: Row, field: String, ers: EResultSet) = {
    val rowA = row.withDefault { alias =>
      val fn = ers.select.fields.find(_.alias == Some(alias)).map(f => f.name).get
      row(fn)
    }
    rowA(field).toString
  }

  def getRow(otable: Option[String], trow: TRow, ers: EResultSet) = {
    otable match {
      case Some(table) => 
        val dTable = getTableNameInDB(table, ers)
        trow(dTable)
      case None => trow.head._2
    }
  }
  
  def filterValues(otable: Option[String], ers: EResultSet)(filter: Row => Boolean) = {
    ers.copy(rs = ers.rs.filter { trow => 
      val row = getRow(otable, trow, ers)
      filter(row)
    })
  }
  
  def joinTables(ers: EResultSet, column1: (Option[String], String), column2: (Option[String], String)) = {
    val (otable, field) = column1
    val (otable2, field2) = column2
    val table2 = getTableNameInDB(otable2.get, ers)
    val rs2 = ers.db(table2)
    ers.copy(rs = ers.rs.flatMap { trow =>
      val row = getRow(otable, trow, ers)
      val rv = rvalue(row, field, ers)
      rs2.find( row2 => rv == rvalue(row2, field2, ers)) match {
        case Some(row2) => Seq(trow + (table2 -> row2))
        case None => NoRow 
      }
    })
  }
  
  def getTableNameInDB(table: String, ers: EResultSet) = {
    ers.db.get(table) match {
      case Some(_) => table
      case None => ers.select.tables.find(t => t.alias == Some(table)).get.name
    }
  }
  
}