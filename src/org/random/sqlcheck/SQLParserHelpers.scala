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

  def getRow(otable: Option[String], trow: TRow) = {
    otable match {
      case Some(table) => trow(table)
      case None => trow.head._2
    }
  }
  
  def filterValues(otable: Option[String], ers: EResultSet)(filter: Row => Boolean) = {
    ers.copy(rs = ers.rs.filter { trow => 
      val row = getRow(otable, trow)
      filter(row)
    })
  }
  
  def joinTables(ers: EResultSet, column1: (Option[String], String), column2: (Option[String], String)) = {
    val (otable, field) = column1
    val (otable2, field2) = column2
    ers.copy(rs = ers.rs.flatMap { trow =>
      val row = getRow(otable, trow)
      val rv = rvalue(row, field, ers)
      val table2 = otable2.get
      val rs2 = ers.db(table2)
      rs2.find( row2 => rv == rvalue(row2, field2, ers)) match {
        case Some(row2) => Seq(trow + (table2 -> row2))
        case None => NoRow 
      }
    })
  }
  
  
}