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
  
  
}