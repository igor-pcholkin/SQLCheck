package org.random.sqlcheck

trait SQLParserHelpers { 
  import SQLParser._
  
  val NoRow = Seq()
  val EmptyConditions = Map[Option[String], Seq[EResultSet => EResultSet]]()

  def groupedByTable(filters: List[Seq[(Option[String], SQLParser.EResultSet => SQLParser.EResultSet)]]) = {
    filters.flatten.groupBy(_._1).map {
      case (tableName, tableFilters) =>
        (tableName, tableFilters map (_._2))
    }
  }
  
  def rvalue(row: Row, field: String, select: Select) = {
    val rowA = row.withDefault { alias =>
      val fn = select.fields.find(_.alias == Some(alias)).map(f => f.name).get
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

  def filterValues(otable: Option[String], field: String, value2: Value[Any])
    (filter: (String, Value[Any]) => Boolean) = { (otable, (ers: EResultSet) => {
      val select = ers.select
      val newRS = ers.rs.filter { trow =>
        val row = getRow(otable, trow, ers)
        filter(rvalue(row, field, select), value2)
      } 
      EResultSet(newRS, select, ers.db)
    })
  }

  def joinTables(column1: (Option[String], String), column2: (Option[String], String)) = {
    val (otable, field) = column1
    val (otable2, field2) = column2
    (otable, (ers: EResultSet) => {
      val table2 = getTableNameInDB(otable2.get, ers)
      val rs2 = ers.db(table2)
      ers.copy(rs = ers.rs.flatMap { trow =>
        val row = getRow(otable, trow, ers)
        val rv = rvalue(row, field, ers.select)
        rs2.find(row2 => rv == rvalue(row2, field2, ers.select)) match {
          case Some(row2) => Seq(trow + (table2 -> row2))
          case None       => NoRow
        }
      })
    })
  }
  
  def getTableNameInDB(table: String, ers: EResultSet) = {
    ers.db.get(table) match {
      case Some(_) => table
      case None => ers.select.tables.find(t => t.alias == Some(table)).get.name
    }
  }
  
}