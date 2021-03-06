package org.random.sqlcheck

trait SQLParserHelpers { 
  import SQLParser._
  
  val NoRow = Seq()
  val EmptyConditions = Map[Option[String], Seq[EResultSet => EResultSet]]()

  def groupedByTable(filters: Seq[(Option[String], EResultSet => EResultSet)]) = {
    filters.groupBy(_._1).map {
      case (tableName, tableFilters) =>
        (tableName, tableFilters map (_._2))
    }
  }
  
  def rcvalue(rowContents: Map[String, Any], field: String, select: Select) = {
    val rowA = rowContents.withDefault { alias =>
      val fn = select.fields.find(_.alias == Some(alias)).map(f => f.name).get
      rowContents(fn)
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
        filter(rcvalue(row.contents, field, select), value2)
      } 
      EResultSet(newRS, select, ers.db)
    })
  }

  def joinTables(column1: (Option[String], String), column2: (Option[String], String)) = {
    val (otable, field) = column1
    val (otable2, field2) = column2
    (otable, (joinType: JoinType, ers: EResultSet) => {
      val table2 = getTableNameInDB(otable2.get, ers)
      val rs2 = ers.db(table2)
      ers.copy(rs = ers.rs.flatMap { trow =>
        val row = getRow(otable, trow, ers)
        val rv = rcvalue(row.contents, field, ers.select)
        rs2.find(row2 => rv == rcvalue(row2, field2, ers.select)) match {
          case Some(row2) => Seq(trow + (table2 -> Row(row2)))
          case None       =>
            joinType match {
              case InnerJoin => NoRow
              case LeftJoin  => Seq(trow)
            }
        }
      })
    })
  }

  def createJoin(jt: Option[JoinType], t: Table, orderedJoin: ((Option[String], String), (Option[String], String)),
                 wheres: Option[Map[Option[String], Seq[EResultSet => EResultSet]]]) = {
    val joinType = jt.getOrElse(InnerJoin)
    val (column1, column2) = orderedJoin
    val joins = wheres.getOrElse(EmptyConditions)
    val allJoins = if (joinType == LeftJoin || joinType == RightJoin) {
      val significantJoin = if (joinType == LeftJoin) joinTables(column1, column2) else joinTables(column2, column1)
      val boundToType = bindToJoinType(LeftJoin, significantJoin)
      val boundSignificantJoin = (boundToType._1, Seq(boundToType._2))
      joins + boundSignificantJoin
    } else {
      val filters = Seq(joinTables(column1, column2), joinTables(column2, column1))
      joins ++ groupedByTable(bindToJoinType(InnerJoin, filters))
    }

    val primaryTable = (joinType match {
      case LeftJoin  => Some(column1._1)
      case RightJoin => Some(column2._1)
      case _         => None
    }).flatten

    Join(t, primaryTable, allJoins)
  }
  
  def getTableNameInDB(table: String, ers: EResultSet) = {
    ers.db.get(table) match {
      case Some(_) => table
      case None => ers.select.tables.find(t => t.alias == Some(table)).get.name
    }
  }
  
  def buildSelect(fields: Seq[Field], tableSpecs: Seq[TableSpec], oWheres: Option[Map[Option[String], Seq[EResultSet => EResultSet]]], 
      orderColumns: Option[Seq[Order]], distinct: Option[String]) = {
    val tables = for {
      tspec <- tableSpecs
      table <- tspec.table :: tspec.joins.map(_.table)
    } yield (table)
    
    val conditions = (for {
      tspec <- tableSpecs
      join <- tspec.joins 
    } yield (join.conditions)).foldLeft(oWheres.getOrElse(EmptyConditions)) { (wheres, wheres2) => 
      wheres ++ wheres2
    }
    
    val primaryTable = (for {
      tspec <- tableSpecs
      join <- tspec.joins 
    } yield (join.primaryTable)).find(pt => pt != None).flatten
    
    Select(fields, tables, conditions, orderColumns.getOrElse(Seq[Order]()), primaryTable, distinct.nonEmpty)
  }

  def bindToJoinType(joinType: JoinType, filters: Seq[(Option[String], (JoinType, EResultSet) => EResultSet)]):
    Seq[(Option[String], EResultSet => EResultSet)] = {
    filters map { f => bindToJoinType(joinType,f) }
  }

  def bindToJoinType(joinType: JoinType, join: (Option[String], (JoinType, EResultSet) => EResultSet)):
    (Option[String], EResultSet => EResultSet) = {
      val (t, c) = join
      (t, c(joinType, _: EResultSet))
  }
  
}