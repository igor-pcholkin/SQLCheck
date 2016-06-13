

package org.random.sqlcheck

import scala.util.parsing.combinator._

object SQLParser extends JavaTokenParsers with ParserUtils with SQLParserHelpers {
  type TABLE_NAME = String
  type VALUE = Any
  type COLUMN_NAME = String
  type DB = Map[TABLE_NAME, Seq[Row]]
  type Row = Map[String, Any]
  type TRow = Map[String, Row]
  type ResultSet = Seq[TRow]

  case class Field(name: String, alias: Option[String])
  case class Table(name: String, alias: Option[String])
  case class WhereCondition(field: String, value: String)
  case class EResultSet(rs: ResultSet, select: Select, db: DB)
  case class Select(fields: Seq[Field], tables: Seq[Table], conditions: Map[Option[String], Seq[EResultSet => EResultSet]])
  
  val NoRow = Seq()
  
  lazy val select =
    (("SELECT".ic ~ "DISTINCT".ic.?) ~> fields) ~
      ("FROM".ic ~> tables) ~
      ("WHERE".ic ~> whereConditions) ^^ {
        case fields ~ tables ~ wheres => Select(fields, tables, wheres)
      }

  lazy val fields = rep1sep(fieldSpec, ",")

  lazy val fieldSpec = field | fieldAll

  lazy val field: Parser[Field] = (fieldName ~ ("AS".ic ~> fieldName).?) ^^ {
    case f ~ a => Field(f, a)
  }

  lazy val fieldName = aqStringValue | ident

  lazy val fieldAll = "*" ^^^ Field("*", None)

  lazy val tables = rep1sep(table, ",")
  
  lazy val table = (ident ~ ("AS".ic ~> ident).?) ^^ {
    case t ~ a => Table(t, a)
  }

  lazy val whereConditions = rep1sep(whereCondition, "AND".ic) ^^ {
    case clist =>
      clist.flatten.groupBy(_._1).map { case (n, vlist) =>
        (n, vlist map (_._2))
      }
  }

  lazy val whereCondition: Parser[Seq[(Option[String], EResultSet => EResultSet)]] = joins | operation ^^ { case op => Seq(op) } 
  
  lazy val operation = equal | like | greater | less

  lazy val column = (ident <~ ".").? ~ ident ^^ { case t ~ f => (t, f) }

  lazy val equal = (column <~ "=") ~ value ^^ {
    case (otable, field) ~ value =>
      (otable, (ers: EResultSet) => filterValues(otable, ers) { row =>
        val rv = rvalue(row, field, ers)
        rv == value.toString
      })
  }

  lazy val joins: Parser[Seq[(Option[String], EResultSet => EResultSet)]] = (column <~ "=") ~ column ^^ { case column1 ~ column2 => 
    val (otable, field) = column1
    val (otable2, field2) = column2
    Seq((otable, (ers: EResultSet) => joinTables(ers, column1, column2)), 
        (otable2, (ers: EResultSet) => joinTables(ers, column2, column1)))
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
  
  lazy val greater = (column <~ ">") ~ value ^^ {
    case (otable, field) ~ value => (otable, (ers: EResultSet) => filterValues(otable, ers) { row =>
      value < rvalue(row, field, ers) 
    })
  }

  lazy val less = (column <~ "<") ~ value ^^ {
    case (otable, field) ~ value => (otable, (ers: EResultSet) => filterValues(otable, ers) { row => 
      value > rvalue(row, field, ers) 
    })
  }

  lazy val like = (column <~ "like".ic) ~ aqStringValue ^^ {
    case (otable, field) ~ pattern =>
      (otable, (ers: EResultSet) =>
        filterValues(otable, ers) { row =>
          val rowStrValue = rvalue(row, field, ers)
          if (pattern.startsWith("%") && pattern.endsWith("%"))
            rowStrValue.contains(pattern.substring(1, pattern.length - 1))
          else if (pattern.startsWith("%"))
            rowStrValue.endsWith(pattern.substring(1))
          else if (pattern.endsWith("%"))
            rowStrValue.startsWith(pattern.substring(0, pattern.length - 1))
          else
            true
        })
  }

  lazy val value = aqStringValue ^^ { case sv => StringValue(sv) } | decimalNumber ^^ { case dn => NumberValue(dn.toDouble) }
}