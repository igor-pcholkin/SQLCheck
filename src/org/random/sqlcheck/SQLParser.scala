

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
  
  def select =
    (("SELECT".ic ~ "DISTINCT".ic.?) ~> fields) ~
      ("FROM".ic ~> tables) ~
      ("WHERE".ic ~> whereConditions) ^^ {
        case fields ~ tables ~ wheres => Select(fields, tables, wheres)
      }

  def fields = rep1sep(fieldSpec, ",")

  def fieldSpec = field | fieldAll

  def field = (fieldName ~ ("AS".ic ~> fieldName).?) ^^ {
    case fieldName ~ alias => Field(fieldName, alias)
  }

  def fieldName = aqStringValue | ident

  def fieldAll = "*" ^^^ Field("*", None)

  def tables = rep1sep(table, ",")
  
  def table = (ident ~ ("AS".ic ~> ident).?) ^^ {
    case tableName ~ alias => Table(tableName, alias)
  }

  def whereConditions = rep1sep(whereCondition, "AND".ic) ^^ {
    case filters => groupedByTable(filters)
  }

  def whereCondition = joins | operation ^^ { case op => Seq(op) } 
  
  def operation = equal | not_equal | like | greater | less | ge | le

  def column = (ident <~ ".").? ~ ident ^^ { case oTable ~ cName => (oTable, cName) }

  def joins = (column <~ "=") ~ column ^^ { case column1 ~ column2 => 
    Seq(joinTables(column1, column2), joinTables(column2, column1))
  }
  
  def equal = (column <~ "=") ~ value ^^ {
    case (otable, field) ~ value => filterValues(otable) { case (row, ers) => rvalue(row, field, ers) == value.toString }
  }

  def not_equal = (column <~ ("!=" | "<>")) ~ value ^^ {
    case (otable, field) ~ value => filterValues(otable) { case (row, ers) => rvalue(row, field, ers) != value.toString }
  }
  
  def greater = (column <~ ">") ~ value ^^ {
    case (otable, field) ~ value => filterValues(otable) { case (row, ers) => value < rvalue(row, field, ers) }
  }

  def less = (column <~ "<") ~ value ^^ {
    case (otable, field) ~ value => filterValues(otable) { case (row, ers) => value > rvalue(row, field, ers) }
  }

  def ge = (column <~ ">=") ~ value ^^ {
    case (otable, field) ~ value => filterValues(otable) {
      case (row, ers) =>
        val rv = rvalue(row, field, ers)
        value == rv.toString || value < rv
    }
  }

  def le = (column <~ "<=") ~ value ^^ {
    case (otable, field) ~ value => filterValues(otable) {
      case (row, ers) =>
        val rv = rvalue(row, field, ers)
        value == rv || value > rv
    }
  }

  def like = (column <~ "like".ic) ~ aqStringValue ^^ {
    case (otable, field) ~ pattern =>
      filterValues(otable) {
        case (row, ers) =>
          val rowStrValue = rvalue(row, field, ers)
          if (pattern.startsWith("%") && pattern.endsWith("%"))
            rowStrValue.contains(pattern.substring(1, pattern.length - 1))
          else if (pattern.startsWith("%"))
            rowStrValue.endsWith(pattern.substring(1))
          else if (pattern.endsWith("%"))
            rowStrValue.startsWith(pattern.substring(0, pattern.length - 1))
          else
            true
      }
  }

  def value = aqStringValue ^^ { case sv => StringValue(sv) } | decimalNumber ^^ { case dn => NumberValue(dn.toDouble) }
}