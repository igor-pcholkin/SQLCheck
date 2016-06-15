

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
  case class Order(column: (Option[String], String), asc: Boolean)
  case class Select(fields: Seq[Field], tables: Seq[Table], conditions: Map[Option[String], Seq[EResultSet => EResultSet]], 
      orderCols: Option[Seq[Order]])
  
  def select =
    (("SELECT".ic ~ "DISTINCT".ic.?) ~> fields) ~
      ("FROM".ic ~> tables) ~
      ("WHERE".ic ~> whereConditions)  ~
      ("ORDER BY".ic ~> orderColumns).? ^^ { case fields ~ tables ~ wheres ~ orderColumns => Select(fields, tables, wheres, orderColumns) }

  def fields = rep1sep(fieldSpec, ",")

  def fieldSpec = field | fieldAll

  def field = (fieldName ~ ("AS".ic ~> fieldName).?) ^^ {
    case fieldName ~ alias => Field(fieldName, alias)
  }

  def orderColumns = rep1sep(orderColumnSpec, ",")     
  
  def orderColumnSpec = column ~ ("ASC".ic | "DESC".ic).? ^^ { case col ~ order =>
    Order(col, order.map(o => o.toLowerCase() == "asc").getOrElse(true))
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
    case (otable, field) ~ value2 => filterValues(otable, field, value2) { case (rowValue, value2) => rowValue == value2.toString }
  }

  def not_equal = (column <~ ("!=" | "<>")) ~ value ^^ {
    case (otable, field) ~ value2 => filterValues(otable, field, value2) { case (rowValue, value2) => rowValue != value2.toString }
  }
  
  def greater = (column <~ ">") ~ value ^^ {
    case (otable, field) ~ value2 => filterValues(otable, field, value2) { case (rowValue, value2) => value2 < rowValue }
  }

  def less = (column <~ "<") ~ value ^^ {
    case (otable, field) ~ value2 => filterValues(otable, field, value2) { case (rowValue, value2) => value2 > rowValue }
  }

  def ge = (column <~ ">=") ~ value ^^ {
    case (otable, field) ~ value2 => filterValues(otable, field, value2) {
      case (rowValue, value2) =>
        rowValue == value2.toString || value2 < rowValue
    }
  }

  def le = (column <~ "<=") ~ value ^^ {
    case (otable, field) ~ value2 => filterValues(otable, field, value2) {
      case (rowValue, value2) =>
        rowValue == value2.toString || value2 > rowValue
    }
  }

  def like = (column <~ "like".ic) ~ aqStringValue ^^ {
    case (otable, field) ~ pattern =>
      filterValues(otable, field, StringValue(pattern)) {
        case (rowValue, value2) =>
          if (pattern.startsWith("%") && pattern.endsWith("%"))
            rowValue.contains(pattern.substring(1, pattern.length - 1))
          else if (pattern.startsWith("%"))
            rowValue.endsWith(pattern.substring(1))
          else if (pattern.endsWith("%"))
            rowValue.startsWith(pattern.substring(0, pattern.length - 1))
          else
            true
      }
  }

  def value = aqStringValue ^^ { case sv => StringValue(sv) } | 
              decimalNumber ^^ { case dn => NumberValue(dn.toDouble) }
              
}