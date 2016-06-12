

import scala.util.parsing.combinator._

object SQLParser extends JavaTokenParsers with ParserUtils {

  type TABLE_NAME = String
  type VALUE = Any
  type COLUMN_NAME = String
  type DB = Map[TABLE_NAME, Seq[Row]]
  type Row = Map[String, Any]
  type ResultSet = Seq[Row]

  case class Field(name: String, alias: Option[String])
  case class WhereCondition(field: String, value: String)
  case class EResultSet(rs: ResultSet, select: Select, db: DB)
  case class Select(fields: Seq[Field], tables: Seq[String], conditions: Map[String, Seq[EResultSet => EResultSet]])
  
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

  lazy val tables = rep1sep(ident, ",")

  lazy val whereConditions = rep1sep(whereCondition, "AND".ic) ^^ {
    case clist =>
      clist.groupBy(_._1).map { case (n, vlist) =>
        (n, vlist map (_._2))
      }
  }

  lazy val whereCondition = equal | join | like | greater | less

  def rvalue(row: Row, field: String, ers: EResultSet) = {
    val rowA = row.withDefault { alias =>
      val fn = ers.select.fields.find(_.alias == Some(alias)).map(f => f.name).get
      row(fn)
    }
    rowA(field).toString
  }

  lazy val column = (ident <~ ".") ~ ident ^^ { case t ~ f => (t, f) }
  
  lazy val equal = (column <~ "=") ~ value ^^ {
    case (table, field) ~ value => (table, (ers: EResultSet) => ers.copy(rs = ers.rs.filter { row =>
      val rv = rvalue(row, field, ers)
      rv == value.toString
    }))
  }

  lazy val join = (column <~ "=") ~ column ^^ { case column1 ~ column2 => 
    val (table, field) = column1
    val (table2, field2) = column2
    (table, (ers: EResultSet) => ers.copy(rs = ers.rs.flatMap { row =>
      val rv = rvalue(row, field, ers)
      val rs2 = ers.db(table2)
      rs2.find( row2 => rv == rvalue(row2, field2, ers)) match {
        case Some(row2) => Seq(row ++ row2)
        case None => NoRow 
      }
    }))
  }
  
  lazy val greater = (column <~ ">") ~ value ^^ {
    case (table, field) ~ value => (table, (ers: EResultSet) => ers.copy(rs = ers.rs.filter { row => value < rvalue(row, field, ers) }))
  }

  lazy val less = (column <~ "<") ~ value ^^ {
    case (table, field) ~ value => (table, (ers: EResultSet) => ers.copy(rs = ers.rs.filter { row => value > rvalue(row, field, ers) }))
  }

  lazy val like = (column <~ "like".ic) ~ aqStringValue ^^ {
    case (table, field) ~ pattern => (table, (ers: EResultSet) => ers.copy(rs = ers.rs.filter { row =>
      val rowStrValue = rvalue(row, field, ers)
      if (pattern.startsWith("%") && pattern.endsWith("%"))
        rowStrValue.contains(pattern.substring(1, pattern.length - 1))
      else if (pattern.startsWith("%"))
        rowStrValue.endsWith(pattern.substring(1))
      else if (pattern.endsWith("%"))
        rowStrValue.startsWith(pattern.substring(0, pattern.length - 1))
      else
        true
    }))
  }

  lazy val value = aqStringValue ^^ { case sv => StringValue(sv) } | decimalNumber ^^ { case dn => NumberValue(dn.toDouble) }
}