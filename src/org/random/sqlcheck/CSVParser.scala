package org.random.sqlcheck

import scala.util.parsing.combinator._

object CSVParser extends JavaTokenParsers with ParserUtils {

  override val whiteSpace = """[ \t]+""".r
  
  def data = (header <~ eol) ~ rows ^^ { case h ~ rs =>
    val cnames = h.toArray
    rs.foldLeft(List[Map[String, String]]()) { (resultSet, row) =>
      val dbRow = row.zipWithIndex.map { case (value, i) =>
        cnames(i) -> value
      }.toMap
      dbRow :: resultSet 
    }
  }
  
  def header = rep1sep(column_name, ",")
  
  def column_name = aqStringValue | ident
  
  def rows = rep1sep(row, eol)
  
  def row = rep1sep(value, ",")
  
  def value = aqStringValue | ident | floatingPointNumber
}