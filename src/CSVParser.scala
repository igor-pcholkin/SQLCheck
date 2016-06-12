import scala.util.parsing.combinator._

object CSVParser extends JavaTokenParsers with ParserUtils {

  override val whiteSpace = """[ \t]+""".r
  
  lazy val data = (header <~ eol) ~ rows ^^ { case h ~ rs =>
    val cnames = h.toArray
    rs.foldLeft(List[Map[String, String]]()) { (resultSet, row) =>
      val dbRow = row.zipWithIndex.map { case (value, i) =>
        cnames(i) -> value
      }.toMap
      dbRow :: resultSet 
    }
  }
  
  lazy val header = rep1sep(column_name, ",")
  
  lazy val column_name = aqStringValue | ident
  
  lazy val rows = rep1sep(row, eol)
  
  lazy val row = rep1sep(value, ",")
  
  lazy val value = aqStringValue | ident | floatingPointNumber
}