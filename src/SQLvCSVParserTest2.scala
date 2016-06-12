
import java.io.FileReader

object SQLvCSVParserTest2 extends App {
  
  import SQLExecutor._
  
  val select = """
    select * from locations where name like "B%" 
  """ 
  
   val resultSet = CSVParser.parseAll(CSVParser.data, new FileReader("countries-locations.csv")) match {
    case CSVParser.Success(inputData, _) => 
      val db = Map("locations" -> inputData)
      SQLParser.parseAll(SQLParser.select, select) match {
        case SQLParser.Success(sel, _) => 
          execute(db, sel)
        case ex@_ => emptyRS(ex)
      }
    case ex@_ => emptyRS(ex)
  }
  
  resultSet("locations").rs.map { row =>
    println(s"""${row("name")}, ${row("latitude")}, ${row("longitude")}""")
  }
  
}