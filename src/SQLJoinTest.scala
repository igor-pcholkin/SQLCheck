
import java.io.FileReader

object SQLJoinTest extends App {
  
  import SQLExecutor._
  
  val select = """
    select * from locations, gbp where locations.name like "L%" AND locations.name=gbp.country 
  """ 
  
  def parseInputData(fileName: String) = CSVParser.parseAll(CSVParser.data, new FileReader(fileName)) match {
    case CSVParser.Success(inputData, _) => inputData
    case ex@_ => println(ex); List[Map[String, String]]()
  }
  
  val locationsData = parseInputData("countries-locations.csv") 
  val gbpData = parseInputData("countries-per-capita.csv")
  
  val db = Map("locations" -> locationsData, "gbp" -> gbpData)
  
  val resultSet = SQLParser.parseAll(SQLParser.select, select) match {
    case SQLParser.Success(sel, _) => execute(db, sel)
    case ex@_ => emptyRS(ex)
  }
  
  resultSet.head._2.rs.map { row =>
    println(s"""${row("name")}, ${row("latitude")}, ${row("longitude")}, ${row("GDP-per-capita ($; 2012)")}""")
  }
  
}