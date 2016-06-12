
import java.io.FileReader

object SQLvCSVParserTest extends App {
  
  import SQLExecutor._
  
  val select = """
    select name, "GDP-per-capita ($; 2012)" as gdp from CountriesGDP where gdp > 18000 and gdp < 20000 
  """ 
  
   val resultSet = CSVParser.parseAll(CSVParser.data, new FileReader("countries-per-capita.csv")) match {
    case CSVParser.Success(inputData, _) => 
      val db = Map("CountriesGDP" -> inputData)
      SQLParser.parseAll(SQLParser.select, select) match {
        case SQLParser.Success(sel, _) => 
          execute(db, sel)
        case ex@_ => emptyRS(ex)
      }
    case ex@_ => emptyRS(ex)
  }
  
  println(f"${"Country"}%20s | ${"GDP"}%8s \n")
  resultSet("CountriesGDP").rs.map { row =>
    println(f"""${row("country")}%20s | ${row("GDP-per-capita ($; 2012)")}%8s""")
  }
  
}