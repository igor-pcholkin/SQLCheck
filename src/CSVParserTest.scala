import java.io.FileReader

object CSVParserTest extends App {
  import CSVParser._
  
  parseAll(data, new FileReader("countries-locations.csv")) match {
    case Success(results, _) => println(results)
    case ex                         => println(ex) 
  }
  

}