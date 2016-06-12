
object SQLParserTest extends App {
  
  val s = """
    select distinct id, friendlyName as 'name' from customers where id > 2
  """
  
  val db = Map("customers" ->
    Seq(
        Map("id" -> 1, "name" -> "Joe Doe"),
        Map("id" -> 2, "name" -> "John Smith"),
        Map("id" -> 3, "name" -> "George Cloony"),
        Map("id" -> 4, "name" -> "Samantha Fox")
    )
  )
  
  val outputResults = SQLParser.parseAll(SQLParser.select, s) match {
    case SQLParser.Success(sel, _) => SQLExecutor.execute(db, sel)
    case ex => ex
  }
  println(outputResults)
}