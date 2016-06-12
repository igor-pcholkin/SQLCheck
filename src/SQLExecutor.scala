

object SQLExecutor {
  import SQLParser._

  def execute(db: DB, select: Select) = {
    db.map {
      case (tableName, inputResultSet) =>
        val conditionsCheck = select.conditions.withDefaultValue(Nil)(tableName).foldLeft((rs: EResultSet) => rs)(_ compose _)
        val outputResultSet = conditionsCheck(EResultSet(inputResultSet, select, db))
        tableName -> outputResultSet
    }
  }

  def emptyRS(ex: Any) = {
    println(ex)
    Map[TABLE_NAME, EResultSet]()
  }
}