package org.random.sqlcheck

import org.scalatest.WordSpecLike
import org.scalatest.MustMatchers

/**
 * A test with manually constructed DB data. 
 **/
class MapDBTest extends WordSpecLike with MustMatchers with DBCreator {
  import SQLExecutor._
  
  val db = Map("customers" ->
    Seq(
        Map("id" -> 1, "name" -> "Joe Doe"),
        Map("id" -> 2, "name" -> "John Smith"),
        Map("id" -> 3, "name" -> "George Cloony"),
        Map("id" -> 4, "name" -> "Samantha Fox")
    )
  )
  
  "SQLCheck" should { 
    "parse and execute select statement on manually prepareed data with comparison" in {
      val resultSet = executeSelect(db, """select distinct id, friendlyName as 'name' from customers where customers.id > 2""" )
      
      resultSet.map { row =>
        info(s"""${row("id")}: ${row("name")}""")
      }
      
      resultSet.length mustBe 2
    }
  }
}