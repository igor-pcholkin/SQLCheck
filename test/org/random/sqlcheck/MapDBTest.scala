package org.random.sqlcheck

import org.scalatest.WordSpecLike
import org.scalatest.MustMatchers

/**
 * A test with manually constructed DB data.
 */
class MapDBTest extends WordSpecLike with MustMatchers with DBCreator {
  import SQLExecutor._

  val db = Map("people" ->
    Seq(
      Map("id" -> 1, "name" -> "Joe Doe"),
      Map("id" -> 2, "name" -> "John Smith"),
      Map("id" -> 3, "name" -> "George Cloony"),
      Map("id" -> 4, "name" -> "Samantha Fox")),

    "address" ->
      Seq(
        Map("id" -> 1, "pid" -> 2, "city" -> "New York"),
        Map("id" -> 2, "pid" -> 3, "city" -> "Los Angeles"),
        Map("id" -> 3, "pid" -> 4, "city" -> "San Francisco")),

    "age" ->
      Seq(
        Map("id" -> 1, "pid" -> 3, "age" -> 58),
        Map("id" -> 2, "pid" -> 4, "age" -> 47)))
  
  "SQLCheck should parse and execute select statement on manually prepared data and" should {
    "do that with comparison" in {

      val resultSet = executeSelect(db, """select id, name from people where id < 3""")

      resultSet.map { row =>
        info(s"""${row("id")}: ${row("name")}""")
      }

      resultSet.length mustBe 2
    }

    "do that with several tables joined and aliases used" in {

      val resultSet = executeSelect(db, """select * from people AS p, address AS ad, age
        where p.id = ad.pid AND age.pid=p.id order by age.age""")

      resultSet.map { row =>
        info(s"""${row("p.id")}: ${row("p.name")}, ${row("ad.city")}, ${row("age.age")}""")
      }

      resultSet.length mustBe 2
    }

    "do that with order check" in {

      val smallDB = Map("people" ->
        Seq(
          Map("id" -> 1, "name" -> "Joe Doe"),
          Map("id" -> 10, "name" -> "John Smith"),
          Map("id" -> 3, "name" -> "George Cloony"),
          Map("id" -> 4, "name" -> "Samantha Fox"))
      )
      
      val resultSet = executeSelect(smallDB, """select * from people AS p ORDER BY p.id DESC""")

      resultSet.map { _("p.id") } mustBe Seq(10, 4, 3, 1)
    }
    
  }

}