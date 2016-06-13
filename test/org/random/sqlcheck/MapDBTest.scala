package org.random.sqlcheck

import org.scalatest.WordSpecLike
import org.scalatest.MustMatchers

/**
 * A test with manually constructed DB data.
 */
class MapDBTest extends WordSpecLike with MustMatchers with DBCreator {
  import SQLExecutor._

  "SQLCheck should parse and execute select statement on manually prepared data and" should {
    "do that with comparison" in {
      val db = Map("people" ->
        Seq(
          Map("id" -> 1, "name" -> "Joe Doe"),
          Map("id" -> 2, "name" -> "John Smith"),
          Map("id" -> 3, "name" -> "George Cloony"),
          Map("id" -> 4, "name" -> "Samantha Fox")))

      val resultSet = executeSelect(db, """select * from people where people.id < 3""")

      resultSet.map { row =>
        info(s"""${row("people.id")}: ${row("people.name")}""")
      }

      resultSet.length mustBe 2
    }

    "do that with several tables joined and aliases used" in {

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

      val resultSet = executeSelect(db, """select * from people AS p, address AS a, age where people.id = address.pid AND age.pid=people.id""")

      resultSet.map { row =>
        info(s"""${row("p.id")}: ${row("p.name")}, ${row("a.city")}, ${row("age.age")}""")
      }

      resultSet.length mustBe 2
    }
  }

}