package org.random.sqlcheck

import org.scalatest.WordSpecLike
import org.scalatest.MustMatchers
import SQLExecutor._

class SQLTest extends WordSpecLike with MustMatchers with DBCreator {

  val db = createDB("testData/", Map("countries-locations.csv" -> "locations", "countries-per-capita.csv" -> "gdp"))

  "SQLCheck" should {
    "parse and execute select statement with join" in {
      val resultSet = executeSelect(db, 
          """select * from locations, gdp where locations.name like "L%" AND locations.name=gdp.country""")
      
      resultSet.map { row =>
        println(s"""${row("name")}, ${row("latitude")}, ${row("longitude")}, ${row("GDP-per-capita ($; 2012)")}""")
      }
      
      resultSet.length mustBe 3
    }

    "parse and execute select statement with comparison" in {
      val resultSet = executeSelect(db, 
          """select name, "GDP-per-capita ($; 2012)" as gdp from gdp where gdp.gdp > 18000 and gdp.gdp < 20000""")

      println(f"${"Country"}%20s | ${"GDP"}%8s \n")
      resultSet.map { row =>
        println(f"""${row("country")}%20s | ${row("GDP-per-capita ($; 2012)")}%8s""")
      }

      resultSet.length mustBe 1
    }
  }
}