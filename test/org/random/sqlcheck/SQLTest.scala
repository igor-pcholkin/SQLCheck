package org.random.sqlcheck

import org.scalatest.WordSpecLike
import org.scalatest.MustMatchers
import SQLExecutor._

/**
 * Tests with DB constructed from CSV files each representing a separate table in the DB.
 */
class SQLTest extends WordSpecLike with MustMatchers with DBCreator {

  val db = createDB("testData/", Map("countries-locations.csv" -> "locations", "countries-per-capita.csv" -> "gdp"))

  "SQLCheck" should {
    "parse and execute select statement with join and like" in {
      val resultSet = executeSelect(db, 
          """select * from locations as l, gdp as g where locations.name like "L%" AND locations.name=gdp.country""")
      
      resultSet.map { row =>
        info(s"""${row("l.name")}, ${row("l.latitude")}, ${row("l.longitude")}, ${row("g.GDP-per-capita ($; 2012)")}""")
      }
      
      resultSet.length mustBe 3
    }

    "parse and execute select statement with comparison operations" in {
      val resultSet = executeSelect(db, 
          """select name, "GDP-per-capita ($; 2012)" as gdp from gdp where gdp.gdp > 18000 and gdp.gdp < 20000""")

      info(f"${"Country"}%20s | ${"GDP"}%8s \n")
      resultSet.map { row =>
        info(f"""${row("gdp.country")}%20s | ${row("gdp.GDP-per-capita ($; 2012)")}%8s""")
      }

      resultSet.length mustBe 1
    }
    
    "parse and execute select statement with omitted table prefix for fields in where clause" in {
      val resultSet = executeSelect(db, 
          """select * from locations as l where name like "A%"""")
      
      resultSet.map { row =>
        info(s"""${row("l.name")}""")
      }
      
      resultSet.length mustBe 15
    }
    
  }
}