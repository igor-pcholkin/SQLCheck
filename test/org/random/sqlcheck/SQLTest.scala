package org.random.sqlcheck

import org.scalatest.WordSpecLike
import org.scalatest.MustMatchers
import SQLExecutor._
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * Tests with DB constructed from CSV files each representing a separate table in the DB.
 */
class SQLTest extends WordSpecLike with MustMatchers with DBCreator {

  val db = createDB("testData/", Map("countries-locations.csv" -> "locations", "countries-per-capita.csv" -> "gdp"))

  "SQLCheck should parse and execute select statement and " should {
    "do that with join and like" in {
      val resultSet = executeSelect(db, 
          """select * from locations as l, gdp as g where locations.name like "L%" AND locations.name=g.country""")
      
      resultSet.map { row =>
        info(s"""${row("l.name")}, ${row("l.latitude")}, ${row("l.longitude")}, ${row("g.GDP-per-capita ($; 2012)")}""")
      }
      
      resultSet.length mustBe 3
    }

    "do that with comparison operations" in {
      val resultSet = executeSelect(db, 
          """select country, "GDP-per-capita ($; 2012)" as gdp from gdp where gdp.gdp > 18000 and gdp.gdp < 20000""")

      resultSet.length mustBe 1
    }

    "do that with comparison operations 2" in {
      val resultSet = executeSelect(db, 
          """select country, "GDP-per-capita ($; 2012)" as gdp from gdp where gdp.gdp >= 18000 and gdp.gdp <= 20000""")

      resultSet.length mustBe 1
    }
    
    "do that with = operation" in {
      val resultSet = executeSelect(db, """select country, "GDP-per-capita ($; 2012)" as gdp from gdp where gdp = 46720.36""")

      resultSet.map { row =>
        info(s"""${row("country")}""")
      }

      resultSet.length mustBe 1
    }

    "do that with != operation" in {
      val resultSet = executeSelect(db, """select country, "GDP-per-capita ($; 2012)" as gdp from gdp where gdp != 46720.36""")

      resultSet.length mustBe 59
    }

    "do that with <> operation" in {
      val resultSet = executeSelect(db, """select country, "GDP-per-capita ($; 2012)" as gdp from gdp where gdp <> 46720.36""")

      resultSet.length mustBe 59
    }
    
    "do that with omitted table prefix for fields in where clause" in {
      val resultSet = executeSelect(db, 
          """select * from locations as l where name like "A%"""")
      
      resultSet.length mustBe 15
    }

    "do that with classic variable binding and alias based projection" in {
      val resultSet = executeSelect(db, """select country, "GDP-per-capita ($; 2012)" as gdp from gdp where country != ? AND country != ?""", 
          Seq("Latvia", "Lithuania"))

      resultSet.head.contents mustBe Map("country" -> "Luxembourg", "gdp" -> "107475.95")    
          
      resultSet.length mustBe 58
    }

    "do that with normal variable binding" in {
      val resultSet = executeSelect(db, """select country, "GDP-per-capita ($; 2012)" as gdp from gdp where country != :v1 AND country != :v2""", 
          Map(":v1" -> "Latvia", ":v2" -> "Lithuania"))

      resultSet.length mustBe 58
    }

    "do that with Play! style variable binding" in {
      val resultSet = executeSelect(db, """select country, "GDP-per-capita ($; 2012)" as gdp from gdp where country != {v1} AND country != {v2}""", 
          Map("{v1}" -> "Latvia", "{v2}" -> "Lithuania"))

      resultSet.length mustBe 58
    }
    
    "do that with order check" in {
      val resultSet = executeSelect(db, """select country from gdp where country like "C%" order by country ASC""") 

      resultSet.map { _("country") } mustBe Seq("Canada", "Chile", "Croatia", "Cyprus", "Czech Republic") 
    }
  }
  
  "SQLCheck" should {
    "validate correct select" in {
      validateSelect(db, """select country from gdp where country like "C%" order by country ASC""") match {
        case Success(_) => 
        case Failure(ex) => fail("Validation should execute successfully")
      }
    }

    "validate incorrect select" in {
      validateSelect(db, """select * from gdp country like "C%" order by country ASC""") match {
        case Success(_) => fail("Validation should fail")
        case Failure(ex) => 
      }
    }
  }
}