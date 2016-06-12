package org.random.sqlcheck

import java.io.FileReader

trait DBCreator {
  def parseInputData(fileName: String) = CSVParser.parseAll(CSVParser.data, new FileReader(fileName)) match {
    case CSVParser.Success(inputData, _) => inputData
    case ex @ _                          => println(ex); List[Map[String, String]]()
  }

  def createDB(dataDir: String, tableMappings: Map[String, String]) = {
    tableMappings.map { case (fileName, table) => (table, parseInputData(s"$dataDir$fileName")) }
  }
}