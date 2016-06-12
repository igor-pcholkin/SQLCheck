name := "SQLCheck"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4" withSources()
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6" withSources()
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test" withSources()  