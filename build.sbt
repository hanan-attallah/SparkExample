import sbt.Keys._

name := "HelloScala"

version := "1.1"

scalaVersion := "2.12.8"


// https://mvnrepository.com/artifact/org.apache.spark/
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"

