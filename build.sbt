name := "solr-connector"

organization := "burakkose"

version := "0.1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= {
  val akkaStreamVersion = "2.5.8"
  val scalaTestVersion = "3.0.4"
  val solrjVersion = "7.2.0"
  val akkaHttpSprayJsonVersion = "10.1.0-RC1"
  val slf4jVersion = "1.7.25"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaStreamVersion,
    "org.apache.solr" % "solr-solrj" % solrjVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpSprayJsonVersion,
    //Test
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "org.apache.solr" % "solr-test-framework" % solrjVersion % Test,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % Test
  )
}