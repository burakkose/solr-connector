name := "solr-connector"

organization := "burakkose"

version := "0.1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= {
  val akkaVersion = "2.5.9"
  val scalaTestVersion = "3.0.4"
  val solrjVersion = "7.2.0"
  val slf4jVersion = "1.7.25"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "org.apache.solr" % "solr-solrj" % solrjVersion,
    //Test
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "org.apache.solr" % "solr-test-framework" % solrjVersion % Test,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % Test,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
  )
}
