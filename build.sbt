name := "solr-connector"

organization := "burakkose"

version := "0.1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= {
  val akkaStreamVersion = "2.5.8"
  val scalaTestVersion = "3.0.4"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaStreamVersion,

    //Test
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  )
}
