import AcceptanceTest._
import IntegrationTest._
import UnitTest._

name := "traffic-cwt"
organization := "com.gilcu2"

UnitTestSettings ++ IntegrationTestSettings ++ AcceptanceTestSettings
lazy val TestAll: Configuration = config("test-all").extend(AcceptanceTest.AcceptanceTestConfig)
configs(IntegrationTestConfig, AcceptanceTestConfig, TestAll)

version := "0.1"

scalaVersion := "2.11.12"

val sparkV = "2.4.4"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkV % "provided",
  "org.apache.spark" %% "spark-sql" % sparkV % "provided",
  "org.apache.spark" %% "spark-graphx" % sparkV % "provided",


  "com.typesafe" % "config" % "1.3.4",

  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.rogach" %% "scallop" % "3.3.1",
  "com.github.nscala-time" %% "nscala-time" % "2.22.0",

  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

mainClass in(Compile, run) := Some("com.gilcu2.ExplorationMain")

test in assembly := {}

assemblyJarName in assembly := "TrafficAccidents.jar"

assemblyMergeStrategy in assembly := {
  //  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.slf4j.**" -> "shaded.@1").inAll
)


