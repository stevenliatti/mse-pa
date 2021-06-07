ThisBuild / scalaVersion := "2.13.4"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "ch.hepia"
ThisBuild / organizationName := "hepia"

lazy val root = (project in file("."))
  .settings(
    name := "FlyManagerSimulator",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.2" % Test,
      "mysql" % "mysql-connector-java" % "8.0.24",
      "org.apache.kafka" % "kafka-clients" % "2.8.0",
      "io.spray" %%  "spray-json" % "1.3.6",
      "org.apache.kafka" % "kafka-streams" % "2.8.0",
      "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0"
    )
  )
