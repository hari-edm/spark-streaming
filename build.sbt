version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.10"

name := "Spark Streaming"

val sparkVersion = "3.2.1"
val kafkaVersion = "3.3.1"

assembly / assemblyJarName := "spark-streaming-fatty-1.0.jar"

libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "3.2.14" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.slf4j" % "slf4j-api" % "2.0.5",
  "org.apache.spark" % "spark-sql_2.13" % "3.3.1",
  "org.apache.spark" % "spark-core_2.13" % "3.3.1",
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.13" % sparkVersion,
  "org.apache.spark" % "spark-yarn_2.13" % sparkVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.3.2"
)
