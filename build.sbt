name := "zeus-realtimeengine-services"

version := "0.1"

scalaVersion := "2.11.12"


val sparkVersion = "2.2.1"

val hbaseVersion = "1.1.1"

val scalaLibVersion = "2.11.12"


// Spark
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided"

// JSON
libraryDependencies += "org.json4s" % "json4s-jackson_2.11" % "3.6.0"

// HBase
libraryDependencies += "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-client" % hbaseVersion % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-server" % hbaseVersion % "provided"

// Redis
libraryDependencies += "redis.clients" % "jedis" % "2.9.0"

// MySql
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.45"

// Config
libraryDependencies += "com.typesafe" % "config" % "1.3.2"

// Scala
libraryDependencies += "org.scala-lang" % "scala-library" % scalaLibVersion % "provided"
libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.11" % "1.1.0"