name := "spark-stream"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"
    