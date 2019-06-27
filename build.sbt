name := "Bot Detector"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("datastax" % "spark-cassandra-connector" % "2.4.0-s_2.11", "org.apache.spark" %% "spark-streaming" % "2.4.3")

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.3"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.3"