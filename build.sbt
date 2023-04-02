lazy val root = project
  .in(file("."))
  .settings(
    name         := "DataV",
    organization := "visionofsid",
    scalaVersion := "2.12.8",
    version      := "0.1.0-SNAPSHOT",
    libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.0.0",
    libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.0.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0-beta",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector-driver" % "3.0.0-beta",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector-assembly" % "3.0.0-beta",
    libraryDependencies +=  "joda-time" % "joda-time" % "2.10.2",

  )