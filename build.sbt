name := "banking-vault-poc"

version := "1.0.0"

scalaVersion := "2.12.18" // Spark 3.5 requires Scala 2.12

// Core Spark dependencies
libraryDependencies ++= Seq(
  // Spark Core and SQL - for distributed data processing
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  
  // Apache Iceberg - for ACID table format with schema evolution
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.4.3",
  "org.apache.iceberg" % "iceberg-hive-metastore" % "1.4.3",
  
  // Hive support - for SQL compatibility and metastore
  "org.apache.spark" %% "spark-hive" % "3.5.0",
  
  // PostgreSQL JDBC driver - to connect to source system
  "org.postgresql" % "postgresql" % "42.6.0",
  
  // Logging - for debugging and monitoring
  "org.slf4j" % "slf4j-api" % "2.0.9",
  "org.slf4j" % "slf4j-simple" % "2.0.9",
  
  // Testing frameworks
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

// Compiler options for better error messages
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint"
)

// Resolve dependency conflicts
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2"
)

// Enable parallel execution
Test / parallelExecution := false

