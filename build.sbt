/**
 * ========================================================================
 * SBT BUILD CONFIGURATION
 * ========================================================================
 *
 * PURPOSE:
 * Define project structure, dependencies, and build settings for
 * Data Vault 2.0 POC with Spark, Iceberg, Avro, and Hive Metastore.
 *
 * KEY DEPENDENCIES:
 * - Apache Spark 3.5.0 (core, sql, avro)
 * - Apache Iceberg 1.4.3 (table format)
 * - Apache Hive 3.1.3 (metastore)
 * - PostgreSQL JDBC 42.7.1 (source database)
 * - Scala 2.12.18
 *
 * COMPATIBILITY MATRIX:
 * - Spark 3.5.x requires Scala 2.12
 * - Iceberg 1.4.x compatible with Spark 3.5.x
 * - Hive 3.1.x compatible with Spark 3.x
 *
 * ========================================================================
 */

name := "data-vault-modeling-etl"
version := "1.0.0"
scalaVersion := "2.12.18"

// Organization and maintainer info
organization := "com.banking"
organizationName := "Banking Data Vault POC"
organizationHomepage := Some(url("https://github.com/yourusername/data-vault-modeling-etl"))

// ============================================================================
// RESOLVER SETTINGS
// ============================================================================

resolvers ++= Seq(
  "Apache Snapshots" at "https://repository.apache.org/snapshots/",
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Confluent" at "https://packages.confluent.io/maven/"
)

// ============================================================================
// DEPENDENCY VERSIONS
// ============================================================================

val sparkVersion = "3.5.0"
val icebergVersion = "1.4.3"
val hiveVersion = "3.1.3"
val hadoopVersion = "3.3.6"
val postgresVersion = "42.7.1"
val avroVersion = "1.11.3"
val log4jVersion = "2.20.0"
val scalaTestVersion = "3.2.17"

// ============================================================================
// LIBRARY DEPENDENCIES
// ============================================================================

libraryDependencies ++= Seq(
  // ──────────────────────────────────────────────────────────────────────
  // APACHE SPARK (Core + SQL + Avro)
  // ──────────────────────────────────────────────────────────────────────
  // Core Spark functionality (RDDs, DataFrames, SparkSession)
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",

  // Spark SQL for structured data processing
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // Spark Avro support for reading/writing Avro files
  "org.apache.spark" %% "spark-avro" % sparkVersion,

  // Spark Hive support (enables HiveContext and HMS integration)
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",

  // WHY "provided"?
  // - Spark jars available in cluster runtime
  // - Reduces fat JAR size
  // - For local development, SBT will still resolve them

  // ──────────────────────────────────────────────────────────────────────
  // APACHE ICEBERG (Table Format)
  // ──────────────────────────────────────────────────────────────────────
  // Iceberg Spark runtime for reading/writing Iceberg tables
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion,

  // Iceberg core library (table format implementation)
  "org.apache.iceberg" % "iceberg-core" % icebergVersion,

  // Iceberg Hive Metastore catalog integration
  "org.apache.iceberg" % "iceberg-hive-metastore" % icebergVersion,

  // WHY ICEBERG:
  // - ACID transactions
  // - Schema evolution
  // - Time travel queries
  // - Hidden partitioning
  // - Efficient metadata management

  // ──────────────────────────────────────────────────────────────────────
  // APACHE HIVE METASTORE
  // ──────────────────────────────────────────────────────────────────────
  // Hive Metastore client for schema registry
  "org.apache.hive" % "hive-metastore" % hiveVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "javax.servlet")
  ),

  // Hive standalone metastore (for embedded mode)
  "org.apache.hive" % "hive-standalone-metastore" % hiveVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j")
  ),

  // Hive common utilities
  "org.apache.hive" % "hive-common" % hiveVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j")
  ),

  // WHY HMS:
  // - Centralized metadata storage
  // - Multi-engine support (Spark, Impala, Presto)
  // - Integration point for Iceberg catalog

  // ──────────────────────────────────────────────────────────────────────
  // APACHE HADOOP (FileSystem APIs)
  // ──────────────────────────────────────────────────────────────────────
  // Hadoop client for HDFS/local filesystem operations
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided" excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j")
  ),

  // Hadoop common utilities
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided" excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j")
  ),

  // ──────────────────────────────────────────────────────────────────────
  // APACHE AVRO (Schema + Serialization)
  // ──────────────────────────────────────────────────────────────────────
  // Avro library for schema management and serialization
  "org.apache.avro" % "avro" % avroVersion,

  // Avro tools for schema compilation and utilities
  "org.apache.avro" % "avro-tools" % avroVersion,

  // WHY AVRO:
  // - Self-describing format (schema embedded)
  // - Compact binary format
  // - Schema evolution support
  // - Integration with NiFi CDC pipeline

  // ──────────────────────────────────────────────────────────────────────
  // DATABASE DRIVERS
  // ──────────────────────────────────────────────────────────────────────
  // PostgreSQL JDBC driver for source database connection
  "org.postgresql" % "postgresql" % postgresVersion,

  // Derby embedded database for Hive Metastore (local development)
  "org.apache.derby" % "derby" % "10.14.2.0",

  // WHY DERBY:
  // - Embedded Java database (no separate install)
  // - Perfect for local HMS development
  // - Production uses MySQL/PostgreSQL for HMS

  // ──────────────────────────────────────────────────────────────────────
  // LOGGING
  // ──────────────────────────────────────────────────────────────────────
  // Log4j 2 for logging (Spark's default logging framework)
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,

  // SLF4J API (logging facade used by many libraries)
  "org.slf4j" % "slf4j-api" % "1.7.36",

  // ──────────────────────────────────────────────────────────────────────
  // TESTING
  // ──────────────────────────────────────────────────────────────────────
  // ScalaTest for unit and integration tests
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,

  // Spark testing base
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.5.0" % Test excludeAll(
    ExclusionRule(organization = "org.apache.hadoop")
  ),

  // ──────────────────────────────────────────────────────────────────────
  // UTILITIES
  // ──────────────────────────────────────────────────────────────────────
  // Typesafe Config for application configuration
  "com.typesafe" % "config" % "1.4.3",

  // JodaTime for date/time operations (used by Hive)
  "joda-time" % "joda-time" % "2.12.5",

  // Commons Lang3 for utility functions
  "org.apache.commons" % "commons-lang3" % "3.13.0"
)

// ============================================================================
// COMPILER OPTIONS
// ============================================================================

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-target:jvm-1.8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-Xfatal-warnings",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)

// ============================================================================
// JAVA OPTIONS
// ============================================================================

javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx4G",
  "-XX:+UseG1GC",
  "-XX:MaxGCPauseMillis=200",
  "-Dderby.system.home=metastore_db",
  "-Dspark.master=local[*]"
)

// ============================================================================
// ASSEMBLY SETTINGS (Fat JAR)
// ============================================================================

// Merge strategy for assembling fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => xs match {
    case "MANIFEST.MF" :: Nil => MergeStrategy.discard
    case "services" :: _ => MergeStrategy.concat
    case _ => MergeStrategy.discard
  }
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "log4j.properties" => MergeStrategy.first
  case "log4j2.xml" => MergeStrategy.first
  case PathList("org", "apache", "spark", "unused", _*) => MergeStrategy.first
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.endsWith(".html") => MergeStrategy.first
  case x if x.contains("hadoop") => MergeStrategy.first
  case _ => MergeStrategy.first
}

// Exclude Spark and Hadoop from fat JAR (provided by cluster)
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp filter { f =>
    f.data.getName.contains("spark-") ||
    f.data.getName.contains("hadoop-") ||
    f.data.getName.contains("scala-library")
  }
}

// Assembly JAR name
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

// ============================================================================
// RUN SETTINGS
// ============================================================================

// Fork JVM for run
run / fork := true

// Connect stdin for interactive mode
run / connectInput := true

// Output strategy
run / outputStrategy := Some(StdoutOutput)

// ============================================================================
// TEST SETTINGS
// ============================================================================

// Parallel test execution
Test / parallelExecution := false

// Fork JVM for tests
Test / fork := true

// Test options
Test / testOptions += Tests.Argument("-oD")

// ============================================================================
// CUSTOM TASKS
// ============================================================================

// Custom task: Initialize environment
lazy val initEnv = taskKey[Unit]("Initialize local development environment")
initEnv := {
  val log = streams.value.log
  log.info("Initializing local development environment...")

  // Create directories
  val dirs = Seq(
    "warehouse/staging/customer",
    "warehouse/staging/account",
    "warehouse/staging/transaction_header",
    "warehouse/staging/transaction_item",
    "warehouse/bronze",
    "warehouse/silver",
    "warehouse/gold",
    "logs/spark-events",
    "metastore_db"
  )

  dirs.foreach { dir =>
    val path = new java.io.File(dir)
    if (!path.exists()) {
      path.mkdirs()
      log.info(s"Created directory: $dir")
    }
  }

  log.info("✅ Environment initialized")
}

// Custom task: Clean all data
lazy val cleanData = taskKey[Unit]("Clean all warehouse and metadata")
cleanData := {
  val log = streams.value.log
  log.info("Cleaning warehouse and metadata...")

  val dirs = Seq("warehouse", "metastore_db", "logs/spark-events")
  dirs.foreach { dir =>
    val path = new java.io.File(dir)
    if (path.exists()) {
      import java.nio.file._
      import java.nio.file.attribute.BasicFileAttributes

      Files.walkFileTree(path.toPath, new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes) = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }
        override def postVisitDirectory(dir: Path, exc: java.io.IOException) = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      })

      log.info(s"Deleted: $dir")
    }
  }

  log.info("✅ Cleanup complete")
}

// ============================================================================
// PROJECT METADATA
// ============================================================================

developers := List(
  Developer(
    id = "data-team",
    name = "Data Engineering Team",
    email = "data@banking.com",
    url = url("https://github.com/yourusername")
  )
)

licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

homepage := Some(url("https://github.com/yourusername/data-vault-modeling-etl"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/yourusername/data-vault-modeling-etl"),
    "scm:git@github.com:yourusername/data-vault-modeling-etl.git"
  )
)

// ============================================================================
// ALIASES FOR COMMON TASKS
// ============================================================================

addCommandAlias("bronze", "runMain bronze.RawVaultETL")
addCommandAlias("silver", "runMain silver.BusinessVaultETL")
addCommandAlias("gold", "runMain gold.DimensionalModelETL")
addCommandAlias("query", "runMain semantic.QueryInterface")
addCommandAlias("seedRef", "runMain seeder.ReferenceDataSeeder")
addCommandAlias("seedTxn", "runMain seeder.TransactionalDataSeeder")

// ============================================================================
// USAGE EXAMPLES
// ============================================================================

/*
 * COMMON SBT COMMANDS:
 *
 * # Initialize environment
 * sbt initEnv
 *
 * # Compile project
 * sbt compile
 *
 * # Run tests
 * sbt test
 *
 * # Seed data
 * sbt seedRef
 * sbt seedTxn
 *
 * # Run ETL layers
 * sbt bronze
 * sbt silver
 * sbt gold
 *
 * # Run specific class
 * sbt "runMain bronze.RawVaultETL"
 *
 * # Build fat JAR
 * sbt assembly
 *
 * # Clean project
 * sbt clean
 *
 * # Clean data
 * sbt cleanData
 *
 * # Interactive console
 * sbt console
 *
 * # Submit to cluster
 * spark-submit --class bronze.RawVaultETL \
 *              --master yarn \
 *              --deploy-mode cluster \
 *              target/scala-2.12/data-vault-modeling-etl-1.0.0.jar
 */

