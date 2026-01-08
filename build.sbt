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
  // APACHE SPARK (Core + SQL + Avro + Hive)
  // ──────────────────────────────────────────────────────────────────────
  // STRATEGY:
  // - spark-core and spark-sql: marked "provided" (from SPARK_HOME)
  // - spark-avro and spark-hive: NOT marked "provided" (included in uber JAR)
  //
  // REASONING:
  // - SPARK_HOME contains only basic jars (core, sql, catalyst, etc.)
  // - spark-avro and spark-hive are optional modules not in default SPARK_HOME
  // - Including them in uber JAR ensures portability across environments

  // Core Spark functionality (RDDs, DataFrames, SparkSession) - FROM SPARK_HOME
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" excludeAll(
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),

  // Spark SQL for structured data processing - FROM SPARK_HOME
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" excludeAll(
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),

  // Spark Avro support for reading/writing Avro files - INCLUDED IN UBER JAR
  "org.apache.spark" %% "spark-avro" % sparkVersion excludeAll(
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),

  // Spark Hive support (enables HiveContext and HMS integration) - INCLUDED IN UBER JAR
  "org.apache.spark" %% "spark-hive" % sparkVersion excludeAll(
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),

  // ──────────────────────────────────────────────────────────────────────
  // APACHE ICEBERG (Table Format)
  // ──────────────────────────────────────────────────────────────────────
  // NOTE: Iceberg dependencies are NOT marked as "provided" - they will be included in the uber JAR
  // This ensures the JAR is portable and works in any environment (local or cluster)

  // Iceberg Spark runtime for reading/writing Iceberg tables
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion excludeAll(
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),

  // Iceberg Hive Metastore catalog integration
  "org.apache.iceberg" % "iceberg-hive-metastore" % icebergVersion excludeAll(
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),

  // Iceberg core library (table format implementation)
  "org.apache.iceberg" % "iceberg-core" % icebergVersion excludeAll(
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),

  // ──────────────────────────────────────────────────────────────────────
  // APACHE HIVE METASTORE
  // ──────────────────────────────────────────────────────────────────────
  // Hive Metastore client for schema registry
  "org.apache.hive" % "hive-metastore" % hiveVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),

  // Hive common utilities
  "org.apache.hive" % "hive-common" % hiveVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.module")
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
  "org.apache.avro" % "avro-tools" % avroVersion % Test,

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
  // JACKSON - Forcer la version compatible avec Spark 3.5 et Scala module 2.15.x
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",

  // Typesafe Config for application configuration
  "com.typesafe" % "config" % "1.4.3",

  // JodaTime for date/time operations (used by Hive)
  "joda-time" % "joda-time" % "2.12.5",

  // Commons Lang3 for utility functions
  "org.apache.commons" % "commons-lang3" % "3.13.0"
)

// Forcer la résolution de Jackson 2.15.2 partout
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"
)

// Exclure les versions conflictuelles de Jackson dans Avro, Hadoop, Hive, etc.
dependencyOverrides ++= Seq(
  "org.apache.avro" % "avro" % avroVersion exclude("com.fasterxml.jackson.core", "*"),
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion exclude("com.fasterxml.jackson.core", "*"),
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion exclude("com.fasterxml.jackson.core", "*"),
  "org.apache.hive" % "hive-common" % hiveVersion exclude("com.fasterxml.jackson.core", "*"),
  "org.apache.hive" % "hive-metastore" % hiveVersion exclude("com.fasterxml.jackson.core", "*"),
  "org.apache.hive" % "hive-standalone-metastore" % hiveVersion exclude("com.fasterxml.jackson.core", "*")
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
  "-Dspark.master=local[*]"
)

// ============================================================================
// ASSEMBLY SETTINGS (Uber JAR Strategy)
// ============================================================================
//
// STRATEGY: Build a portable uber JAR that works both locally and in cluster
//
// EXCLUDED (marked as "provided", not in JAR):
//   - Spark Core & SQL (from SPARK_HOME)
//   - Hadoop Client, Common (from cluster or SPARK_HOME)
//   - Scala library (provided by Spark runtime)
//
// INCLUDED (in the uber JAR):
//   - Spark Avro & Hive (~20-30 MB) - NOT in default SPARK_HOME
//   - Iceberg runtime, core, and HMS integration (~10-15 MB)
//   - Hive Metastore client libraries
//   - PostgreSQL JDBC driver
//   - Derby embedded database
//   - Avro libraries
//   - Jackson 2.15.2 (forced version for compatibility)
//   - All utility libraries (commons-lang3, config, etc.)
//
// WHY THIS APPROACH:
//   ✅ Same JAR works locally (with local Spark) and in cluster
//   ✅ No need to install Iceberg or spark-avro/hive separately in cluster
//   ✅ Avoids version conflicts with cluster-provided Spark/Hadoop
//   ✅ Reasonable JAR size (~50-60 MB vs 200+ MB if all Spark included)
//   ✅ No need for --packages flag in spark-submit
//
// USAGE:
//   Build:     sbt clean assembly
//   Local:     spark-submit --class bronze.RawVaultETL target/scala-2.12/data-vault-modeling-etl-1.0.0.jar
//   Cluster:   spark-submit --master yarn --class bronze.RawVaultETL data-vault-modeling-etl-1.0.0.jar
//
// ============================================================================

// Exclude ONLY spark-core, spark-sql, and Hadoop from the uber JAR
// spark-avro, spark-hive, Iceberg, and all other dependencies WILL be included
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp filter { f =>
    f.data.getName.contains("spark-core") ||
    f.data.getName.contains("spark-sql") ||
    f.data.getName.contains("hadoop-client") ||
    f.data.getName.contains("hadoop-common") ||
    f.data.getName.contains("scala-library")
  }
}

// Merge strategy for assembling uber JAR
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

// ============================================================================
// IDE SUPPORT (IntelliJ)
// ============================================================================

// NOTE:
// IDE runtime classpath tweaks were removed on purpose.
// This project targets spark-submit execution where Spark core/sql are provided by SPARK_HOME/cluster.

// Same for Test configuration
// Test / fullClasspath ++= (Provided / fullClasspath).value // <-- Removed: caused sbt error

// Output to stdout for better logging
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
// PROJECT METADATA
// ============================================================================

licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/yourusername/data-vault-modeling-etl"),
    "scm:git@github.com:yourusername/data-vault-modeling-etl.git"
  )
)

developers := List(
  Developer(
    id = "banking-team",
    name = "Banking Data Vault Team",
    email = "datavault@banking.com",
    url = url("https://github.com/yourusername")
  )
)

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
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
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
// COMMAND ALIASES
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
