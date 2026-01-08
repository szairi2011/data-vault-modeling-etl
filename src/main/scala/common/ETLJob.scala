package common

/**
 * ========================================================================
 * ETL JOB TRAIT - COMMON INTERFACE FOR ALL ETL JOBS
 * ========================================================================
 *
 * PURPOSE:
 * Define standard interface that all ETL jobs must implement.
 * Enables:
 * - Programmatic invocation (Airflow, testing)
 * - Consistent execution pattern
 * - Dependency injection of SparkSession
 *
 * USAGE:
 * ```scala
 * object MyETL extends ETLJob {
 *   def execute(spark: SparkSession, config: ETLConfig): Unit = {
 *     // Business logic here
 *   }
 * }
 * ```
 * ========================================================================
 */

import org.apache.spark.sql.SparkSession

trait ETLJob {

  /**
   * Execute the ETL job logic
   *
   * @param spark Active SparkSession (provided by caller)
   * @param config Job configuration
   */
  def execute(spark: SparkSession, config: ETLConfig): Unit

  /**
   * Optional: Validate prerequisites before execution
   *
   * @param spark SparkSession for validation queries
   * @param config Job configuration
   * @throws IllegalStateException if prerequisites not met
   */
  def validatePrerequisites(spark: SparkSession, config: ETLConfig): Unit = {
    // Default: no validation
    // Override in specific jobs if needed
  }

  /**
   * Standard main entry point for all ETL jobs
   * Delegates to execute() after setting up SparkSession
   *
   * @param args Command-line arguments
   * @param appName Application name for Spark UI
   */
  def runMain(args: Array[String], appName: String): Unit = {
    println(s"""
         |╔════════════════════════════════════════════════════════════════╗
         |║ $appName
         |╚════════════════════════════════════════════════════════════════╝
         |""".stripMargin)

    val config = ETLConfig.fromArgs(args)

    println(s"""
         |Configuration:
         |  Mode: ${config.mode}
         |  Entity: ${config.entity.getOrElse("all")}
         |  Snapshot Date: ${config.snapshotDate}
         |  Record Source: ${config.recordSource}
         |""".stripMargin)

    implicit val spark: SparkSession = SparkSessionFactory.createSession(appName)

    try {
      validatePrerequisites(spark, config)
      execute(spark, config)
      println(s"\n✅ $appName completed successfully")
    } catch {
      case e: Exception =>
        println(s"\n❌ $appName failed: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}

