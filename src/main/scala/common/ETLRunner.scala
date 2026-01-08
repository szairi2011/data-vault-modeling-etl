package common

/**
 * ========================================================================
 * ETL RUNNER - PROGRAMMATIC JOB EXECUTION
 * ========================================================================
 *
 * PURPOSE:
 * Provide programmatic API for running ETL jobs without main() method.
 * Used by:
 * - Airflow DAGs (SparkSubmitOperator with application_args)
 * - Integration tests
 * - Job orchestration frameworks
 *
 * USAGE:
 * ```scala
 * // From Airflow or orchestration tool
 * ETLRunner.runJob(
 *   bronze.RawVaultETL,
 *   Map("mode" -> "incremental", "entity" -> "customer")
 * )
 * ```
 * ========================================================================
 */

import org.apache.spark.sql.SparkSession

object ETLRunner {

  /**
   * Run an ETL job programmatically
   *
   * @param job ETL job instance (object)
   * @param configOverrides Configuration overrides
   * @param appName Optional application name (defaults to job class simple name)
   */
  def runJob(
    job: ETLJob,
    configOverrides: Map[String, String] = Map.empty,
    appName: Option[String] = None
  ): Unit = {

    val jobName = appName.getOrElse(job.getClass.getSimpleName.replace("$", ""))
    println(s"\n🚀 ETLRunner: Starting $jobName programmatically")

    val config = ETLConfig.fromMap(configOverrides)
    implicit val spark: SparkSession = SparkSessionFactory.createSession(jobName)

    try {
      job.validatePrerequisites(spark, config)
      job.execute(spark, config)

      println(s"\n✅ ETLRunner: $jobName completed successfully")
    } catch {
      case e: Exception =>
        println(s"\n❌ ETLRunner: $jobName failed: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      spark.stop()
    }
  }

  /**
   * Run job with command-line style arguments
   */
  def runJobWithArgs(
    job: ETLJob,
    args: Array[String],
    appName: Option[String] = None
  ): Unit = {
    val config = ETLConfig.fromArgs(args)
    val configMap = Map(
      "mode" -> config.mode,
      "entity" -> config.entity.getOrElse(""),
      "date" -> config.snapshotDate.toString,
      "source" -> config.recordSource,
      "buildPIT" -> config.buildPIT.toString,
      "buildBridge" -> config.buildBridge.toString,
      "rebuildDims" -> config.rebuildDims.toString,
      "rebuildFacts" -> config.rebuildFacts.toString
    ).filter(_._2.nonEmpty)

    runJob(job, configMap, appName)
  }
}

