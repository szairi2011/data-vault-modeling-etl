package bronze.utils

/**
 * ========================================================================
 * LOAD METADATA UTILITY
 * ========================================================================
 *
 * PURPOSE:
 * Track all ETL load operations in the load_metadata table for:
 * - Audit trail and compliance
 * - Performance monitoring
 * - Troubleshooting failed loads
 * - Data lineage tracking
 *
 * METADATA LIFECYCLE:
 * 1. startLoad() - Creates record with IN_PROGRESS status
 * 2. completeLoad() - Updates record with SUCCESS status and statistics
 * 3. failLoad() - Updates record with FAILED status and error message
 *
 * EXAMPLE USAGE:
 * ```scala
 * val loadId = LoadMetadata.startLoad("customer", "PostgreSQL", LocalDate.now())
 * try {
 *   // ETL processing...
 *   LoadMetadata.completeLoad(loadId, recordsExtracted, recordsLoaded)
 * } catch {
 *   case e: Exception =>
 *     LoadMetadata.failLoad(loadId, e.getMessage)
 *     throw e
 * }
 * ```
 * ========================================================================
 */

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import java.time.{LocalDate, LocalDateTime}
import java.sql.Timestamp

object LoadMetadata {

  // In-memory cache of load start times for duration calculation
  private val loadStartTimes = scala.collection.mutable.Map[Long, LocalDateTime]()

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ START LOAD - Initialize metadata record                        │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * @param entityName Entity being loaded (customer, account, etc.)
   * @param recordSource Source system (PostgreSQL, etc.)
   * @param loadDate Business date of load
   * @return Unique load ID for tracking
   */
  def startLoad(
      entityName: String,
      recordSource: String,
      loadDate: LocalDate
  )(implicit spark: SparkSession): Long = {

    // Generate unique load ID (timestamp-based)
    val loadId = System.currentTimeMillis()
    val startTime = LocalDateTime.now()

    // Cache start time for duration calculation
    loadStartTimes.put(loadId, startTime)

    println(s"""
         |📋 Starting load: $entityName
         |   Load ID: $loadId
         |   Source: $recordSource
         |   Date: $loadDate
         |""".stripMargin)

    // Create metadata record
    import spark.implicits._
    val metadataDF = Seq(
      (
        loadId,
        entityName,
        recordSource,
        java.sql.Date.valueOf(loadDate),
        Timestamp.valueOf(startTime),
        null: Timestamp, // load_end_timestamp
        "IN_PROGRESS",
        0L, // records_extracted
        0L, // records_loaded
        null: String, // error_message
        null: Integer // load_duration_seconds
      )
    ).toDF(
      "load_id",
      "entity_name",
      "record_source",
      "load_date",
      "load_start_timestamp",
      "load_end_timestamp",
      "load_status",
      "records_extracted",
      "records_loaded",
      "error_message",
      "load_duration_seconds"
    )

    // Write to load_metadata table
    try {
      metadataDF.write
        .format("iceberg")
        .mode("append")
        .saveAsTable("bronze.load_metadata")
    } catch {
      case e: Exception =>
        println(s"⚠️  Could not write to load_metadata: ${e.getMessage}")
        // Don't fail the ETL if metadata tracking fails
    }

    loadId
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ COMPLETE LOAD - Mark load as successful                        │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * @param loadId Load identifier from startLoad()
   * @param recordsExtracted Number of records read from source
   * @param recordsLoaded Number of records written to vault
   */
  def completeLoad(
      loadId: Long,
      recordsExtracted: Long,
      recordsLoaded: Long
  )(implicit spark: SparkSession): Unit = {

    val endTime = LocalDateTime.now()
    val startTime = loadStartTimes.getOrElse(loadId, endTime)
    val durationSeconds = java.time.Duration.between(startTime, endTime).getSeconds.toInt

    println(s"""
         |✅ Load completed: $loadId
         |   Extracted: $recordsExtracted records
         |   Loaded: $recordsLoaded records
         |   Duration: $durationSeconds seconds
         |""".stripMargin)

    // Update metadata record
    try {
      spark.sql(s"""
        UPDATE bronze.load_metadata
        SET
          load_end_timestamp = TIMESTAMP '${Timestamp.valueOf(endTime)}',
          load_status = 'SUCCESS',
          records_extracted = $recordsExtracted,
          records_loaded = $recordsLoaded,
          load_duration_seconds = $durationSeconds
        WHERE load_id = $loadId
      """)

      // Clean up cache
      val _ = loadStartTimes.remove(loadId)

    } catch {
      case e: Exception =>
        println(s"⚠️  Could not update load_metadata: ${e.getMessage}")
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ FAIL LOAD - Mark load as failed with error message             │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * @param loadId Load identifier from startLoad()
   * @param errorMessage Error message to record
   */
  def failLoad(
      loadId: Long,
      errorMessage: String
  )(implicit spark: SparkSession): Unit = {

    val endTime = LocalDateTime.now()
    val startTime = loadStartTimes.getOrElse(loadId, endTime)
    val durationSeconds = java.time.Duration.between(startTime, endTime).getSeconds.toInt

    println(s"""
         |❌ Load failed: $loadId
         |   Error: $errorMessage
         |   Duration: $durationSeconds seconds
         |""".stripMargin)

    // Escape single quotes in error message for SQL
    val escapedMessage = errorMessage.replace("'", "''")

    // Update metadata record
    try {
      spark.sql(s"""
        UPDATE bronze.load_metadata
        SET
          load_end_timestamp = TIMESTAMP '${Timestamp.valueOf(endTime)}',
          load_status = 'FAILED',
          error_message = '$escapedMessage',
          load_duration_seconds = $durationSeconds
        WHERE load_id = $loadId
      """)

      // Clean up cache
      val _ = loadStartTimes.remove(loadId)

    } catch {
      case e: Exception =>
        println(s"⚠️  Could not update load_metadata: ${e.getMessage}")
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ GET LAST SUCCESSFUL LOAD - For incremental processing          │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * @param entityName Entity name
   * @return Timestamp of last successful load, or None if never loaded
   */
  def getLastSuccessfulLoad(entityName: String)
                           (implicit spark: SparkSession): Option[Timestamp] = {
    try {
      val result = spark.sql(s"""
        SELECT MAX(load_end_timestamp) as last_load
        FROM bronze.load_metadata
        WHERE entity_name = '$entityName'
          AND load_status = 'SUCCESS'
      """).first()

      Option(result.getAs[Timestamp]("last_load"))

    } catch {
      case e: Exception =>
        println(s"⚠️  Could not query load_metadata: ${e.getMessage}")
        None
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ SHOW RECENT LOADS - For monitoring                             │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def showRecentLoads(limit: Int = 10)(implicit spark: SparkSession): Unit = {
    println(s"""
         |┌────────────────────────────────────────────────────────────────┐
         |│ RECENT ETL LOADS (Last $limit)                                │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    try {
      spark.sql(s"""
        SELECT
          load_id,
          entity_name,
          load_status,
          records_extracted,
          records_loaded,
          load_duration_seconds,
          load_start_timestamp
        FROM bronze.load_metadata
        ORDER BY load_start_timestamp DESC
        LIMIT $limit
      """).show(limit, truncate = false)
    } catch {
      case e: Exception =>
        println(s"⚠️  Could not query load_metadata: ${e.getMessage}")
    }
  }
}
