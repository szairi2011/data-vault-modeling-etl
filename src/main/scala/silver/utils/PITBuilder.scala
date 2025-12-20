package silver.utils

/**
 * ========================================================================
 * POINT-IN-TIME (PIT) BUILDER UTILITY
 * ========================================================================
 *
 * PURPOSE:
 * Reusable utility for generating PIT snapshots from any Hub-Satellite pair.
 *
 * LEARNING OBJECTIVES:
 * - Generic PIT generation algorithm
 * - Window functions for temporal data
 * - Performance optimization strategies
 * - Snapshot date range generation
 *
 * PIT ALGORITHM:
 * ```
 * For each snapshot_date in date_range:
 *   For each hub_key:
 *     Find satellite record where:
 *       - valid_from <= snapshot_date
 *       - (valid_to > snapshot_date OR valid_to IS NULL)
 *     Take the LATEST such record (ORDER BY valid_from DESC, LIMIT 1)
 * ```
 *
 * OPTIMIZATION STRATEGIES:
 * 1. **Window Functions**: Avoid correlated subqueries
 * 2. **Partitioning**: Process by snapshot_date partition
 * 3. **Caching**: Cache hub DataFrame for multiple dates
 * 4. **Broadcast**: Broadcast small dimension tables
 *
 * ========================================================================
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.time.LocalDate

object PITBuilder {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BUILD PIT TABLE FOR SINGLE SNAPSHOT DATE                       │
   * └─────────────────────────────────────────────��───────────────────┘
   *
   * @param hubTable Hub table name (e.g., "bronze.hub_customer")
   * @param satTable Satellite table name (e.g., "bronze.sat_customer")
   * @param hubKey Hub hash key column name
   * @param businessKey Business key column name (for convenience)
   * @param snapshotDate Date for this snapshot
   * @param spark Implicit SparkSession
   * @return DataFrame with PIT snapshot
   *
   * USAGE:
   * ```scala
   * val pitDF = PITBuilder.buildSnapshot(
   *   "bronze.hub_customer",
   *   "bronze.sat_customer",
   *   "customer_hash_key",
   *   "customer_id",
   *   LocalDate.of(2025, 1, 15)
   * )
   * ```
   */
  def buildSnapshot(
      hubTable: String,
      satTable: String,
      hubKey: String,
      businessKey: String,
      snapshotDate: LocalDate
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    println(s"""
         |📸 Building PIT snapshot:
         |   Hub: $hubTable
         |   Satellite: $satTable
         |   Date: $snapshotDate
         |""".stripMargin)

    // Read hub (all keys)
    val hubDF = spark.table(hubTable)

    // Read satellite (filter for records valid on or before snapshot_date)
    val satDF = spark.table(satTable)
      .filter($"valid_from" <= lit(snapshotDate.toString))

    // Window function to get latest satellite version per hub key
    val windowSpec = Window
      .partitionBy(hubKey)
      .orderBy($"valid_from".desc)

    val latestSatDF = satDF
      .withColumn("rn", row_number().over(windowSpec))
      .filter($"rn" === 1)
      .drop("rn")

    // Join hub + latest satellite
    val pitDF = hubDF
      .join(latestSatDF, Seq(hubKey), "inner")
      .withColumn("snapshot_date", lit(snapshotDate.toString).cast("date"))
      .withColumn("load_date", current_date())

    val rowCount = pitDF.count()
    println(s"   ✅ Generated $rowCount PIT records")

    pitDF
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BUILD PIT TABLE FOR DATE RANGE (BATCH)                         │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * @param hubTable Hub table name
   * @param satTable Satellite table name
   * @param hubKey Hub hash key column
   * @param businessKey Business key column
   * @param startDate Start of date range (inclusive)
   * @param endDate End of date range (inclusive)
   * @return DataFrame with all snapshots in range
   *
   * PERFORMANCE:
   * - Generates all snapshots in single pass
   * - More efficient than calling buildSnapshot repeatedly
   * - Uses cross join with date spine
   *
   * USAGE:
   * ```scala
   * val pitDF = PITBuilder.buildSnapshotRange(
   *   "bronze.hub_customer",
   *   "bronze.sat_customer",
   *   "customer_hash_key",
   *   "customer_id",
   *   LocalDate.of(2025, 1, 1),
   *   LocalDate.of(2025, 1, 31)
   * )
   * ```
   */
  def buildSnapshotRange(
      hubTable: String,
      satTable: String,
      hubKey: String,
      businessKey: String,
      startDate: LocalDate,
      endDate: LocalDate
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    println(s"""
         |📸 Building PIT snapshot range:
         |   Hub: $hubTable
         |   Satellite: $satTable
         |   Date Range: $startDate to $endDate
         |""".stripMargin)

    // Generate date spine (all dates in range)
    val dateSpine = generateDateSpine(startDate, endDate)

    // Read hub
    val hubDF = spark.table(hubTable)

    // Read satellite
    val satDF = spark.table(satTable)

    // Cross join hub with date spine
    val hubDateDF = hubDF.crossJoin(dateSpine)

    // Join with satellite, keeping only valid records for each date
    val pitDF = hubDateDF
      .join(
        satDF,
        hubDateDF(hubKey) === satDF(hubKey) &&
        satDF("valid_from") <= hubDateDF("snapshot_date") &&
        (satDF("valid_to") > hubDateDF("snapshot_date") || satDF("valid_to").isNull),
        "inner"
      )
      .select(
        hubDateDF("*"),
        satDF("*")
      )
      .drop(satDF(hubKey)) // Avoid duplicate column
      .withColumn("load_date", current_date())

    val rowCount = pitDF.count()
    println(s"   ✅ Generated $rowCount PIT records across date range")

    pitDF
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ GENERATE DATE SPINE (DATE DIMENSION)                           │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Create DataFrame with all dates in range (one row per date).
   *
   * RESULT:
   * ```
   * snapshot_date
   * -------------
   * 2025-01-01
   * 2025-01-02
   * 2025-01-03
   * ...
   * ```
   */
  def generateDateSpine(startDate: LocalDate, endDate: LocalDate)
                       (implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val dates = Iterator.iterate(startDate)(_.plusDays(1))
      .takeWhile(d => d.isBefore(endDate) || d.isEqual(endDate))
      .toSeq

    val dateSpineDF = dates.toDF("snapshot_date")
      .withColumn("snapshot_date", $"snapshot_date".cast("date"))

    println(s"   📅 Generated date spine: ${dates.size} dates")
    dateSpineDF
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BUILD PIT WITH CUSTOM BUSINESS LOGIC                           │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Build PIT with custom transformations applied.
   *
   * @param hubTable Hub table name
   * @param satTable Satellite table name
   * @param hubKey Hub hash key column
   * @param snapshotDate Snapshot date
   * @param transformFunc Custom transformation function
   * @return Transformed PIT DataFrame
   *
   * USAGE:
   * ```scala
   * val pitDF = PITBuilder.buildWithTransform(
   *   "bronze.hub_customer",
   *   "bronze.sat_customer",
   *   "customer_hash_key",
   *   LocalDate.now(),
   *   df => df.withColumn("full_name",
   *     concat($"first_name", lit(" "), $"last_name"))
   * )
   * ```
   */
  def buildWithTransform(
      hubTable: String,
      satTable: String,
      hubKey: String,
      businessKey: String,
      snapshotDate: LocalDate,
      transformFunc: DataFrame => DataFrame
  )(implicit spark: SparkSession): DataFrame = {

    val basePIT = buildSnapshot(hubTable, satTable, hubKey, businessKey, snapshotDate)
    val transformedPIT = transformFunc(basePIT)

    println("   🔧 Applied custom transformations")
    transformedPIT
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ VALIDATE PIT INTEGRITY                                          │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Check PIT table for data quality issues.
   *
   * CHECKS:
   * 1. Each hub key has exactly one record per snapshot_date
   * 2. No NULL values in critical columns
   * 3. valid_from <= snapshot_date (sanity check)
   *
   * @param pitDF PIT DataFrame to validate
   * @param hubKey Hub key column name
   * @return True if valid, throws exception if issues found
   */
  def validatePIT(pitDF: DataFrame, hubKey: String): Boolean = {

    import pitDF.sparkSession.implicits._

    println("\n🔍 Validating PIT integrity...")

    // Check 1: Unique key per snapshot_date
    val duplicates = pitDF
      .groupBy(hubKey, "snapshot_date")
      .count()
      .filter($"count" > 1)
      .count()

    if (duplicates > 0) {
      throw new IllegalStateException(
        s"❌ PIT validation failed: Found $duplicates duplicates (hub_key, snapshot_date)"
      )
    }

    // Check 2: No NULLs in key columns
    val nullKeys = pitDF
      .filter(col(hubKey).isNull || $"snapshot_date".isNull)
      .count()

    if (nullKeys > 0) {
      throw new IllegalStateException(
        s"❌ PIT validation failed: Found $nullKeys NULL keys"
      )
    }

    // Check 3: valid_from sanity check
    val invalidDates = pitDF
      .filter($"valid_from" > $"snapshot_date")
      .count()

    if (invalidDates > 0) {
      throw new IllegalStateException(
        s"❌ PIT validation failed: Found $invalidDates records with valid_from > snapshot_date"
      )
    }

    println("✅ PIT validation passed")
    true
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ GET PIT STATISTICS                                              │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Show summary statistics for PIT table.
   */
  def showPITStats(pitTable: String)(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    println(s"""
         |┌────────────────────────────────────────────────────────────────┐
         |│ PIT TABLE STATISTICS: $pitTable
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    val df = spark.table(pitTable)

    // Overall stats
    val totalRecords = df.count()
    val distinctDates = df.select("snapshot_date").distinct().count()
    val distinctKeys = df.select(df.columns.head).distinct().count()

    println(s"""
         |Total Records: $totalRecords
         |Distinct Snapshot Dates: $distinctDates
         |Distinct Keys: $distinctKeys
         |Avg Records per Date: ${totalRecords / distinctDates}
         |""".stripMargin)

    // Date range
    println("Date Range:")
    df.select(
      min("snapshot_date").as("earliest"),
      max("snapshot_date").as("latest")
    ).show(false)

    // Sample data
    println("Sample Records:")
    df.orderBy($"snapshot_date".desc).show(5, truncate = false)
  }
}

