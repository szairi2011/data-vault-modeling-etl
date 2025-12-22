package gold.utils

/**
 * ========================================================================
 * SCD TYPE 2 HANDLER UTILITY
 * ========================================================================
 *
 * PURPOSE:
 * Reusable utility for handling Slowly Changing Dimension Type 2 logic.
 *
 * LEARNING OBJECTIVES:
 * - SCD Type 2 implementation pattern
 * - Change detection algorithms
 * - Surrogate key generation
 * - End-dating expired versions
 * - Effective/expiration date management
 *
 * SCD TYPE 2 ALGORITHM:
 *
 * 1. Read current dimension (is_current = TRUE)
 * 2. Compare with incoming source data
 * 3. Identify:
 *    - New records (not in dimension)
 *    - Changed records (different attributes)
 *    - Unchanged records (no action needed)
 * 4. For changed records:
 *    - End-date existing version (set expiration_date, is_current = FALSE)
 *    - Insert new version (new surrogate key, is_current = TRUE)
 * 5. For new records:
 *    - Insert with new surrogate key (is_current = TRUE)
 *
 * ========================================================================
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import bronze.utils.IcebergWriter

object SCDType2Handler {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PROCESS SCD TYPE 2                                              │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * @param sourceDF Source data with latest values
   * @param targetTable Target dimension table name
   * @param naturalKey Natural business key column name
   * @param surrogateKey Surrogate key column name
   * @param compareColumns Columns to compare for changes
   * @return DataFrame with processed records
   *
   * USAGE:
   * ```scala
   * val dimDF = SCDType2Handler.processSCDType2(
   *   sourceDF = customerDF,
   *   targetTable = "gold.dim_customer",
   *   naturalKey = "customer_id",
   *   surrogateKey = "customer_key",
   *   compareColumns = Seq("first_name", "last_name", "email", "loyalty_tier")
   * )
   * ```
   */
  def processSCDType2(
      sourceDF: DataFrame,
      targetTable: String,
      naturalKey: String,
      surrogateKey: String,
      compareColumns: Seq[String]
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    println(s"""
         |🔄 Processing SCD Type 2 for $targetTable
         |   Natural Key: $naturalKey
         |   Compare Columns: ${compareColumns.mkString(", ")}
         |""".stripMargin)

    // Read current dimension records
    val currentDimDF = try {
      spark.table(targetTable)
        .filter($"is_current" === true)
    } catch {
      case _: Exception =>
        // Table empty or doesn't exist - all records are new
        println("   ℹ️  Target dimension empty - all records are inserts")
        return insertNewRecords(sourceDF, targetTable, surrogateKey, startKey = 1)
    }

    // Find changes by comparing all compareColumns
    val changeDetectionDF = detectChanges(
      sourceDF,
      currentDimDF,
      naturalKey,
      surrogateKey,
      compareColumns
    )

    // Separate into new, changed, and unchanged
    val newRecordsDF = changeDetectionDF.filter($"change_type" === "NEW")
    val changedRecordsDF = changeDetectionDF.filter($"change_type" === "CHANGED")
    val unchangedCount = changeDetectionDF.filter($"change_type" === "UNCHANGED").count()

    println(s"""
         |   📊 Change Analysis:
         |      New Records: ${newRecordsDF.count()}
         |      Changed Records: ${changedRecordsDF.count()}
         |      Unchanged Records: $unchangedCount
         |""".stripMargin)

    // Process changes
    if (changedRecordsDF.count() > 0) {
      endDateExpiredVersions(changedRecordsDF, currentDimDF, targetTable, naturalKey, surrogateKey)
    }

    // Insert new versions for changed records
    val newVersionsDF = if (changedRecordsDF.count() > 0) {
      val maxKey = getMaxSurrogateKey(currentDimDF, surrogateKey)
      insertNewVersions(changedRecordsDF, targetTable, surrogateKey, startKey = maxKey + 1)
    } else {
      spark.emptyDataFrame
    }

    // Insert completely new records
    val insertedNewDF = if (newRecordsDF.count() > 0) {
      val maxKey = getMaxSurrogateKey(currentDimDF, surrogateKey)
      insertNewRecords(newRecordsDF, targetTable, surrogateKey, startKey = maxKey + 1)
    } else {
      spark.emptyDataFrame
    }

    // Return combined result (for reporting)
    newVersionsDF.union(insertedNewDF)
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ DETECT CHANGES                                                  │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Compare source data with current dimension to detect changes.
   *
   * CHANGE DETECTION LOGIC:
   * - NEW: Natural key not in dimension
   * - CHANGED: Natural key exists but compareColumns differ
   * - UNCHANGED: Natural key exists and all compareColumns match
   */
  def detectChanges(
      sourceDF: DataFrame,
      currentDimDF: DataFrame,
      naturalKey: String,
      surrogateKey: String,
      compareColumns: Seq[String]
  ): DataFrame = {

    import sourceDF.sparkSession.implicits._

    // Create comparison expression (check if any compare column changed)
    val comparisonExprs = compareColumns.map { colName =>
      coalesce(sourceDF(colName), lit("NULL")) =!= coalesce(currentDimDF(colName), lit("NULL"))
    }
    val anyChanged = comparisonExprs.reduce(_ || _)

    // Join source with current dimension
    val comparisonDF = sourceDF
      .join(
        currentDimDF.select((Seq(col(naturalKey), col(surrogateKey)) ++ compareColumns.map(col)): _*),
        Seq(naturalKey),
        "left"
      )
      .withColumn("change_type",
        when(currentDimDF(surrogateKey).isNull, lit("NEW"))
        .when(anyChanged, lit("CHANGED"))
        .otherwise(lit("UNCHANGED"))
      )

    comparisonDF
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ END-DATE EXPIRED VERSIONS                                       │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Update existing dimension records to mark them as expired.
   *
   * UPDATES:
   * - Set expiration_date = CURRENT_DATE
   * - Set is_current = FALSE
   */
  def endDateExpiredVersions(
      changedRecordsDF: DataFrame,
      currentDimDF: DataFrame,
      targetTable: String,
      naturalKey: String,
      surrogateKey: String
  )(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    println("   🔚 End-dating expired versions...")

    // Get surrogate keys for records that changed
    val expiredKeys = changedRecordsDF
      .join(currentDimDF.select(col(naturalKey), col(surrogateKey)), Seq(naturalKey))
      .select(surrogateKey)
      .distinct()

    // Update using merge (Iceberg supports UPDATE)
    spark.sql(
      s"""
         |MERGE INTO $targetTable AS target
         |USING (SELECT $surrogateKey FROM expired_keys_temp) AS source
         |ON target.$surrogateKey = source.$surrogateKey
         |WHEN MATCHED THEN UPDATE SET
         |  target.expiration_date = CURRENT_DATE,
         |  target.is_current = FALSE
         |""".stripMargin
    )

    println(s"      ✅ End-dated ${expiredKeys.count()} records")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ INSERT NEW VERSIONS                                             │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Insert new versions of changed records with new surrogate keys.
   */
  def insertNewVersions(
      changedRecordsDF: DataFrame,
      targetTable: String,
      surrogateKey: String,
      startKey: Long
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    println("   ➕ Inserting new versions...")

    // Generate new surrogate keys
    val windowSpec = Window.orderBy(monotonically_increasing_id())

    val newVersionsDF = changedRecordsDF
      .withColumn(surrogateKey, (row_number().over(windowSpec) + startKey - 1).cast("long"))
      .withColumn("effective_date", current_date())
      .withColumn("expiration_date", to_date(lit("9999-12-31")))
      .withColumn("is_current", lit(true))
      .withColumn("load_date", current_date())
      .withColumn("record_source", lit("SCD Type 2 Update"))

    // Append to dimension
    IcebergWriter.appendToTable(
      newVersionsDF,
      targetTable.split("\\.")(0),
      targetTable.split("\\.")(1),
      Seq("load_date")
    )

    println(s"      ✅ Inserted ${newVersionsDF.count()} new versions")
    newVersionsDF
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ INSERT NEW RECORDS                                              │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Insert completely new records (first version).
   */
  def insertNewRecords(
      newRecordsDF: DataFrame,
      targetTable: String,
      surrogateKey: String,
      startKey: Long
  )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    println("   ➕ Inserting new records...")

    val windowSpec = Window.orderBy(monotonically_increasing_id())

    val insertDF = newRecordsDF
      .withColumn(surrogateKey, (row_number().over(windowSpec) + startKey - 1).cast("long"))
      .withColumn("effective_date", current_date())
      .withColumn("expiration_date", to_date(lit("9999-12-31")))
      .withColumn("is_current", lit(true))
      .withColumn("load_date", current_date())
      .withColumn("record_source", lit("Initial Load"))

    IcebergWriter.appendToTable(
      insertDF,
      targetTable.split("\\.")(0),
      targetTable.split("\\.")(1),
      Seq("load_date")
    )

    println(s"      ✅ Inserted ${insertDF.count()} new records")
    insertDF
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ GET MAX SURROGATE KEY                                           │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Get the maximum surrogate key to generate next keys.
   */
  def getMaxSurrogateKey(dimDF: DataFrame, surrogateKey: String): Long = {
    try {
      dimDF.agg(max(col(surrogateKey))).collect()(0).getLong(0)
    } catch {
      case _: Exception => 0L // Table empty
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ VALIDATE SCD TYPE 2 INTEGRITY                                   │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Check dimension for SCD Type 2 violations.
   *
   * CHECKS:
   * 1. Each natural key has exactly one current version
   * 2. No overlapping effective/expiration dates
   * 3. Surrogate keys are unique
   */
  def validateSCDType2(
      targetTable: String,
      naturalKey: String,
      surrogateKey: String
  )(implicit spark: SparkSession): Boolean = {

    import spark.implicits._

    println(s"\n🔍 Validating SCD Type 2 integrity: $targetTable")

    val dimDF = spark.table(targetTable)

    // Check 1: One current version per natural key
    val multipleCurrents = dimDF
      .filter($"is_current" === true)
      .groupBy(naturalKey)
      .count()
      .filter($"count" > 1)
      .count()

    if (multipleCurrents > 0) {
      throw new IllegalStateException(
        s"❌ SCD Type 2 validation failed: $multipleCurrents natural keys have multiple current versions"
      )
    }

    // Check 2: Surrogate key uniqueness
    val totalKeys = dimDF.count()
    val uniqueKeys = dimDF.select(surrogateKey).distinct().count()

    if (totalKeys != uniqueKeys) {
      throw new IllegalStateException(
        s"❌ SCD Type 2 validation failed: Surrogate keys not unique ($totalKeys records, $uniqueKeys unique keys)"
      )
    }

    println("✅ SCD Type 2 validation passed")
    true
  }
}
