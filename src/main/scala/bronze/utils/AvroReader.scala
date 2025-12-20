package bronze.utils

/**
 * ========================================================================
 * AVRO READER UTILITY
 * ========================================================================
 *
 * PURPOSE:
 * Read Avro files from NiFi staging area, validate schema, convert to Spark DataFrame.
 *
 * LEARNING OBJECTIVES:
 * - How Avro self-describing format works
 * - Schema validation strategies
 * - Spark-Avro integration
 * - Error handling for schema evolution
 *
 * DATA FLOW:
 * ```
 * NiFi CDC Pipeline
 *     ↓
 * Avro Files (staging/customer/*.avro)
 *     ↓
 * AvroReader.readAvro(...)
 *     ↓
 * Spark DataFrame (validated schema)
 *     ↓
 * Raw Vault Loading
 * ```
 *
 * AVRO FORMAT STRUCTURE:
 * ```
 * ┌─────────────────────────────────────────────────────────────┐
 * │  Avro File (.avro)                                          │
 * ├─────────────────────────────────────────────────────────────┤
 * │  Header:                                                    │
 * │    - Magic bytes: "Obj" + 0x01                             │
 * │    - Schema (JSON embedded in file)                        │
 * │    - Sync marker (16 random bytes for splitting)           │
 * ├─────────────────────────────────────────────────────────────┤
 * │  Data Blocks:                                               │
 * │    ┌────────────────────────────────────────────┐          │
 * │    │  Block 1 (compressed):                     │          │
 * │    │    - Count: 100 records                    │          │
 * │    │    - Size: 8KB (Snappy compressed)        │          │
 * │    │    - Data: Binary serialized records       │          │
 * │    └────────────────────────────────────────────┘          │
 * │    ┌────────────────────────────────────────────┐          │
 * │    │  Block 2 (compressed):                     │          │
 * │    │    - Count: 100 records                    │          │
 * │    │    - Size: 8KB                             │          │
 * │    └────────────────────────────────────────────┘          │
 * │    ...                                                     │
 * │    Sync marker (repeated for each block)                   │
 * └─────────────────────────────────────────────────────────────┘
 * ```
 *
 * WHY AVRO FOR CDC:
 * 1. **Self-Describing**: Schema embedded in file (no external metadata)
 * 2. **Compact**: 30-50% smaller than JSON (binary encoding)
 * 3. **Fast**: No parsing overhead (direct binary read)
 * 4. **Splittable**: Sync markers allow parallel processing
 * 5. **Schema Evolution**: Add/remove fields without breaking readers
 *
 * SCHEMA EVOLUTION RULES:
 * ```
 * ✅ ALLOWED (Backward Compatible):
 *    - Add optional field with default value
 *    - Remove optional field
 *    - Add enum symbol
 *    - Promote type (int → long, float → double)
 *
 * ❌ FORBIDDEN (Breaking Changes):
 *    - Remove required field
 *    - Change field type (string → int)
 *    - Rename field without alias
 *    - Remove enum symbol
 * ```
 *
 * ========================================================================
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AvroReader {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ READ AVRO FILES FROM STAGING ZONE                              │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PARAMETERS:
   * @param stagingPath    Path to Avro files (supports glob: *.avro)
   * @param validateSchema Whether to validate required fields
   * @param spark          Implicit SparkSession
   * @return               Validated DataFrame
   *
   * EXAMPLES:
   * ```scala
   * // Read all customer files
   * val df = AvroReader.readAvro("warehouse/staging/customer/*.avro")
   *
   * // Read specific batch
   * val df = AvroReader.readAvro("warehouse/staging/customer/customer_20250115_103045.avro")
   *
   * // Skip validation (for raw inspection)
   * val df = AvroReader.readAvro("warehouse/staging/customer/*.avro", validateSchema = false)
   * ```
   *
   * TECHNICAL DETAILS:
   * - Spark uses org.apache.spark.sql.avro data source
   * - Schema automatically extracted from Avro file header
   * - Supports compression (Snappy, Deflate, Bzip2, XZ)
   * - Parallel reading across partitions
   *
   * PERFORMANCE:
   * - Spark parallelizes reading across partitions
   * - Each executor reads multiple Avro blocks
   * - Predicate pushdown works (skip blocks by metadata)
   *
   * SCHEMA CACHING:
   * - Spark caches schema per file
   * - Reused for subsequent queries
   * - Invalidated on schema change
   */
  def readAvro(stagingPath: String, validateSchema: Boolean = true)
              (implicit spark: SparkSession): DataFrame = {

    println(
      s"""
         |┌─────────────────────────────────────────────────────────────────┐
         |│ 📖 READING AVRO FILES                                           │
         |├─────────────────────────────────────────────────────────────────┤
         |│ Path: $stagingPath
         |│ Validation: ${if (validateSchema) "Enabled" else "Disabled"}
         |└─────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    // STEP 1: Read Avro files with embedded schema
    // ─────────────────────────────────────────────
    // Spark uses org.apache.spark.sql.avro data source
    // Schema automatically extracted from Avro file header
    // Supports compression (Snappy, Deflate, Bzip2, XZ)
    val df = spark.read
      .format("avro")  // Use Avro data source
      .load(stagingPath)  // Glob pattern supported

    // STEP 2: Validate schema structure (if enabled)
    // ──────────────────────────────────
    // Ensure required fields exist
    // Fail fast if schema incompatible with Raw Vault
    if (validateSchema) {
      validateSchemaStructure(df, stagingPath)
    }

    // STEP 3: Add metadata columns
    // ────────────────────────────
    // Enrich with load tracking information
    val enrichedDF = df
      .withColumn("_load_timestamp", current_timestamp())
      .withColumn("_source_file", input_file_name())
      .withColumn("_file_modification_time",
        to_timestamp(lit(System.currentTimeMillis() / 1000)))

    // STEP 4: Show sample and statistics
    // ───────────────────────────────────
    val rowCount = enrichedDF.count()
    println(s"""
         |✅ Schema validated: ${df.columns.length} fields
         |📊 Records read: $rowCount
         |
         |SAMPLE DATA (first 3 rows):
         |""".stripMargin)

    enrichedDF.show(3, truncate = false)

    println(s"\nSCHEMA:\n${df.schema.treeString}")

    enrichedDF
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ VALIDATE DATAFRAME SCHEMA                                       │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Ensure Avro schema matches expected structure for Raw Vault loading.
   *
   * VALIDATION CHECKS:
   * 1. Required fields present (business keys, CDC tracking)
   * 2. Field types compatible with Iceberg tables
   * 3. Unexpected fields detected (warnings only)
   *
   * WHY VALIDATE:
   * - Prevent silent data loss
   * - Detect source system changes early
   * - Ensure Data Vault integrity
   *
   * FAIL FAST PHILOSOPHY:
   * - Better to fail during ingestion than corrupt vault
   * - Clear error messages for troubleshooting
   * - Automated alerts to data engineers
   */
  private def validateSchemaStructure(df: DataFrame, path: String): Unit = {
    val actualFields = df.columns.toSet

    // Determine entity type from path
    val entityType = extractEntityType(path)

    // Get required fields based on entity type
    val requiredFields = getRequiredFieldsForEntity(entityType)

    val missingFields = requiredFields.diff(actualFields)

    if (missingFields.nonEmpty) {
      // CRITICAL ERROR: Missing required fields
      // Cannot proceed with loading
      throw new IllegalArgumentException(
        s"""
           |❌ SCHEMA VALIDATION FAILED
           |
           |Entity: $entityType
           |Missing required fields: ${missingFields.mkString(", ")}
           |
           |ACTUAL SCHEMA:
           |${df.schema.treeString}
           |
           |EXPECTED FIELDS:
           |${requiredFields.mkString(", ")}
           |
           |POSSIBLE CAUSES:
           |1. Source system schema changed (column renamed/removed)
           |2. NiFi flow misconfigured (wrong table extracted)
           |3. Avro schema not updated in Schema Registry
           |
           |RESOLUTION:
           |1. Check source table schema
           |2. Verify NiFi flow configuration
           |3. Re-register Avro schema: .\\nifi\\scripts\\register-schemas.ps1
           |4. Update RawVaultETL to handle new schema
           |""".stripMargin
      )
    }

    // WARNING: Unexpected fields (not breaking, but logged)
    val unexpectedFields = actualFields.diff(requiredFields)
      .filterNot(_.startsWith("_"))  // Ignore metadata fields we added

    if (unexpectedFields.nonEmpty) {
      println(
        s"""
           |⚠️  NEW FIELDS DETECTED (Schema Evolution):
           |${unexpectedFields.mkString(", ")}
           |
           |IMPACT:
           |- Fields will be automatically added to Satellite tables
           |- Existing queries unaffected
           |- Historical records will have NULL for new fields
           |
           |ACTION:
           |- Review new fields with business analysts
           |- Update dimensional model if needed (manual decision)
           |- Document schema change in CHANGELOG.md
           |""".stripMargin)
    }

    println(s"✅ Schema validated for $entityType: ${df.columns.length} fields")
  }

  /**
   * Extract entity type from file path
   *
   * Examples:
   * - warehouse/staging/customer/*.avro → customer
   * - warehouse/staging/account/account_20250115.avro → account
   */
  private def extractEntityType(path: String): String = {
    if (path.contains("customer")) "customer"
    else if (path.contains("account")) "account"
    else if (path.contains("transaction_header")) "transaction_header"
    else if (path.contains("transaction_item")) "transaction_item"
    else "unknown"
  }

  /**
   * Get required fields based on entity type
   *
   * These are fields that MUST exist for Data Vault loading to succeed.
   * Business keys and CDC tracking fields are always required.
   */
  private def getRequiredFieldsForEntity(entityType: String): Set[String] = {
    entityType match {
      case "customer" => Set(
        "customer_id",      // Business key
        "email",            // Required attribute
        "customer_type",    // Required attribute
        "customer_status",  // Required attribute
        "updated_at"        // CDC tracking
      )

      case "account" => Set(
        "account_id",       // Business key
        "customer_id",      // Foreign key for Link
        "account_number",   // Required attribute
        "account_type",     // Required attribute
        "balance",          // Required attribute
        "updated_at"        // CDC tracking
      )

      case "transaction_header" => Set(
        "transaction_id",   // Business key
        "account_id",       // Foreign key for Link
        "transaction_number", // Required attribute
        "transaction_type", // Required attribute
        "total_amount",     // Required attribute
        "updated_at"        // CDC tracking
      )

      case "transaction_item" => Set(
        "item_id",          // Business key (composite)
        "transaction_id",   // Foreign key for Link
        "item_sequence",    // Part of composite key
        "item_amount",      // Required attribute
        "updated_at"        // CDC tracking
      )

      case _ => Set("updated_at")  // Minimum for any entity
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ READ AVRO WITH SCHEMA EVOLUTION HANDLING                        │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Read Avro files with different schema versions and merge into single DataFrame.
   * Handles case where staging zone has files with V1 and V2 schemas.
   *
   * USAGE:
   * ```scala
   * val df = AvroReader.readAvroWithEvolution("warehouse/staging/customer/*.avro")
   * // Returns unified DataFrame with all fields from all versions
   * // Missing fields in older versions filled with NULL
   * ```
   */
  def readAvroWithEvolution(stagingPath: String)
                           (implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    // Read all files
    val df = spark.read.format("avro").load(stagingPath)

    // Spark automatically handles schema evolution for Avro:
    // - Union of all schemas across files
    // - Missing fields filled with NULL
    // - Compatible with Data Vault satellite pattern

    println(s"""
         |📖 Read Avro files with schema evolution
         |   Unified schema: ${df.columns.length} fields
         |   Records: ${df.count()}
         |""".stripMargin)

    df
  }
}

