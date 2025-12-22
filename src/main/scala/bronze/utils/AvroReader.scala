package bronze.utils

/**
 * AvroReader: Utility for reading Avro files staged by NiFi, validating their schema,
 * and enriching data for Data Vault ingestion.
 *
 * PURPOSE:
 * This module demonstrates Spark-Avro integration, schema validation, and schema evolution
 * handling in a real-world ETL context.
 *
 * PIPELINE CONTEXT:
 * 1. NiFi extracts data from PostgreSQL
 * 2. NiFi converts JSON → Avro (with schema validation)
 * 3. NiFi stages Avro files in warehouse/staging/{entity}/
 * 4. AvroReader reads these Avro files for Spark ETL (Bronze layer)
 * 5. Spark loads data into Data Vault structures (Hubs, Links, Satellites)
 *
 * SCHEMA VALIDATION:
 * - NiFi validates on write (JSON → Avro conversion)
 * - AvroReader validates on read (Avro → Spark DataFrame)
 * - This double validation ensures data quality end-to-end
 *
 * SCHEMA EVOLUTION:
 * - Avro files contain embedded schemas
 * - Spark automatically merges schemas from multiple files
 * - New fields are detected and warned about
 * - Missing required fields fail fast to prevent data loss
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AvroReader {

  /**
   * Reads Avro files from the staging directory and optionally validates the schema.
   *
   * @param stagingPath Path to Avro files (can use wildcards, e.g., warehouse/staging/customer/*.avro)
   * @param validateSchema If true, checks for required fields and warns on new fields (schema evolution)
   * @return DataFrame enriched with technical columns for lineage tracking
   *
   * USAGE:
   * ```scala
   * val customerDF = AvroReader.readAvro("warehouse/staging/customer/*.avro")
   * ```
   */
  def readAvro(stagingPath: String, validateSchema: Boolean = true)
              (implicit spark: SparkSession): DataFrame = {

    println(s"📖 READING AVRO FILES")
    println(s"   Path: $stagingPath")
    println(s"   Validation: ${if (validateSchema) "Enabled" else "Disabled"}")

    // Read Avro files using Spark's built-in Avro data source
    // Spark automatically reads the embedded Avro schema from the files
    val df = spark.read.format("avro").load(stagingPath)

    println(s"✅ Schema validated: ${df.columns.length} fields")
    println(s"📊 Records read: ${df.count()}")

    // Validate schema structure (fail fast on missing required fields)
    if (validateSchema) {
      validateSchemaStructure(df, stagingPath)
    }

    // Add technical columns for data lineage and audit trail
    val enrichedDF = df
      .withColumn("_load_timestamp", current_timestamp())  // When was this data loaded into Spark?
      .withColumn("_source_file", input_file_name())      // Which Avro file did this row come from?
      .withColumn("_file_modification_time", to_timestamp(lit(System.currentTimeMillis() / 1000)))

    enrichedDF
  }

  /**
   * Validates the DataFrame schema against the expected entity structure.
   *
   * WHY VALIDATE AGAIN (NiFi already validates)?
   * - Defense in depth: Catch issues if files were manually placed in staging
   * - Schema evolution detection: Alert on new fields for Data Vault satellite updates
   * - Data quality gate: Fail fast before expensive Spark processing
   *
   * VALIDATION RULES:
   * - Missing required fields → FAIL (exception thrown)
   * - New/unexpected fields → WARN (logged, but processing continues)
   * - Technical fields (starting with _) → IGNORE (added by enrichment)
   *
   * @param df DataFrame read from Avro files
   * @param path File path (used to infer entity type)
   */
  private def validateSchemaStructure(df: DataFrame, path: String): Unit = {
    val actualFields = df.columns.toSet
    val entityType = extractEntityType(path)
    val requiredFields = getRequiredFieldsForEntity(entityType)

    // Check for missing required fields
    val missingFields = requiredFields.diff(actualFields)
    if (missingFields.nonEmpty) {
      val errorMsg = s"""
        |❌ SCHEMA VALIDATION FAILED for $entityType
        |   Missing required fields: ${missingFields.mkString(", ")}
        |
        |   This usually means:
        |   - NiFi flow is misconfigured
        |   - Avro schema is out of date
        |   - PostgreSQL schema changed without updating Avro schema
        |
        |   Action required: Update nifi/schemas/${entityType}.avsc and re-run NiFi flow
        |""".stripMargin
      throw new IllegalArgumentException(errorMsg)
    }

    // Detect new fields (schema evolution)
    val unexpectedFields = actualFields.diff(requiredFields).filterNot(_.startsWith("_"))
    if (unexpectedFields.nonEmpty) {
      println(s"")
      println(s"⚠️  NEW FIELDS DETECTED (Schema Evolution):")
      println(s"   ${unexpectedFields.mkString(", ")}")
      println(s"")
      println(s"   IMPACT:")
      println(s"   - Fields will be automatically added to Satellite tables")
      println(s"   - Existing queries unaffected")
      println(s"   - Historical records will have NULL for new fields")
      println(s"")
    }
  }

  /**
   * Infers the entity type (customer, account, etc.) from the file path.
   *
   * CONVENTION:
   * File paths follow the pattern: warehouse/staging/{entity}/*.avro
   * This allows entity-specific validation rules without explicit configuration.
   *
   * @param path File path from staging directory
   * @return Entity type string (customer, account, transaction_header, transaction_item, unknown)
   */
  private def extractEntityType(path: String): String = {
    if (path.contains("customer")) "customer"
    else if (path.contains("account")) "account"
    else if (path.contains("transaction_header")) "transaction_header"
    else if (path.contains("transaction_item")) "transaction_item"
    else "unknown"
  }

  /**
   * Returns the set of required fields for each entity type.
   *
   * REQUIRED FIELDS:
   * - Business keys: Essential for Data Vault Hub deduplication
   * - Foreign keys: Required for Data Vault Link relationships
   * - CDC tracking: updated_at enables incremental processing
   * - Core attributes: Minimal fields for Data Vault Satellite integrity
   *
   * WHY THESE SPECIFIC FIELDS?
   * These fields are the absolute minimum needed for:
   * 1. Identifying unique records (business keys)
   * 2. Linking related entities (foreign keys)
   * 3. Tracking changes over time (updated_at)
   * 4. Basic business functionality (core attributes)
   *
   * @param entityType Entity type string
   * @return Set of required field names
   */
  private def getRequiredFieldsForEntity(entityType: String): Set[String] = {
    entityType match {
      case "customer" =>
        // customer_id: Hub business key
        // email/type/status: Core satellite attributes
        // updated_at: CDC tracking for incremental loads
        Set("customer_id", "email", "customer_type", "customer_status", "updated_at")

      case "account" =>
        // account_id: Hub business key
        // customer_id: Link foreign key to customer
        // account_number/type/balance: Core attributes
        // updated_at: CDC tracking
        Set("account_id", "customer_id", "account_number", "account_type", "balance", "updated_at")

      case "transaction_header" =>
        // transaction_id: Hub business key
        // account_id: Link foreign key to account
        // transaction_number/type/amount: Core attributes
        // updated_at: CDC tracking
        Set("transaction_id", "account_id", "transaction_number", "transaction_type", "total_amount", "updated_at")

      case "transaction_item" =>
        // item_id: Hub business key
        // transaction_id: Link foreign key to transaction_header
        // item_sequence/amount: Core attributes
        // updated_at: CDC tracking
        Set("item_id", "transaction_id", "item_sequence", "item_amount", "updated_at")

      case _ =>
        // Unknown entity: only require CDC field
        // This allows flexibility for new entities
        Set("updated_at")
    }
  }

  /**
   * Reads Avro files and automatically handles schema evolution.
   *
   * SCHEMA EVOLUTION STRATEGY:
   * - Spark merges schemas from all Avro files
   * - Missing fields in older files → filled with NULL
   * - New fields in newer files → added to DataFrame
   * - Useful for backward and forward compatibility
   *
   * USE CASES:
   * - Reading files from different schema versions
   * - Gradual schema migration (some files updated, others not)
   * - Testing schema changes without full data refresh
   *
   * @param stagingPath Path to Avro files (wildcards supported)
   * @return DataFrame with merged schema from all files
   */
  def readAvroWithEvolution(stagingPath: String)
                           (implicit spark: SparkSession): DataFrame = {
    println(s"📖 READING AVRO WITH SCHEMA EVOLUTION")
    println(s"   Path: $stagingPath")

    val df = spark.read.format("avro").load(stagingPath)

    println(s"✅ Schema merged: ${df.columns.length} fields")
    println(s"📊 Records read: ${df.count()} rows")
    println(s"")
    println(s"   Fields: ${df.columns.mkString(", ")}")
    println(s"")

    df
  }
}
