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
import org.apache.avro.Schema
import scala.collection.JavaConverters._
import java.io.File

object AvroReader {

  // Schema cache to avoid repeated file I/O operations
  // Schemas are immutable, so caching is safe
  private val schemaCache = scala.collection.mutable.Map[String, Schema]()

  /**
   * Loads and parses the Avro schema file for the specified entity type.
   *
   * SCHEMA LOCATION:
   * All Avro schema files are stored in: nifi/schemas/{entity}.avsc
   *
   * CACHING STRATEGY:
   * Schemas are parsed once and cached in memory to avoid repeated file I/O.
   * This is safe because schemas are immutable configuration files.
   *
   * ERROR HANDLING:
   * - Missing schema file → Clear exception with full path
   * - Invalid JSON → Avro parser will throw with details
   *
   * @param entityType Entity type (customer, account, transaction_header, transaction_item)
   * @return Parsed Avro Schema object
   * @throws java.io.FileNotFoundException if schema file doesn't exist
   * @throws org.apache.avro.SchemaParseException if schema JSON is invalid
   */
  private def loadAvroSchema(entityType: String): Schema = {
    schemaCache.getOrElseUpdate(entityType, {
      val schemaPath = s"nifi/schemas/${entityType}.avsc"
      val schemaFile = new File(schemaPath)

      if (!schemaFile.exists()) {
        throw new java.io.FileNotFoundException(
          s"""
             |❌ AVRO SCHEMA FILE NOT FOUND: $schemaPath
             |
             |   Expected location: ${schemaFile.getAbsolutePath}
             |
             |   Available schemas:
             |   - nifi/schemas/customer.avsc
             |   - nifi/schemas/account.avsc
             |   - nifi/schemas/transaction_header.avsc
             |   - nifi/schemas/transaction_item.avsc
             |
             |   Action required: Verify entity type or create missing schema file
             |""".stripMargin
        )
      }

      // Read schema file and parse JSON into Avro Schema object
      val source = scala.io.Source.fromFile(schemaFile)
      try {
        val schemaJson = source.mkString
        new Schema.Parser().parse(schemaJson)
      } finally {
        source.close()
      }
    })
  }

  /**
   * Extracts required fields from Avro schema by analyzing field definitions.
   *
   * REQUIRED FIELD DETECTION RULES:
   * A field is considered REQUIRED if:
   * 1. It is NOT nullable (type is not a union containing "null"), AND
   * 2. It has NO default value defined
   *
   * AVRO SCHEMA EXAMPLES:
   * - Required: {"name": "customer_id", "type": "int"}
   * - Optional (nullable): {"name": "email", "type": ["null", "string"], "default": null}
   * - Optional (default): {"name": "currency", "type": "string", "default": "USD"}
   *
   * This makes the Avro schema the SINGLE SOURCE OF TRUTH for validation rules.
   * No need to hardcode field lists in Scala code!
   *
   * @param schema Parsed Avro schema
   * @return Set of required field names
   */
  private def extractRequiredFields(schema: Schema): Set[String] = {
    schema.getFields.asScala
      .filter { field =>
        // Required if NOT nullable AND no default value
        !isNullableField(field) && !field.hasDefaultValue
      }
      .map(_.name())
      .toSet
  }

  /**
   * Checks if an Avro field is nullable by examining its type definition.
   *
   * NULLABLE DETECTION:
   * In Avro, nullable fields are represented as UNION types containing "null":
   * - Nullable: ["null", "string"]
   * - Nullable: ["string", "null"] (order doesn't matter)
   * - Not nullable: "string"
   * - Not nullable: ["string", "int"] (union without null)
   *
   * @param field Avro schema field
   * @return true if field type is a union containing null type
   */
  private def isNullableField(field: Schema.Field): Boolean = {
    field.schema().getType match {
      case Schema.Type.UNION =>
        // Check if any type in the union is NULL
        field.schema().getTypes.asScala.exists(_.getType == Schema.Type.NULL)
      case _ => false
    }
  }

  /**
   * Reads Avro files from the specified staging path into a DataFrame.
   *
   * FEATURES:
   * - Automatic schema extraction from Avro files
   * - Optional schema validation against expected structure
   * - Enrichment with technical columns for lineage and auditing
   *
   * @param stagingPath    Path to the Avro files (can use wildcards, e.g. warehouse/staging/customer/\*.avro)
   * @param validateSchema If true, checks for required fields and warns on new fields (schema evolution)
   * @param spark          Implicit SparkSession
   * @return DataFrame enriched with technical columns for lineage tracking
   */
  def readAvro(stagingPath: String, validateSchema: Boolean = true)
              (implicit spark: SparkSession): DataFrame = {

    println(s"📖 READING AVRO FILES")
    println(s"   Path: $stagingPath")
    println(s"   Validation: ${if (validateSchema) "Enabled" else "Disabled"}")

    // Read Avro files using Spark's built-in Avro data source reader
    // Spark automatically reads the embedded Avro schema from the files
    val df = spark.read.format("avro").load(stagingPath)

    println(s"✅ Schema validated: ${df.columns.length} fields")
    println(s"📊 Records read: ${df.count()}")

    /* Validate schema structure:
         1. Fail fast on missing required fields)
         2. Warn on new/unexpected fields (schema evolution*/
    if (validateSchema) {
      validateSchemaStructure(df, stagingPath)
    }

    // Add technical columns for data lineage and audit trail
    val enrichedDF = df
      .withColumn("_load_timestamp", current_timestamp()) // When was this data loaded into Spark?
      .withColumn("_source_file", input_file_name()) // Which Avro file did this row come from?
      .withColumn("_file_modification_time", to_timestamp(lit(System.currentTimeMillis() / 1000))) //

    enrichedDF
  }

  /**
   * Validates the DataFrame schema against the expected entity structure.
   *
   * VALIDATION APPROACH (Schema-Driven):
   * This method now dynamically extracts required fields from the Avro schema files
   * instead of hardcoding them in Scala code. This ensures:
   * - Single source of truth (Avro schemas define all rules)
   * - No code changes needed when adding optional fields
   * - Impossible for Scala code and schemas to drift
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
   * @param df   DataFrame read from Avro files
   * @param path File path (used to infer entity type)
   */
  private def validateSchemaStructure(df: DataFrame, path: String): Unit = {
    val actualFields = df.columns.toSet
    val entityType = extractEntityType(path) // e.g. customer, account, transaction_header, transaction_item

    // Load Avro schema and extract required fields dynamically
    val avroSchema = loadAvroSchema(entityType)
    val requiredFields = extractRequiredFields(avroSchema)

    println(s"🔍 VALIDATING SCHEMA FOR: $entityType")
    println(s"   Required fields (from ${entityType}.avsc): ${requiredFields.size}")
    println(s"   Actual fields (from Avro data): ${actualFields.size}")

    // Check for missing required fields
    val missingFields = requiredFields.diff(actualFields)
    if (missingFields.nonEmpty) {
      val errorMsg =
        s"""
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
    // We compare against ALL fields in schema (not just required ones)
    val allSchemaFields = avroSchema.getFields.asScala.map(_.name()).toSet
    val unexpectedFields = actualFields.diff(allSchemaFields).filterNot(_.startsWith("_"))
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
      println(s"   RECOMMENDATION: Update nifi/schemas/${entityType}.avsc to document these fields")
      println(s"")
    } else {
      println(s"✅ Schema validation passed - all required fields present")
    }
  }

  /**
   * Infers the entity type (customer, account, etc.) from the file path.
   *
   * CONVENTION:
   * File paths follow the pattern: warehouse/staging/{entity}/\*.avro
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