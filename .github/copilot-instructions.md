# GitHub Copilot Instructions - Banking Data Vault 2.0 POC

This file provides context to GitHub Copilot for generating code consistent with the Data Vault 2.0 architecture, coding patterns, and best practices used in this project.

---

## 🌐 Language & Communication

**IMPORTANT: Always use English for ALL interactions, responses, code comments, documentation, and communication.**

This applies to:
- All responses to user questions
- Code generation and comments
- Documentation and README files
- Error messages and logging
- Commit messages and PR descriptions
- Variable names, method names, and identifiers

**No exceptions.** English is the project's standard language regardless of system locale settings.

---

## 📚 Documentation Standards

**APPROVED DOCUMENTATION FILES (DO NOT CREATE ADDITIONAL ONES):**

The project maintains **ONLY** the following documentation files:

1. **`README.md`** - Project overview, quick start, and links to other docs
2. **`docs/architecture.md`** - Deep dive into Data Vault 2.0 architecture and data models
3. **`docs/setup_guide.md`** - Step-by-step setup, execution, build, and deployment instructions

**DO NOT CREATE:**
- Additional markdown files in root directory
- Separate deployment guides (include in setup_guide.md)
- Separate build guides (include in setup_guide.md)
- Separate configuration guides (include in setup_guide.md)
- Tutorial files (include in appropriate existing docs)
- Design decision documents (include in architecture.md)

**When asked to document something:**
1. First check if it belongs in an existing approved file
2. Update the appropriate existing file instead of creating new ones
3. Only suggest new documentation if absolutely critical and get user approval first

---

## 📋 Project Overview

**Project Type:** End-to-end Data Vault 2.0 implementation for a banking system  
**Domain:** Banking & Financial Transactions  
**Primary Language:** Scala 2.12  
**Architecture Pattern:** Multi-layer data warehouse (Bronze → Silver → Gold)  
**Data Methodology:** Data Vault 2.0 with full audit trail and schema evolution resilience

### Core Purpose
Demonstrate modern data engineering best practices:
- Data Vault 2.0 modeling (Hubs, Links, Satellites)
- Multi-item transaction modeling (e-commerce style)
- Schema evolution handling without breaking queries
- Incremental CDC via Apache NiFi
- SCD Type 2 history tracking
- Query engine performance benchmarking (Spark SQL, Hive Tez, Impala)

---

## 🏗️ Architecture & Data Flow

### Three-Layer Architecture

```
PostgreSQL (Source) → NiFi (CDC + Avro) → Staging
    ↓
Bronze Layer (Raw Vault: Hubs, Links, Satellites) - Immutable, insert-only
    ↓
Silver Layer (Business Vault: PIT tables, Bridge tables) - Optimized for queries
    ↓
Gold Layer (Star Schema: Dimensions, Facts) - BI-ready with SCD Type 2
```

### Layer Characteristics

**Bronze Layer (Raw Vault)**
- **Purpose:** Immutable landing zone preserving all raw data
- **Pattern:** Data Vault 2.0 (Hubs, Links, Satellites)
- **Rule:** Insert-only, never UPDATE or DELETE
- **Key Concept:** Hash keys (MD5) for deterministic joins

**Silver Layer (Business Vault)**
- **Purpose:** Optimized query structures
- **Components:** Point-In-Time (PIT) tables, Bridge tables
- **Pattern:** Temporal queries with window functions
- **Key Concept:** Snapshot tables for performance

**Gold Layer (Dimensional Model)**
- **Purpose:** BI/Analytics consumption layer
- **Pattern:** Star schema (Dimensions + Facts)
- **History:** SCD Type 2 with `valid_from`, `valid_to`, `is_current`
- **Key Concept:** Business-friendly denormalized views

### Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Source DB | PostgreSQL | 12+ | Operational transactional database |
| CDC Tool | Apache NiFi | 2.7.2 | Data extraction with schema validation |
| Data Format | Apache Avro | 1.11.3 | Type-safe serialization with evolution |
| ETL Engine | Apache Spark | 3.5.0 | Distributed data processing |
| Language | Scala | 2.12 | ETL job implementation |
| Storage | Apache Iceberg | 1.4.3 | ACID-compliant table format with time travel |
| Catalog | Hive Metastore | 3.1.3 | Table metadata management |
| Query Engines | Spark SQL, Hive Tez, Impala | Various | Performance benchmarking |

---

## 💻 Code Patterns & Conventions

### File Structure for ETL Jobs

Every Scala ETL file should follow this template:

```scala
/**
 * [Component Name] - [Brief Description]
 *
 * Purpose:
 *   - [Main objective]
 *   - [Secondary objective]
 *
 * Input:
 *   - Source: [data source location]
 *   - Format: [Avro/Iceberg/etc]
 *
 * Output:
 *   - Destination: [warehouse path]
 *   - Tables: [list of tables created/updated]
 *
 * Key Logic:
 *   - [Important processing step 1]
 *   - [Important processing step 2]
 *
 * Dependencies:
 *   - Utilities: [list of utilities used]
 *   - Upstream: [prerequisite jobs]
 */
package bronze // or silver, gold, seeder

import org.apache.spark.sql.{SparkSession, DataFrame}
import common.{ETLJob, ETLConfig}

/**
 * NEW PATTERN (Modular Design):
 * - Extends ETLJob trait for consistent execution interface
 * - Delegates to SparkSessionFactory for session creation
 * - Uses ConfigLoader for cascading configuration
 * - Supports IDE, sbt, spark-submit, and Airflow execution
 */
object ComponentName extends ETLJob {
  
  // Thin main() delegates to runMain() from ETLJob trait
  def main(args: Array[String]): Unit = {
    runMain(args, "Component Name - Brief Description")
  }
  
  // Pure business logic - no infrastructure concerns
  override def execute(spark: SparkSession, config: ETLConfig): Unit = {
    implicit val implicitSpark = spark
    
    println("📖 Starting [Component Name]...")
    
    // Processing logic here
    
    println("✅ [Component Name] completed successfully")
  }
  
  // Optional: validate prerequisites
  override def validatePrerequisites(spark: SparkSession, config: ETLConfig): Unit = {
    // Check input data exists, etc.
  }
}

/**
 * LEGACY PATTERN (For reference only - DO NOT use for new code):
 * - Embedded SparkSession creation
 * - Inline configuration
 * - Not easily testable or reusable
 */
/*
object LegacyComponentName {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Component Name")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hive")
      .enableHiveSupport()
      .getOrCreate()
    
    try {
      // Processing logic here
    } finally {
      spark.stop()
    }
  }
}
*/
```

### Utility Classes Pattern

Utility classes should be stateless and reusable:

```scala
package bronze.utils // or silver.utils, gold.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * [Utility Name] - [Brief Description]
 *
 * Usage Example:
 *   {{{
 *   val hashedDF = UtilityName.generateHashKey(df, Seq("customer_id"), "customer_hash_key")
 *   }}}
 */
object UtilityName {
  
  /**
   * [Method description]
   *
   * @param df Input DataFrame
   * @param columns Columns to process
   * @return Transformed DataFrame
   */
  def methodName(df: DataFrame, columns: Seq[String])(implicit spark: SparkSession): DataFrame = {
    // Implementation
  }
}
```

### Key Utilities in This Project

Reference these utilities when generating code:

- **`bronze.utils.HashKeyGenerator`** - MD5 hash key generation for Data Vault
  - `generateHashKey(df, businessKeys, outputColumn)` - Single entity hash
  - `generateLinkHashKey(df, hashKeyColumns, outputColumn)` - Composite link hash

- **`bronze.utils.AvroReader`** - Read and validate Avro files from NiFi
  - `readAvro(path, schemaFile)` - Read with schema validation
  
- **`silver.utils.PITBuilder`** - Build Point-In-Time tables
  - `buildPIT(satelliteDF, effectiveDate)` - Temporal snapshot construction
  
- **`gold.utils.SCDType2Handler`** - Slowly Changing Dimension Type 2
  - `applySCD2(currentDF, newDF, businessKey)` - Manage historical records

### Logging Conventions

Use structured, emoji-enhanced logging for clarity:

```scala
println("📖 Reading data from staging...")        // Starting an operation
println("✅ Successfully loaded 1,234 records")   // Success message
println("⚠️  Warning: No new records found")      // Warning
println("❌ Error: Schema mismatch detected")     // Error
println("🔍 Validating schema...")                // Validation step
println("💾 Writing to Iceberg table...")         // Persistence operation
println("🏗️  Building PIT table...")              // Construction operation
```

---

## 🔧 Modular ETL Architecture

### Cross-Cutting Concerns Separation

The project uses a **modular design pattern** that separates infrastructure concerns from business logic, enabling:
- ✅ Multiple execution modes (IDE, sbt, spark-submit, Airflow)
- ✅ Flexible configuration (JVM props, env vars, .properties files)
- ✅ No code duplication across ETL jobs
- ✅ Testable business logic
- ✅ Easy catalog switching (Hive vs Hadoop)

### Core Components

**1. ConfigLoader (common.ConfigLoader)**

Centralized configuration with cascading precedence:

```scala
// Precedence: JVM Props → Env Vars → .properties → Default
val warehouse = ConfigLoader.getString(
  "spark.sql.catalog.spark_catalog.warehouse",  // Property key
  "SPARK_WAREHOUSE",                             // Env var key
  "warehouse"                                    // Default
)

// Optional values
val hmsUri = ConfigLoader.getOptionalString(
  "spark.sql.catalog.spark_catalog.uri",
  "HIVE_METASTORE_URI"
)

// Type-safe getters
val maxRetries = ConfigLoader.getInt("etl.max.retries", "ETL_MAX_RETRIES", 3)
val enabled = ConfigLoader.getBoolean("etl.validation.enabled", "ETL_VALIDATION_ENABLED", true)
```

**2. SparkSessionFactory (common.SparkSessionFactory)**

Centralized SparkSession creation:

```scala
// Creates session with Iceberg, catalog configuration, networking
val spark = SparkSessionFactory.createSession("My ETL Job")

// Automatically configures:
// - Iceberg extensions
// - Catalog type (Hive or Hadoop based on HMS URI presence)
// - Driver networking (host, bind address, port)
// - Warehouse location
// - IPv4 preference
```

**3. ETLConfig (common.ETLConfig)**

Type-safe job configuration:

```scala
// From command-line args
val config = ETLConfig.fromArgs(args)
// Parses: --mode, --entity, --date, --build-pit, --rebuild-all, etc.

// From map (programmatic)
val config = ETLConfig.fromMap(Map(
  "mode" -> "incremental",
  "entity" -> "customer"
))

// Access
config.mode           // "full" or "incremental"
config.entity         // Option[String]
config.snapshotDate   // LocalDate
config.buildPIT       // Boolean (for Silver layer)
config.rebuildDims    // Boolean (for Gold layer)
```

**4. ETLJob Trait (common.ETLJob)**

Standard interface for all ETL jobs:

```scala
trait ETLJob {
  // Implement this with your business logic
  def execute(spark: SparkSession, config: ETLConfig): Unit
  
  // Optional: pre-flight checks
  def validatePrerequisites(spark: SparkSession, config: ETLConfig): Unit = {}
  
  // Provided: standard main entry point
  def runMain(args: Array[String], appName: String): Unit
}
```

**5. ETLRunner (common.ETLRunner)**

Programmatic execution API for Airflow/orchestration:

```scala
// Run job with config map
ETLRunner.runJob(
  bronze.RawVaultETL,
  Map("mode" -> "incremental", "entity" -> "customer")
)

// Run job with args
ETLRunner.runJobWithArgs(
  bronze.RawVaultETL,
  Array("--mode", "full")
)
```

### Configuration File Format

**Use `.properties` format (NOT `.conf`):**

```properties
# src/main/resources/application.properties

# Catalog & Warehouse
spark.sql.catalog.spark_catalog.warehouse=warehouse
# spark.sql.catalog.spark_catalog.uri=thrift://localhost:9083

# Networking
spark.driver.host=127.0.0.1
spark.driver.bindAddress=0.0.0.0
spark.master=local[*]

# Data Paths
staging.path=warehouse/staging
bronze.path=warehouse/bronze
silver.path=warehouse/silver
gold.path=warehouse/gold

# ETL Defaults
etl.mode=incremental
etl.record.source=PostgreSQL
```

**Why .properties?**
- ✅ Simpler than HOCON (.conf) - flat key-value pairs
- ✅ No extra dependency - Java's built-in `Properties` class
- ✅ Direct 1:1 mapping to JVM props and env vars
- ✅ Familiar to Spark users (same as spark-defaults.conf)

### Execution Modes

| Mode | Entry Point | Config Override Method | Use Case |
|------|-------------|------------------------|----------|
| **IDE (IntelliJ)** | `main()` → `runMain()` | Run Config env vars or VM options | Development, debugging |
| **sbt runMain** | `main()` → `runMain()` | `-Dkey=value` or export | Local testing |
| **spark-submit** | `main()` → `runMain()` | `--conf key=value` | Production, cluster |
| **Airflow** | `ETLRunner.runJob()` | DAG conf map | Orchestration |

**Example Commands:**

```powershell
# IDE - Set in Run Configuration:
# Main class: bronze.RawVaultETL
# Program args: --mode full --entity customer
# Env vars: SPARK_WAREHOUSE=warehouse

# sbt runMain
sbt "runMain bronze.RawVaultETL --mode full"
sbt -Dspark.sql.catalog.spark_catalog.warehouse=custom "runMain bronze.RawVaultETL --mode full"

# spark-submit
spark-submit \
  --class bronze.RawVaultETL \
  --conf spark.sql.catalog.spark_catalog.warehouse=warehouse \
  data-vault-etl.jar \
  --mode incremental

# Airflow (Python DAG)
SparkSubmitOperator(
    application='data-vault-etl.jar',
    java_class='bronze.RawVaultETL',
    application_args=['--mode', 'incremental'],
    conf={'spark.sql.catalog.spark_catalog.warehouse': 'hdfs://namenode/warehouse'}
)
```

### When Implementing New ETL Jobs

**DO:**
- ✅ Extend `common.ETLJob` trait
- ✅ Implement `execute(spark, config)` with pure business logic
- ✅ Use `ConfigLoader` for any custom configuration needs
- ✅ Delegate `main()` to `runMain()`
- ✅ Use `implicit val implicitSpark = spark` to pass SparkSession to helper methods
- ✅ Test with different execution modes (IDE, sbt, spark-submit)

**DON'T:**
- ❌ Create SparkSession manually in `main()`
- ❌ Hardcode configuration values
- ❌ Mix infrastructure code with business logic
- ❌ Use `.mode("overwrite")` in Bronze layer
- ❌ Duplicate catalog/networking configuration

**Example New ETL Job:**

```scala
package bronze

import org.apache.spark.sql.SparkSession
import common.{ETLJob, ETLConfig}
import bronze.utils.{AvroReader, HashKeyGenerator, IcebergWriter}

object MyNewETL extends ETLJob {
  
  def main(args: Array[String]): Unit = {
    runMain(args, "My New ETL Job")
  }
  
  override def execute(spark: SparkSession, config: ETLConfig): Unit = {
    implicit val implicitSpark = spark
    
    // Read staging data
    val stagingPath = ConfigLoader.getString("staging.path", "STAGING_PATH", "warehouse/staging")
    val df = AvroReader.readAvro(s"$stagingPath/my_entity/*.avro")
    
    // Generate hash keys
    val withHashKey = HashKeyGenerator.generateHashKey("entity_hash_key", Seq("entity_id"), df)
    
    // Write to bronze
    IcebergWriter.appendToTable(withHashKey, "bronze", "hub_entity", Seq("load_date"))
  }
}
```

---

## 🗄️ Data Vault 2.0 Conventions

### Naming Conventions

**Tables:**
- Hubs: `hub_<entity>` (e.g., `hub_customer`, `hub_account`, `hub_transaction`)
- Links: `link_<entity1>_<entity2>` (e.g., `link_customer_account`, `link_transaction_item`)
- Satellites: `sat_<entity>` (e.g., `sat_customer`, `sat_account`, `sat_transaction_item`)
- PIT Tables: `pit_<entity>` (e.g., `pit_customer`, `pit_account`)
- Bridge Tables: `bridge_<entity>` (e.g., `bridge_transaction`)
- Dimensions: `dim_<entity>` (e.g., `dim_customer`, `dim_account`)
- Facts: `fact_<entity>` (e.g., `fact_transaction`, `fact_transaction_item`)

**Columns:**
- Hash Keys: `<entity>_hash_key` (e.g., `customer_hash_key`, `account_hash_key`)
- Business Keys: Original source column names (e.g., `customer_id`, `account_number`)
- Load Metadata: `load_timestamp`, `record_source`
- Temporal: `valid_from`, `valid_to`, `is_current` (for SCD Type 2)

### Hash Key Generation

Always use MD5 hashing for deterministic, reproducible keys:

```scala
// Single entity hash key (for Hubs and Satellites)
import org.apache.spark.sql.functions._

df.withColumn("customer_hash_key", 
  md5(concat_ws("||", col("customer_id"))))

// Composite hash key (for Links)
df.withColumn("link_hash_key",
  md5(concat_ws("||", col("customer_hash_key"), col("account_hash_key"))))
```

**Important:** Use `||` as the delimiter for concatenation to ensure consistency.

### Mandatory Technical Columns

**All Hubs:**
```scala
<entity>_hash_key: STRING      // MD5 hash of business key(s)
<business_key>: <TYPE>         // Original business identifier
load_timestamp: TIMESTAMP      // When record was loaded
record_source: STRING          // Source system identifier
```

**All Satellites:**
```scala
<entity>_hash_key: STRING      // Foreign key to Hub
load_timestamp: TIMESTAMP      // Effective date of this version
record_source: STRING          // Source system identifier
<attribute_columns>: <TYPES>   // Descriptive attributes
```

**All Links:**
```scala
<link>_hash_key: STRING        // MD5 hash of all participating hash keys
<entity1>_hash_key: STRING     // First entity reference
<entity2>_hash_key: STRING     // Second entity reference
load_timestamp: TIMESTAMP      // When relationship was established
record_source: STRING          // Source system identifier
```

### Insert-Only Pattern

**CRITICAL RULE:** Bronze layer tables are immutable. Never generate UPDATE or DELETE statements.

```scala
// ✅ CORRECT - Insert new records only
newRecords
  .write
  .format("iceberg")
  .mode("append")  // Always append
  .saveAsTable("local.bronze.sat_customer")

// ❌ WRONG - Never overwrite or update
newRecords
  .write
  .mode("overwrite")  // NEVER use this in Bronze
  .saveAsTable("local.bronze.sat_customer")
```

---

## 📦 Avro Schema Management

### Schema Location
All Avro schemas are stored in: `nifi/schemas/*.avsc`

Current schemas:
- `customer.avsc` - Customer master data
- `account.avsc` - Account master data
- `transaction_header.avsc` - Transaction headers
- `transaction_item.avsc` - Transaction line items

### Schema Evolution Rules

**When adding new fields to Avro schemas:**

1. **Make fields optional** with default values:
```json
{
  "name": "loyalty_tier",
  "type": ["null", "string"],
  "default": null
}
```

2. **Spark will automatically handle** the new fields
3. **Old queries continue working** - new fields appear as NULL for historical data
4. **Update NiFi flow** to extract the new column
5. **No Bronze table schema changes needed** - Iceberg schema evolution handles it

### Reading Avro in Spark

```scala
val df = spark.read
  .format("avro")
  .load("warehouse/staging/customer/*.avro")
```

---

## 🔧 Common Development Tasks

### 1. Create a New Hub Table

```scala
// Define schema
val hubSchema = StructType(Seq(
  StructField("customer_hash_key", StringType, nullable = false),
  StructField("customer_id", IntegerType, nullable = false),
  StructField("load_timestamp", TimestampType, nullable = false),
  StructField("record_source", StringType, nullable = false)
))

// Create Iceberg table
spark.sql("""
  CREATE TABLE IF NOT EXISTS local.bronze.hub_customer (
    customer_hash_key STRING,
    customer_id INT,
    load_timestamp TIMESTAMP,
    record_source STRING
  ) USING iceberg
  PARTITIONED BY (days(load_timestamp))
""")

// Load data with hash key
val hubData = sourceDF
  .withColumn("customer_hash_key", md5(concat_ws("||", col("customer_id"))))
  .withColumn("load_timestamp", current_timestamp())
  .withColumn("record_source", lit("banking_source"))
  .select("customer_hash_key", "customer_id", "load_timestamp", "record_source")

hubData.write.format("iceberg").mode("append").saveAsTable("local.bronze.hub_customer")
```

### 2. Create a New Satellite Table

```scala
// Create table with attributes
spark.sql("""
  CREATE TABLE IF NOT EXISTS local.bronze.sat_customer (
    customer_hash_key STRING,
    load_timestamp TIMESTAMP,
    record_source STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING
  ) USING iceberg
  PARTITIONED BY (days(load_timestamp))
""")

// Load with deduplication (keep latest per hash key)
val satData = sourceDF
  .withColumn("customer_hash_key", md5(concat_ws("||", col("customer_id"))))
  .withColumn("load_timestamp", current_timestamp())
  .withColumn("record_source", lit("banking_source"))
  .select("customer_hash_key", "load_timestamp", "record_source", 
          "first_name", "last_name", "email", "phone")

satData.write.format("iceberg").mode("append").saveAsTable("local.bronze.sat_customer")
```

### 3. Implement Incremental ETL

```scala
// Parse command-line arguments
val mode = if (args.contains("--mode") && args.indexOf("--mode") + 1 < args.length) {
  args(args.indexOf("--mode") + 1)
} else {
  "incremental"
}

val sourceDF = mode match {
  case "full" =>
    println("📖 Running in FULL mode - loading all records")
    spark.read.format("avro").load("warehouse/staging/customer/*.avro")
    
  case "incremental" =>
    println("📖 Running in INCREMENTAL mode - loading new records only")
    val maxTimestamp = spark.sql("""
      SELECT COALESCE(MAX(load_timestamp), '1900-01-01') as max_ts 
      FROM local.bronze.sat_customer
    """).first().getTimestamp(0)
    
    spark.read.format("avro")
      .load("warehouse/staging/customer/*.avro")
      .filter(col("extracted_at") > lit(maxTimestamp))
}
```

### 4. Build a Point-In-Time (PIT) Table

```scala
// Join Hub with latest Satellite for each entity
val pitDF = spark.sql("""
  SELECT 
    h.customer_hash_key,
    h.customer_id,
    s.first_name,
    s.last_name,
    s.email,
    s.phone,
    s.load_timestamp as effective_timestamp
  FROM local.bronze.hub_customer h
  INNER JOIN (
    SELECT 
      customer_hash_key,
      first_name,
      last_name,
      email,
      phone,
      load_timestamp,
      ROW_NUMBER() OVER (PARTITION BY customer_hash_key ORDER BY load_timestamp DESC) as rn
    FROM local.bronze.sat_customer
  ) s ON h.customer_hash_key = s.customer_hash_key
  WHERE s.rn = 1
""")

pitDF.write.format("iceberg").mode("overwrite").saveAsTable("local.silver.pit_customer")
```

### 5. Implement SCD Type 2 in Gold Layer

```scala
// Add temporal columns to dimension
val dimDF = sourceDF
  .withColumn("valid_from", current_timestamp())
  .withColumn("valid_to", lit(null).cast(TimestampType))
  .withColumn("is_current", lit(true))

// For updates: close old record, insert new record
val updatesDF = newRecordsDF
  .join(existingDF, Seq("customer_id"))
  .filter("existing.is_current = true AND new.has_changes = true")

// Close old records
updatesDF.select("customer_id")
  .write
  .format("iceberg")
  .mode("append")
  .option("merge-schema", "true")
  .saveAsTable("local.gold.dim_customer")
  
// SQL alternative for merging
spark.sql("""
  MERGE INTO local.gold.dim_customer t
  USING new_customer_data s
  ON t.customer_id = s.customer_id AND t.is_current = true
  WHEN MATCHED THEN UPDATE SET 
    t.valid_to = current_timestamp(),
    t.is_current = false
  WHEN NOT MATCHED THEN INSERT *
""")
```

---

## 🎯 Project-Specific Best Practices

### 1. Documentation Standards
- **Every ETL job** must have comprehensive header comments (see template above)
- **Every utility method** must have ScalaDoc with usage examples
- **Inline comments** for complex business logic or non-obvious transformations

### 2. Error Handling
Always wrap main logic in try-catch blocks:
```scala
try {
  println("📖 Starting process...")
  // Main logic
  println("✅ Process completed successfully")
} catch {
  case e: Exception =>
    println(s"❌ Error: ${e.getMessage}")
    e.printStackTrace()
    throw e  // Re-throw to fail the job
} finally {
  spark.stop()
}
```

### 3. Schema Validation
Validate Avro files match expected schemas in NiFi:
- Use `ValidateRecord` processor with Avro schema registry
- Reject invalid records to error flow
- Log validation failures for investigation

### 4. Performance Optimization
- **Partitioning:** Use `PARTITIONED BY (days(load_timestamp))` for time-series data
- **Broadcast Joins:** For small dimension tables joining to large facts
- **Caching:** Use `.cache()` for DataFrames referenced multiple times
- **Coalesce:** Reduce partition count before writing small datasets

```scala
// Good practice for small dimension
val dimCustomer = spark.table("local.gold.dim_customer").cache()

// Reduce partitions before writing
factDF.coalesce(10).write.format("iceberg").mode("append").saveAsTable("local.gold.fact_transaction")
```

### 5. Iceberg Configuration
Always include these Spark configs for Iceberg tables:
```scala
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.local.type", "hive")
.config("spark.sql.catalog.local.warehouse", "warehouse")
```

### 6. Testing & Validation
After loading data, validate record counts:
```scala
val recordCount = spark.table("local.bronze.hub_customer").count()
println(s"✅ Loaded $recordCount records to hub_customer")

// Validate no duplicates in Hub
val duplicates = spark.sql("""
  SELECT customer_hash_key, COUNT(*) as cnt
  FROM local.bronze.hub_customer
  GROUP BY customer_hash_key
  HAVING cnt > 1
""").count()

if (duplicates > 0) {
  println(s"⚠️  Warning: Found $duplicates duplicate hash keys!")
}
```

---

## 📂 Directory Structure Reference

```
src/main/scala/
├── bronze/                      # Raw Vault ETL jobs
│   ├── RawVaultSchema.scala    # Create Bronze tables
│   ├── RawVaultETL.scala       # Main ETL orchestrator
│   └── utils/                  # Bronze utilities
│       ├── HashKeyGenerator.scala
│       └── AvroReader.scala
├── silver/                      # Business Vault ETL jobs
│   ├── BusinessVaultETL.scala  # Build PIT/Bridge tables
│   └── utils/                  # Silver utilities
│       └── PITBuilder.scala
├── gold/                        # Dimensional Model ETL jobs
│   ├── DimensionalModelETL.scala
│   └── utils/                  # Gold utilities
│       └── SCDType2Handler.scala
└── seeder/                      # Data generation
    └── TransactionalDataSeeder.scala

nifi/
├── schemas/                     # Avro schema definitions
│   ├── customer.avsc
│   ├── account.avsc
│   ├── transaction_header.avsc
│   └── transaction_item.avsc
└── scripts/                     # Validation scripts
    └── validate-nifi-schemas.ps1

warehouse/
├── staging/                     # Avro files from NiFi
│   ├── customer/
│   ├── account/
│   ├── transaction_header/
│   └── transaction_item/
├── bronze/                      # Iceberg Raw Vault tables
├── silver/                      # Iceberg Business Vault tables
└── gold/                        # Iceberg Dimensional tables

scripts/windows/                 # PowerShell orchestration
├── 01-setup.ps1                # Environment setup
├── 02-run-etl.ps1              # ETL execution
└── 05-cleanup.ps1              # Cleanup
```

---

## 🚀 Execution Patterns

### Running ETL Jobs

```powershell
# Run with SBT
sbt "runMain bronze.RawVaultETL --mode full"
sbt "runMain silver.BusinessVaultETL --build-pit"
sbt "runMain gold.DimensionalModelETL --load-dimensions"

# Package and run with spark-submit (alternative)
sbt package
spark-submit --class bronze.RawVaultETL target/scala-2.12/*.jar --mode incremental
```

### Command-Line Arguments Pattern

```scala
// Parse arguments in main()
val mode = if (args.contains("--mode") && args.indexOf("--mode") + 1 < args.length) {
  args(args.indexOf("--mode") + 1)
} else {
  "incremental"  // default
}

val buildPIT = args.contains("--build-pit")
val fullRefresh = args.contains("--full-refresh")
```

---

## 📊 Query Examples for Reference

### Customer 360 View
```sql
SELECT 
  c.customer_id,
  c.first_name,
  c.last_name,
  COUNT(DISTINCT a.account_number) as num_accounts,
  SUM(a.balance) as total_balance
FROM local.gold.dim_customer c
LEFT JOIN local.gold.dim_account a ON c.customer_id = a.customer_id
WHERE c.is_current = true AND a.is_current = true
GROUP BY c.customer_id, c.first_name, c.last_name
```

### Multi-Item Transaction Analysis
```sql
SELECT 
  th.transaction_id,
  th.transaction_date,
  th.total_amount as header_amount,
  COUNT(ti.item_id) as num_items,
  SUM(ti.amount) as calculated_total
FROM local.gold.fact_transaction th
INNER JOIN local.gold.fact_transaction_item ti ON th.transaction_id = ti.transaction_id
GROUP BY th.transaction_id, th.transaction_date, th.total_amount
HAVING ABS(header_amount - calculated_total) > 0.01  -- Find discrepancies
```

### Schema Evolution Query (with new loyalty_tier column)
```sql
-- Works for both old and new records
SELECT 
  customer_id,
  first_name,
  last_name,
  COALESCE(loyalty_tier, 'STANDARD') as loyalty_tier  -- Default for NULL
FROM local.gold.dim_customer
WHERE is_current = true
```

---

## 🔗 Additional Resources

**Documentation:**
- [Architecture & Design Guide](docs/architecture.md) - Deep dive into data models
- [Setup & Execution Guide](docs/setup_guide.md) - Step-by-step implementation
- [Query Benchmarks](sample_queries/benchmarks/) - Performance testing queries

**External References:**
- Data Vault 2.0: https://datavaultalliance.com/
- Apache Iceberg: https://iceberg.apache.org/
- Apache Avro: https://avro.apache.org/
- Apache Spark: https://spark.apache.org/docs/latest/

---

## ⚡ Performance & Benchmarking

This project includes a comprehensive benchmarking framework comparing three query engines:

### Query Engines
1. **Spark SQL** - Baseline distributed processing engine
2. **Hive on Tez** - DAG-based batch processing
3. **Impala** - MPP for low-latency analytics

### Benchmark Suite Location
`sample_queries/benchmarks/` contains 5 standardized queries:
1. Simple aggregations
2. Complex joins
3. Temporal queries
4. Multi-item analysis
5. Schema evolution scenarios

### Running Benchmarks
```sql
-- Execute in Spark SQL, Hive Beeline, or Impala Shell
-- Measure: Execution time, resource usage, concurrency
source sample_queries/benchmarks/01_simple_aggregation.sql
```

---

## 🎯 When to Use Each Layer

| Scenario | Layer | Reason |
|----------|-------|--------|
| Audit trail / compliance queries | Bronze | Complete immutable history |
| Historical point-in-time analysis | Silver | Pre-built PIT tables for performance |
| BI dashboards & reports | Gold | Denormalized star schema, SCD Type 2 |
| Exploratory data analysis | Silver or Gold | Balance between detail and performance |
| Machine learning feature engineering | Silver | Rich temporal features, less aggregation |
| Executive reporting | Gold | Business-friendly, aggregated metrics |

---

## ✅ Code Generation Checklist

When GitHub Copilot generates new code for this project, ensure:

- [ ] **Header comments** follow the established template
- [ ] **Hash keys** use MD5 with `||` delimiter
- [ ] **Bronze tables** only use `.mode("append")`, never overwrite
- [ ] **Temporal columns** (`load_timestamp`, `valid_from`, `valid_to`) are included
- [ ] **Error handling** includes try-catch with emoji logging
- [ ] **SparkSession** includes Iceberg extensions and Hive support
- [ ] **Avro schemas** are read from `nifi/schemas/*.avsc`
- [ ] **Table names** follow conventions (`hub_*`, `sat_*`, `link_*`, `dim_*`, `fact_*`)
- [ ] **Partitioning** uses `days(load_timestamp)` for time-series data
- [ ] **Record counts** are validated and logged after operations

---

*This file should be updated when architecture patterns, conventions, or technologies change in the project.*

