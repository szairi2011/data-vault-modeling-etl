package silver

/**
 * ========================================================================
 * BUSINESS VAULT SCHEMA DEFINITIONS (SILVER LAYER)
 * ========================================================================
 *
 * PURPOSE:
 * Define Point-in-Time (PIT) tables and Bridge tables for optimized querying.
 *
 * LEARNING OBJECTIVES:
 * - Why Business Vault layer exists (performance optimization)
 * - Point-in-Time (PIT) table pattern
 * - Bridge table pattern for many-to-many
 * - Pre-joining strategy benefits
 * - Snapshot date generation
 *
 * BUSINESS VAULT PURPOSE:
 *
 * The Business Vault sits between Raw Vault (Bronze) and Dimensional Model (Gold).
 * It serves two main purposes:
 *
 * 1. PERFORMANCE OPTIMIZATION:
 *    - Pre-join frequently used tables
 *    - Materialize complex temporal logic
 *    - Reduce query complexity
 *
 * 2. BUSINESS LOGIC:
 *    - Apply business rules
 *    - Create derived attributes
 *    - Standardize calculations
 *
 * ARCHITECTURE:
 * ```
 * RAW VAULT (Bronze)           BUSINESS VAULT (Silver)
 * ┌──────────────┐             ┌────────────────────┐
 * │ Hub_Customer │─────────────→│ PIT_Customer       │
 * │ Sat_Customer │             │ (Point-in-Time)    │
 * └──────────────┘             │ - Denormalized     │
 *                              │ - Daily snapshots  │
 *                              │ - Fast queries     │
 *                              └────────────────────┘
 *
 * │ Hub_Customer │             ┌────────────────────┐
 * │ Link_Cust_Acc│─────────────→│ Bridge_Customer_   │
 * │ Hub_Account  │             │ Account            │
 *                              │ (Pre-joined M:M)   │
 *                              └────────────────────┘
 * ```
 *
 * POINT-IN-TIME (PIT) TABLES:
 * - Snapshot of data as it was on specific date
 * - One row per hub + snapshot date
 * - Contains latest satellite values as of that date
 * - Eliminates complex temporal joins
 *
 * BRIDGE TABLES:
 * - Pre-joined many-to-many relationships
 * - Includes both parent hubs
 * - Optional: Include most recent satellite data
 * - Speeds up multi-hop queries
 *
 * ========================================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import bronze.utils.IcebergWriter

object BusinessVaultSchema {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PIT_CUSTOMER - Point-in-Time Customer Snapshot                 │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Daily snapshot of customer data, eliminating temporal join complexity.
   *
   * STRUCTURE:
   * - One row per customer per snapshot_date
   * - Contains latest satellite values as of snapshot_date
   * - Massive query performance improvement
   *
   * QUERY COMPARISON:
   *
   * WITHOUT PIT (Complex):
   * ```sql
   * SELECT c.*, s.*
   * FROM hub_customer c
   * JOIN sat_customer s ON c.customer_hash_key = s.customer_hash_key
   * WHERE s.valid_from <= DATE('2025-01-15')
   *   AND (s.valid_to > DATE('2025-01-15') OR s.valid_to IS NULL)
   * ```
   *
   * WITH PIT (Simple):
   * ```sql
   * SELECT * FROM pit_customer
   * WHERE snapshot_date = DATE('2025-01-15')
   * ```
   *
   * PARTITIONING:
   * - Partition by snapshot_date (daily)
   * - Enables efficient date range queries
   * - Old snapshots can be archived/expired
   *
   * REFRESH STRATEGY:
   * - Daily batch: Generate today's snapshot
   * - Incremental: Only new snapshot_date
   * - Historical backfill: Generate past snapshots if needed
   */
  val PIT_CUSTOMER_SCHEMA = StructType(Seq(
    // Hub reference
    StructField("customer_hash_key", StringType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "Reference to Hub_Customer")
        .build()),
    StructField("customer_id", IntegerType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "Business key for easy joining")
        .build()),

    // Snapshot metadata
    StructField("snapshot_date", DateType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "Date of this snapshot (partition key)")
        .build()),

    // Satellite attributes (latest as of snapshot_date)
    StructField("customer_type", StringType, nullable = true),
    StructField("first_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true),
    StructField("company_name", StringType, nullable = true),
    StructField("email", StringType, nullable = true),
    StructField("phone", StringType, nullable = true),
    StructField("date_of_birth", DateType, nullable = true),
    StructField("tax_id", StringType, nullable = true),
    StructField("customer_since", DateType, nullable = true),
    StructField("customer_status", StringType, nullable = true),
    StructField("credit_score", IntegerType, nullable = true),
    StructField("loyalty_tier", StringType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Added via schema evolution")
        .build()),

    // Temporal tracking (from satellite)
    StructField("valid_from", TimestampType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "When this version became active in satellite")
        .build()),
    StructField("valid_to", TimestampType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "When this version was superseded (NULL = current)")
        .build()),

    // Audit
    StructField("load_date", DateType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "When this PIT record was created")
        .build()),
    StructField("record_source", StringType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PIT_ACCOUNT - Point-in-Time Account Snapshot                   │
   * └─────────────────────────────────────────────────────────────────┘
   */
  val PIT_ACCOUNT_SCHEMA = StructType(Seq(
    StructField("account_hash_key", StringType, nullable = false),
    StructField("account_id", IntegerType, nullable = false),
    StructField("snapshot_date", DateType, nullable = false),

    // Satellite attributes
    StructField("account_number", StringType, nullable = true),
    StructField("account_type", StringType, nullable = true),
    StructField("product_id", IntegerType, nullable = true),
    StructField("branch_id", IntegerType, nullable = true),
    StructField("balance", DecimalType(15, 2), nullable = true),
    StructField("available_balance", DecimalType(15, 2), nullable = true),
    StructField("currency_code", StringType, nullable = true),
    StructField("interest_rate", DecimalType(5, 4), nullable = true),
    StructField("credit_limit", DecimalType(15, 2), nullable = true),
    StructField("opened_date", DateType, nullable = true),
    StructField("account_status", StringType, nullable = true),

    // Temporal
    StructField("valid_from", TimestampType, nullable = false),
    StructField("valid_to", TimestampType, nullable = true),

    // Audit
    StructField("load_date", DateType, nullable = false),
    StructField("record_source", StringType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BRIDGE_CUSTOMER_ACCOUNT - Pre-joined Customer-Account          │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Pre-join customer and account for fast many-to-many queries.
   *
   * WITHOUT BRIDGE:
   * ```sql
   * -- Complex 3-way join
   * SELECT c.*, a.*
   * FROM hub_customer c
   * JOIN link_customer_account lca ON c.customer_hash_key = lca.customer_hash_key
   * JOIN hub_account a ON lca.account_hash_key = a.account_hash_key
   * ```
   *
   * WITH BRIDGE:
   * ```sql
   * -- Simple table scan
   * SELECT * FROM bridge_customer_account
   * WHERE customer_id = 123
   * ```
   *
   * DENORMALIZATION STRATEGY:
   * - Include both customer and account business keys
   * - Optional: Include most recent attributes
   * - Trade storage for query performance
   *
   * WHEN TO USE BRIDGES:
   * ✅ Frequently queried relationships
   * ✅ Many-to-many with complex joins
   * ✅ BI tool queries (need simplicity)
   * ❌ Rarely queried relationships
   * ❌ Highly volatile data
   */
  val BRIDGE_CUSTOMER_ACCOUNT_SCHEMA = StructType(Seq(
    // Link reference
    StructField("link_customer_account_hash_key", StringType, nullable = false),

    // Customer hub
    StructField("customer_hash_key", StringType, nullable = false),
    StructField("customer_id", IntegerType, nullable = false),

    // Account hub
    StructField("account_hash_key", StringType, nullable = false),
    StructField("account_id", IntegerType, nullable = false),

    // Optional: Current customer attributes (denormalized)
    StructField("customer_name", StringType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Derived: first_name + last_name OR company_name")
        .build()),
    StructField("customer_status", StringType, nullable = true),
    StructField("customer_type", StringType, nullable = true),

    // Optional: Current account attributes (denormalized)
    StructField("account_number", StringType, nullable = true),
    StructField("account_type", StringType, nullable = true),
    StructField("account_status", StringType, nullable = true),
    StructField("current_balance", DecimalType(15, 2), nullable = true),

    // Relationship metadata
    StructField("relationship_start_date", DateType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "When customer-account relationship was created")
        .build()),
    StructField("is_primary_account", BooleanType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "Business rule: First account = primary")
        .build()),

    // Audit
    StructField("load_date", DateType, nullable = false),
    StructField("record_source", StringType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PIT_TRANSACTION - Point-in-Time Transaction Snapshot           │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * NOTE: Transactions are typically immutable, so PIT may seem redundant.
   * However, useful for:
   * - Status changes (PENDING → POSTED → REVERSED)
   * - Consistent snapshot with other entities
   * - Simplified queries with other PITs
   */
  val PIT_TRANSACTION_SCHEMA = StructType(Seq(
    StructField("transaction_hash_key", StringType, nullable = false),
    StructField("transaction_id", IntegerType, nullable = false),
    StructField("snapshot_date", DateType, nullable = false),

    // Satellite attributes
    StructField("transaction_number", StringType, nullable = true),
    StructField("transaction_type", StringType, nullable = true),
    StructField("transaction_date", DateType, nullable = true),
    StructField("posting_date", DateType, nullable = true),
    StructField("total_amount", DecimalType(15, 2), nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("channel", StringType, nullable = true),
    StructField("status", StringType, nullable = true),

    // Temporal
    StructField("valid_from", TimestampType, nullable = false),
    StructField("valid_to", TimestampType, nullable = true),

    // Audit
    StructField("load_date", DateType, nullable = false),
    StructField("record_source", StringType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ CREATE ALL BUSINESS VAULT TABLES                               │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createAllTables()(implicit spark: SparkSession): Unit = {

    println("""
         |┌────────────────────────────────────────────────────────────────┐
         |│ CREATING BUSINESS VAULT TABLES (SILVER LAYER)                │
         |├────────────────────────────────────────────────────────────────┤
         |│ Database: silver                                               │
         |│ Format: Apache Iceberg v2                                      │
         |│ Purpose: Performance optimization & business logic             │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    // Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    // Create PIT tables
    println("\n📸 Creating Point-in-Time (PIT) tables...")
    IcebergWriter.createTableIfNotExists(
      "silver", "pit_customer", PIT_CUSTOMER_SCHEMA,
      Seq("snapshot_date"),
      Map("comment" -> "Daily customer snapshots for efficient temporal queries")
    )

    IcebergWriter.createTableIfNotExists(
      "silver", "pit_account", PIT_ACCOUNT_SCHEMA,
      Seq("snapshot_date"),
      Map("comment" -> "Daily account snapshots")
    )

    IcebergWriter.createTableIfNotExists(
      "silver", "pit_transaction", PIT_TRANSACTION_SCHEMA,
      Seq("snapshot_date"),
      Map("comment" -> "Daily transaction snapshots")
    )

    // Create Bridge tables
    println("\n🌉 Creating Bridge tables...")
    IcebergWriter.createTableIfNotExists(
      "silver", "bridge_customer_account", BRIDGE_CUSTOMER_ACCOUNT_SCHEMA,
      Seq("load_date"),
      Map("comment" -> "Pre-joined customer-account relationships")
    )

    println("\n✅ All Business Vault tables created successfully")

    // Show created tables
    println("\n📊 Business Vault Tables:")
    spark.sql("SHOW TABLES IN silver").show(false)
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PERFORMANCE BENEFITS OF BUSINESS VAULT                         │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * QUERY PERFORMANCE COMPARISON:
   *
   * 1. CUSTOMER 360 VIEW
   *    Without PIT: 5-table join with temporal logic (2-5 seconds)
   *    With PIT: 2-table simple join (< 1 second)
   *    Improvement: 5-10x faster
   *
   * 2. ACCOUNT BALANCE TREND
   *    Without PIT: Complex window functions over satellites
   *    With PIT: Simple GROUP BY snapshot_date
   *    Improvement: 10-20x faster
   *
   * 3. CUSTOMER-ACCOUNT QUERIES
   *    Without Bridge: 3-way join (hub-link-hub)
   *    With Bridge: Single table scan
   *    Improvement: 3-5x faster
   *
   * STORAGE COST:
   * - PIT tables: ~2x size of satellites (worth it for query speed)
   * - Bridge tables: Minimal overhead (just hash keys + metadata)
   *
   * REFRESH OVERHEAD:
   * - Daily PIT generation: 5-15 minutes (off-hours)
   * - Bridge updates: Real-time or hourly (fast)
   *
   * DATA VAULT PRINCIPLE:
   * Business Vault is OPTIONAL. Raw Vault alone is sufficient.
   * Add Business Vault when query performance matters.
   */
}

