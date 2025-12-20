package gold

/**
 * ========================================================================
 * DIMENSIONAL MODEL SCHEMA DEFINITIONS (GOLD LAYER)
 * ========================================================================
 *
 * PURPOSE:
 * Define star schema for business intelligence and analytics.
 *
 * LEARNING OBJECTIVES:
 * - Star schema design (Kimball methodology)
 * - Slowly Changing Dimensions (SCD) Type 1 and Type 2
 * - Fact table design
 * - Surrogate keys vs. natural keys
 * - Date dimension pattern
 *
 * KIMBALL DIMENSIONAL MODELING:
 *
 * Star schema consists of:
 * 1. DIMENSION TABLES: Who, What, Where, When, Why
 * 2. FACT TABLES: Measurements (metrics, quantities, amounts)
 *
 * ARCHITECTURE:
 * ```
 *              ┌──────────────┐
 *              │ Dim_Date     │
 *              └──────┬───────┘
 *                     │
 *              ┌──────▼────────────────┐
 *              │  Fact_Transaction     │
 *              │  (Measurements)       │
 *              └─┬────┬────┬────┬─────┘
 *         ┌──────┘    │    │    └──────┐
 *         │           │    │           │
 *    ┌────▼─────┐ ┌──▼────▼───┐  ┌────▼──────┐
 *    │Dim_      │ │ Dim_      │  │ Dim_      │
 *    │Customer  │ │ Account   │  │ Product   │
 *    │(SCD 2)   │ │ (SCD 2)   │  │ (SCD 1)   │
 *    └──────────┘ └───────────┘  └───────────┘
 * ```
 *
 * SLOWLY CHANGING DIMENSIONS:
 *
 * SCD TYPE 1: Overwrite
 * - Update in place
 * - No history
 * - Use for: Correcting errors, minor attributes
 * - Example: Product description typo fix
 *
 * SCD TYPE 2: Historical Tracking
 * - Add new row for changes
 * - Keep full history
 * - Use for: Significant business changes
 * - Example: Customer address change, loyalty tier change
 *
 * SURROGATE KEYS:
 * - Artificial sequence number (1, 2, 3...)
 * - Stable even if natural key changes
 * - Enables SCD Type 2 (multiple rows per natural key)
 * - Fast integer joins (vs. string hash keys)
 *
 * ========================================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import bronze.utils.IcebergWriter

object DimensionalModelSchema {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ DIM_CUSTOMER - Customer Dimension (SCD Type 2)                 │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Track customer history with SCD Type 2 (versioning).
   *
   * SCD TYPE 2 PATTERN:
   * - New row for each change
   * - effective_date / expiration_date track validity
   * - is_current flag for latest version
   * - Surrogate key (customer_key) for versioning
   *
   * EXAMPLE:
   * ```
   * customer_key | customer_id | name    | tier   | effective_date | expiration_date | is_current
   * -------------|-------------|---------|--------|----------------|-----------------|------------
   * 1001         | 123         | John    | SILVER | 2024-01-01     | 2025-01-15      | N
   * 1002         | 123         | John    | GOLD   | 2025-01-15     | 9999-12-31      | Y
   * ```
   *
   * QUERY PATTERNS:
   * - Current records: WHERE is_current = TRUE
   * - Point-in-time: WHERE effective_date <= '2025-01-10' AND expiration_date > '2025-01-10'
   * - History: GROUP BY customer_id (all versions)
   */
  val DIM_CUSTOMER_SCHEMA = StructType(Seq(
    // Surrogate key (generated sequence)
    StructField("customer_key", LongType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "Surrogate key (sequence number) - Primary key for fact joins")
        .build()),

    // Natural business key
    StructField("customer_id", IntegerType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "Natural business key from source")
        .build()),

    // Descriptive attributes
    StructField("customer_type", StringType, nullable = true),
    StructField("first_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true),
    StructField("full_name", StringType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Derived: first_name + last_name OR company_name")
        .build()),
    StructField("company_name", StringType, nullable = true),
    StructField("email", StringType, nullable = true),
    StructField("phone", StringType, nullable = true),
    StructField("date_of_birth", DateType, nullable = true),
    StructField("age", IntegerType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Derived: Current year - birth year")
        .build()),
    StructField("customer_since", DateType, nullable = true),
    StructField("tenure_years", IntegerType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Derived: Years since customer_since")
        .build()),
    StructField("customer_status", StringType, nullable = true),
    StructField("credit_score", IntegerType, nullable = true),
    StructField("credit_category", StringType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Derived: Excellent/Good/Fair/Poor based on credit_score")
        .build()),
    StructField("loyalty_tier", StringType, nullable = true),

    // SCD Type 2 metadata
    StructField("effective_date", DateType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "When this version became active")
        .build()),
    StructField("expiration_date", DateType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "When this version expired (9999-12-31 for current)")
        .build()),
    StructField("is_current", BooleanType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "TRUE for current version, FALSE for historical")
        .build()),

    // Audit
    StructField("record_source", StringType, nullable = false),
    StructField("load_date", DateType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ DIM_ACCOUNT - Account Dimension (SCD Type 2)                   │
   * └─────────────────────────────────────────────────────────────────┘
   */
  val DIM_ACCOUNT_SCHEMA = StructType(Seq(
    StructField("account_key", LongType, nullable = false),
    StructField("account_id", IntegerType, nullable = false),

    // Descriptive attributes
    StructField("account_number", StringType, nullable = true),
    StructField("account_type", StringType, nullable = true),
    StructField("account_status", StringType, nullable = true),
    StructField("currency_code", StringType, nullable = true),
    StructField("interest_rate", DecimalType(5, 4), nullable = true),
    StructField("credit_limit", DecimalType(15, 2), nullable = true),
    StructField("opened_date", DateType, nullable = true),
    StructField("account_age_months", IntegerType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Derived: Months since opened_date")
        .build()),

    // Foreign keys to other dimensions (denormalized)
    StructField("product_key", LongType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Reference to Dim_Product")
        .build()),
    StructField("branch_key", LongType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Reference to Dim_Branch")
        .build()),

    // SCD Type 2 metadata
    StructField("effective_date", DateType, nullable = false),
    StructField("expiration_date", DateType, nullable = false),
    StructField("is_current", BooleanType, nullable = false),

    // Audit
    StructField("record_source", StringType, nullable = false),
    StructField("load_date", DateType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ DIM_PRODUCT - Product Dimension (SCD Type 1)                   │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * SCD TYPE 1:
   * - No versioning (update in place)
   * - Simpler than Type 2
   * - Use for reference data that changes infrequently
   */
  val DIM_PRODUCT_SCHEMA = StructType(Seq(
    StructField("product_key", LongType, nullable = false),
    StructField("product_id", IntegerType, nullable = false),
    StructField("product_name", StringType, nullable = true),
    StructField("product_category", StringType, nullable = true),
    StructField("product_subcategory", StringType, nullable = true),
    StructField("interest_rate", DecimalType(5, 4), nullable = true),
    StructField("min_balance", DecimalType(15, 2), nullable = true),
    StructField("monthly_fee", DecimalType(10, 2), nullable = true),
    StructField("is_active", BooleanType, nullable = false),

    // Audit (no SCD Type 2 metadata)
    StructField("record_source", StringType, nullable = false),
    StructField("load_date", DateType, nullable = false),
    StructField("last_updated_date", DateType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ DIM_BRANCH - Branch Dimension (SCD Type 1)                     │
   * └─────────────────────────────────────────────────────────────────┘
   */
  val DIM_BRANCH_SCHEMA = StructType(Seq(
    StructField("branch_key", LongType, nullable = false),
    StructField("branch_id", IntegerType, nullable = false),
    StructField("branch_name", StringType, nullable = true),
    StructField("branch_code", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("state", StringType, nullable = true),
    StructField("zip_code", StringType, nullable = true),
    StructField("region", StringType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Derived: Geographic region (Northeast, South, etc.)")
        .build()),
    StructField("is_active", BooleanType, nullable = false),

    // Audit
    StructField("record_source", StringType, nullable = false),
    StructField("load_date", DateType, nullable = false),
    StructField("last_updated_date", DateType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ DIM_DATE - Date Dimension (Pre-populated)                      │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Date dimension for time-based analytics.
   *
   * PATTERN:
   * - Pre-populated with all dates (e.g., 2020-2030)
   * - One row per calendar date
   * - Rich attributes (day of week, month, quarter, fiscal year, holidays)
   *
   * BENEFITS:
   * - Enables time intelligence queries
   * - Fiscal calendar support
   * - Holiday analysis
   * - Year-over-year comparisons
   */
  val DIM_DATE_SCHEMA = StructType(Seq(
    StructField("date_key", IntegerType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "Surrogate key: YYYYMMDD format (e.g., 20250115)")
        .build()),
    StructField("date", DateType, nullable = false),

    // Day attributes
    StructField("day_of_month", IntegerType, nullable = false),
    StructField("day_of_week", IntegerType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "1=Sunday, 7=Saturday")
        .build()),
    StructField("day_name", StringType, nullable = false),
    StructField("day_abbrev", StringType, nullable = false),

    // Week attributes
    StructField("week_of_year", IntegerType, nullable = false),
    StructField("iso_week", IntegerType, nullable = false),

    // Month attributes
    StructField("month", IntegerType, nullable = false),
    StructField("month_name", StringType, nullable = false),
    StructField("month_abbrev", StringType, nullable = false),
    StructField("first_day_of_month", DateType, nullable = false),
    StructField("last_day_of_month", DateType, nullable = false),

    // Quarter attributes
    StructField("quarter", IntegerType, nullable = false),
    StructField("quarter_name", StringType, nullable = false),

    // Year attributes
    StructField("year", IntegerType, nullable = false),

    // Fiscal year (if different from calendar year)
    StructField("fiscal_year", IntegerType, nullable = false),
    StructField("fiscal_quarter", IntegerType, nullable = false),

    // Flags
    StructField("is_weekend", BooleanType, nullable = false),
    StructField("is_holiday", BooleanType, nullable = false),
    StructField("holiday_name", StringType, nullable = true),
    StructField("is_business_day", BooleanType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ FACT_TRANSACTION - Transaction Fact Table                      │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Core fact table for transaction analytics.
   *
   * FACT TABLE CHARACTERISTICS:
   * - Contains measurements (amounts, quantities)
   * - Foreign keys to dimensions
   * - Grain: One row per transaction
   * - Additive measures can be summed across all dimensions
   *
   * GRAIN DEFINITION:
   * "One row per transaction header"
   */
  val FACT_TRANSACTION_SCHEMA = StructType(Seq(
    // Surrogate key (optional for facts)
    StructField("transaction_key", LongType, nullable = false),

    // Natural business key
    StructField("transaction_id", IntegerType, nullable = false),
    StructField("transaction_number", StringType, nullable = true),

    // Foreign keys to dimensions
    StructField("transaction_date_key", IntegerType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "FK to Dim_Date on transaction_date")
        .build()),
    StructField("posting_date_key", IntegerType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "FK to Dim_Date on posting_date")
        .build()),
    StructField("customer_key", LongType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "FK to Dim_Customer (SCD Type 2)")
        .build()),
    StructField("account_key", LongType, nullable = false,
      metadata = new MetadataBuilder()
        .putString("comment", "FK to Dim_Account (SCD Type 2)")
        .build()),

    // Degenerate dimensions (attributes in fact, no separate dim)
    StructField("transaction_type", StringType, nullable = true),
    StructField("channel", StringType, nullable = true),
    StructField("status", StringType, nullable = true),

    // Measures (additive)
    StructField("total_amount", DecimalType(15, 2), nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Additive measure - can be summed")
        .build()),

    // Measures (semi-additive - balance snapshots)
    StructField("account_balance_before", DecimalType(15, 2), nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Semi-additive - balance before transaction")
        .build()),
    StructField("account_balance_after", DecimalType(15, 2), nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Semi-additive - balance after transaction")
        .build()),

    // Counts (additive)
    StructField("item_count", IntegerType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Number of line items in this transaction")
        .build()),

    // Audit
    StructField("record_source", StringType, nullable = false),
    StructField("load_date", DateType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ FACT_ACCOUNT_BALANCE - Daily Account Balance Snapshot          │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Daily snapshot of account balances (periodic snapshot fact table).
   *
   * GRAIN:
   * "One row per account per day"
   *
   * SNAPSHOT FACT PATTERN:
   * - Captures state at regular intervals
   * - Semi-additive measures (balances)
   * - Enables trend analysis
   */
  val FACT_ACCOUNT_BALANCE_SCHEMA = StructType(Seq(
    // Composite key
    StructField("account_key", LongType, nullable = false),
    StructField("snapshot_date_key", IntegerType, nullable = false),

    // Natural keys
    StructField("account_id", IntegerType, nullable = false),
    StructField("snapshot_date", DateType, nullable = false),

    // Dimensions
    StructField("customer_key", LongType, nullable = false),
    StructField("product_key", LongType, nullable = false),
    StructField("branch_key", LongType, nullable = false),

    // Semi-additive measures (balances at point in time)
    StructField("balance", DecimalType(15, 2), nullable = true),
    StructField("available_balance", DecimalType(15, 2), nullable = true),

    // Additive measures (changes since last snapshot)
    StructField("balance_change", DecimalType(15, 2), nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Change from previous day")
        .build()),
    StructField("transaction_count", IntegerType, nullable = true,
      metadata = new MetadataBuilder()
        .putString("comment", "Number of transactions this day")
        .build()),
    StructField("deposit_amount", DecimalType(15, 2), nullable = true),
    StructField("withdrawal_amount", DecimalType(15, 2), nullable = true),

    // Status
    StructField("account_status", StringType, nullable = true),

    // Audit
    StructField("record_source", StringType, nullable = false),
    StructField("load_date", DateType, nullable = false)
  ))

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ CREATE ALL DIMENSIONAL MODEL TABLES                            │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createAllTables()(implicit spark: SparkSession): Unit = {

    println("""
         |┌────────────────────────────────────────────────────────────────┐
         |│ CREATING DIMENSIONAL MODEL TABLES (GOLD LAYER)               │
         |├────────────────────────────────────────────────────────────────┤
         |│ Database: gold                                                 │
         |│ Format: Apache Iceberg v2                                      │
         |│ Design: Kimball Star Schema                                    │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    // Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    // Create dimensions
    println("\n⭐ Creating Dimension tables...")
    IcebergWriter.createTableIfNotExists("gold", "dim_customer", DIM_CUSTOMER_SCHEMA, Seq("load_date"))
    IcebergWriter.createTableIfNotExists("gold", "dim_account", DIM_ACCOUNT_SCHEMA, Seq("load_date"))
    IcebergWriter.createTableIfNotExists("gold", "dim_product", DIM_PRODUCT_SCHEMA, Seq("load_date"))
    IcebergWriter.createTableIfNotExists("gold", "dim_branch", DIM_BRANCH_SCHEMA, Seq("load_date"))
    IcebergWriter.createTableIfNotExists("gold", "dim_date", DIM_DATE_SCHEMA, Seq.empty) // No partitioning

    // Create facts
    println("\n📊 Creating Fact tables...")
    IcebergWriter.createTableIfNotExists("gold", "fact_transaction", FACT_TRANSACTION_SCHEMA, Seq("transaction_date_key"))
    IcebergWriter.createTableIfNotExists("gold", "fact_account_balance", FACT_ACCOUNT_BALANCE_SCHEMA, Seq("snapshot_date_key"))

    println("\n✅ All Dimensional Model tables created")

    // Show created tables
    println("\n📊 Dimensional Model Tables:")
    spark.sql("SHOW TABLES IN gold").show(false)
  }
}

