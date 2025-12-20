package gold

/**
 * ========================================================================
 * DIMENSIONAL MODEL ETL - GOLD LAYER ORCHESTRATION
 * ========================================================================
 *
 * PURPOSE:
 * Build star schema from Business Vault (Silver) or Raw Vault (Bronze).
 *
 * LEARNING OBJECTIVES:
 * - SCD Type 2 implementation
 * - Surrogate key generation
 * - Dimension loading patterns
 * - Fact table loading
 * - Date dimension population
 *
 * ETL FLOW:
 * ```
 * Business Vault (Silver) or Raw Vault (Bronze)
 *     ↓
 * Generate Surrogate Keys (sequences)
 *     ↓
 * Load Dimensions (SCD Type 1 & 2)
 *     ↓
 * Load Facts (with FK lookups to dimensions)
 *     ↓
 * Validate referential integrity
 * ```
 *
 * ========================================================================
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import bronze.utils.{IcebergWriter, LoadMetadata}
import gold.utils.SCDType2Handler
import java.time.LocalDate

object DimensionalModelETL {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ MAIN ENTRY POINT                                                │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def main(args: Array[String]): Unit = {

    println("""
         |╔════════════════════════════════════════════════════════════════╗
         |║    DATA VAULT 2.0 - DIMENSIONAL MODEL ETL (GOLD LAYER)        ║
         |╚════════════════════════════════════════════════════════════════╝
         |""".stripMargin)

    val rebuildAll = args.contains("--rebuild-all")
    val rebuildDims = args.contains("--rebuild-dims") || rebuildAll
    val rebuildFacts = args.contains("--rebuild-facts") || rebuildAll

    println(s"""
         |Configuration:
         |  Rebuild Dimensions: $rebuildDims
         |  Rebuild Facts: $rebuildFacts
         |""".stripMargin)

    implicit val spark = createSparkSession()

    try {
      // Create dimensional model tables
      DimensionalModelSchema.createAllTables()

      // Load date dimension first (required for facts)
      loadDateDimension()

      if (rebuildDims) {
        loadDimensions()
      }

      if (rebuildFacts) {
        loadFacts()
      }

      println("\n✅ Dimensional Model ETL completed successfully")

    } catch {
      case e: Exception =>
        println(s"\n❌ Dimensional Model ETL failed: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ CREATE SPARK SESSION                                            │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createSparkSession(): SparkSession = {
    println("\n🚀 Initializing Spark Session...")

    val spark = SparkSession.builder()
      .appName("Dimensional Model ETL - Gold Layer")
      .config("spark.sql.extensions",
              "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println(s"✅ Spark ${spark.version} initialized")
    spark
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD ALL DIMENSIONS                                             │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadDimensions()(implicit spark: SparkSession): Unit = {

    println("""
         |┌────────────────────────────────────────────────────────────────┐
         |│ LOADING DIMENSION TABLES                                      │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    // Load SCD Type 1 dimensions (simpler - no history)
    loadDimProduct()
    loadDimBranch()

    // Load SCD Type 2 dimensions (track history)
    loadDimCustomer()
    loadDimAccount()
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD DIM_DATE (Pre-populate with calendar dates)               │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Generate date dimension with all dates from 2020-2030.
   *
   * PATTERN:
   * - One-time population (or yearly refresh)
   * - Includes holidays, fiscal calendar, etc.
   */
  def loadDateDimension()(implicit spark: SparkSession): Unit = {

    println("\n📅 Loading Date Dimension...")

    import spark.implicits._

    // Check if already populated
    val existingCount = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_date")
      .collect()(0).getLong(0)

    if (existingCount > 0) {
      println(s"   ℹ️  Date dimension already populated ($existingCount dates)")
      return
    }

    // Generate date range (2020-2030)
    val startDate = LocalDate.of(2020, 1, 1)
    val endDate = LocalDate.of(2030, 12, 31)

    val dates = Iterator.iterate(startDate)(_.plusDays(1))
      .takeWhile(d => d.isBefore(endDate) || d.isEqual(endDate))
      .toSeq

    // Create DataFrame with date attributes
    val dateDF = dates.map { date =>
      val dateKey = date.getYear * 10000 + date.getMonthValue * 100 + date.getDayOfMonth
      val dayOfWeek = date.getDayOfWeek.getValue % 7 + 1 // 1=Sunday, 7=Saturday
      val dayName = date.getDayOfWeek.toString
      val dayAbbrev = dayName.substring(0, 3)
      val weekOfYear = date.format(java.time.format.DateTimeFormatter.ofPattern("w")).toInt
      val month = date.getMonthValue
      val monthName = date.getMonth.toString
      val monthAbbrev = monthName.substring(0, 3)
      val quarter = (month - 1) / 3 + 1
      val quarterName = s"Q$quarter"
      val year = date.getYear
      val fiscalYear = if (month >= 7) year + 1 else year // July fiscal year start
      val fiscalQuarter = ((month + 5) % 12) / 3 + 1
      val isWeekend = dayOfWeek == 1 || dayOfWeek == 7
      val firstDayOfMonth = date.withDayOfMonth(1)
      val lastDayOfMonth = date.withDayOfMonth(date.lengthOfMonth())

      // Simplified holiday detection (US holidays)
      val (isHoliday, holidayName) = getHolidayInfo(date)

      val isBusinessDay = !isWeekend && !isHoliday

      (dateKey, java.sql.Date.valueOf(date), date.getDayOfMonth, dayOfWeek, dayName, dayAbbrev,
       weekOfYear, weekOfYear, month, monthName, monthAbbrev,
       java.sql.Date.valueOf(firstDayOfMonth), java.sql.Date.valueOf(lastDayOfMonth),
       quarter, quarterName, year, fiscalYear, fiscalQuarter,
       isWeekend, isHoliday, holidayName, isBusinessDay)
    }.toDF(
      "date_key", "date", "day_of_month", "day_of_week", "day_name", "day_abbrev",
      "week_of_year", "iso_week", "month", "month_name", "month_abbrev",
      "first_day_of_month", "last_day_of_month",
      "quarter", "quarter_name", "year", "fiscal_year", "fiscal_quarter",
      "is_weekend", "is_holiday", "holiday_name", "is_business_day"
    )

    // Write to dim_date
    IcebergWriter.appendToTable(dateDF, "gold", "dim_date", Seq.empty)

    println(s"   ✅ Populated date dimension with ${dates.size} dates")
  }

  /**
   * Helper: Detect US holidays (simplified)
   */
  def getHolidayInfo(date: LocalDate): (Boolean, Option[String]) = {
    val month = date.getMonthValue
    val day = date.getDayOfMonth

    // Fixed date holidays
    if (month == 1 && day == 1) return (true, Some("New Year's Day"))
    if (month == 7 && day == 4) return (true, Some("Independence Day"))
    if (month == 12 && day == 25) return (true, Some("Christmas Day"))

    // No holiday
    (false, None)
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD DIM_PRODUCT (SCD Type 1)                                  │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadDimProduct()(implicit spark: SparkSession): Unit = {

    println("\n📦 Loading Dim_Product (SCD Type 1)...")

    import spark.implicits._

    // Read from bronze (reference data doesn't need PIT)
    val productDF = spark.sql("""
      SELECT
        product_id,
        product_name,
        product_category,
        product_subcategory,
        interest_rate,
        min_balance,
        monthly_fee,
        is_active
      FROM bronze.product  -- Assuming reference table exists
    """)

    // Generate surrogate keys (monotonic_id or row_number)
    val windowSpec = Window.orderBy("product_id")

    val dimProductDF = productDF
      .withColumn("product_key", row_number().over(windowSpec).cast("long"))
      .withColumn("record_source", lit("Bronze"))
      .withColumn("load_date", current_date())
      .withColumn("last_updated_date", current_date())
      .select(
        $"product_key",
        $"product_id",
        $"product_name",
        $"product_category",
        $"product_subcategory",
        $"interest_rate",
        $"min_balance",
        $"monthly_fee",
        $"is_active",
        $"record_source",
        $"load_date",
        $"last_updated_date"
      )

    // Overwrite entire dimension (SCD Type 1 pattern)
    dimProductDF.writeTo("gold.dim_product")
      .using("iceberg")
      .overwritePartitions()

    val rowCount = dimProductDF.count()
    println(s"   ✅ Loaded $rowCount products to Dim_Product")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD DIM_BRANCH (SCD Type 1)                                   │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadDimBranch()(implicit spark: SparkSession): Unit = {

    println("\n🏢 Loading Dim_Branch (SCD Type 1)...")

    import spark.implicits._

    val branchDF = spark.sql("""
      SELECT
        branch_id,
        branch_name,
        branch_code,
        city,
        state,
        zip_code,
        region,
        is_active
      FROM bronze.branch
    """)

    val windowSpec = Window.orderBy("branch_id")

    val dimBranchDF = branchDF
      .withColumn("branch_key", row_number().over(windowSpec).cast("long"))
      .withColumn("record_source", lit("Bronze"))
      .withColumn("load_date", current_date())
      .withColumn("last_updated_date", current_date())

    dimBranchDF.writeTo("gold.dim_branch")
      .using("iceberg")
      .overwritePartitions()

    val rowCount = dimBranchDF.count()
    println(s"   ✅ Loaded $rowCount branches to Dim_Branch")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD DIM_CUSTOMER (SCD Type 2)                                 │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * SCD TYPE 2 LOGIC:
   * 1. Read current dimension
   * 2. Read latest PIT customer data
   * 3. Compare and find changes
   * 4. End-date expired versions
   * 5. Insert new versions with new surrogate keys
   */
  def loadDimCustomer()(implicit spark: SparkSession): Unit = {

    println("\n👤 Loading Dim_Customer (SCD Type 2)...")

    import spark.implicits._

    // Read latest PIT customer (or current raw vault)
    val sourceDF = spark.sql("""
      SELECT
        customer_id,
        customer_type,
        first_name,
        last_name,
        company_name,
        email,
        phone,
        date_of_birth,
        customer_since,
        customer_status,
        credit_score,
        loyalty_tier
      FROM silver.pit_customer
      WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM silver.pit_customer)
    """)

    // Add derived attributes
    val enrichedSourceDF = sourceDF
      .withColumn("full_name",
        when($"customer_type" === "INDIVIDUAL",
          concat($"first_name", lit(" "), $"last_name"))
        .otherwise($"company_name")
      )
      .withColumn("age",
        when($"date_of_birth".isNotNull,
          year(current_date()) - year($"date_of_birth"))
        .otherwise(null)
      )
      .withColumn("tenure_years",
        when($"customer_since".isNotNull,
          year(current_date()) - year($"customer_since"))
        .otherwise(null)
      )
      .withColumn("credit_category",
        when($"credit_score" >= 750, "Excellent")
        .when($"credit_score" >= 700, "Good")
        .when($"credit_score" >= 650, "Fair")
        .when($"credit_score".isNotNull, "Poor")
        .otherwise(null)
      )

    // Use SCD Type 2 handler
    val dimCustomerDF = SCDType2Handler.processSCDType2(
      sourceDF = enrichedSourceDF,
      targetTable = "gold.dim_customer",
      naturalKey = "customer_id",
      surrogateKey = "customer_key",
      compareColumns = Seq("customer_type", "first_name", "last_name", "email",
                           "customer_status", "loyalty_tier")
    )

    val rowCount = dimCustomerDF.count()
    println(s"   ✅ Processed $rowCount customer records (SCD Type 2)")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD DIM_ACCOUNT (SCD Type 2)                                  │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadDimAccount()(implicit spark: SparkSession): Unit = {

    println("\n🏦 Loading Dim_Account (SCD Type 2)...")

    import spark.implicits._

    // Read latest PIT account
    val sourceDF = spark.sql("""
      SELECT
        account_id,
        account_number,
        account_type,
        account_status,
        currency_code,
        interest_rate,
        credit_limit,
        opened_date,
        product_id,
        branch_id
      FROM silver.pit_account
      WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM silver.pit_account)
    """)

    // Add derived attributes
    val enrichedSourceDF = sourceDF
      .withColumn("account_age_months",
        when($"opened_date".isNotNull,
          months_between(current_date(), $"opened_date").cast("int"))
        .otherwise(null)
      )

    // Lookup dimension keys for product and branch
    val withKeysDF = enrichedSourceDF
      .join(
        spark.table("gold.dim_product").select($"product_id", $"product_key"),
        Seq("product_id"),
        "left"
      )
      .join(
        spark.table("gold.dim_branch").select($"branch_id", $"branch_key"),
        Seq("branch_id"),
        "left"
      )

    val dimAccountDF = SCDType2Handler.processSCDType2(
      sourceDF = withKeysDF,
      targetTable = "gold.dim_account",
      naturalKey = "account_id",
      surrogateKey = "account_key",
      compareColumns = Seq("account_type", "account_status", "interest_rate", "credit_limit")
    )

    val rowCount = dimAccountDF.count()
    println(s"   ✅ Processed $rowCount account records (SCD Type 2)")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD ALL FACTS                                                  │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadFacts()(implicit spark: SparkSession): Unit = {

    println("""
         |┌────────────────────────────────────────────────────────────────┐
         |│ LOADING FACT TABLES                                           │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    loadFactTransaction()
    loadFactAccountBalance()
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD FACT_TRANSACTION                                           │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadFactTransaction()(implicit spark: SparkSession): Unit = {

    println("\n📊 Loading Fact_Transaction...")

    import spark.implicits._

    // Read from bronze vault
    val transactionDF = spark.sql("""
      SELECT
        t.transaction_id,
        t.transaction_number,
        st.transaction_date,
        st.posting_date,
        st.transaction_type,
        st.channel,
        st.status,
        st.total_amount,
        a.account_id,
        ca.customer_id
      FROM bronze.hub_transaction t
      JOIN bronze.sat_transaction st ON t.transaction_hash_key = st.transaction_hash_key
        AND st.valid_to IS NULL
      JOIN bronze.hub_account a ON TRUE  -- Simplified - need proper link
      JOIN bronze.link_customer_account ca ON a.account_hash_key = ca.account_hash_key
    """)

    // Lookup dimension keys
    val factDF = transactionDF
      .join(
        spark.table("gold.dim_date").select($"date".as("transaction_date"), $"date_key".as("transaction_date_key")),
        Seq("transaction_date"),
        "left"
      )
      .join(
        spark.table("gold.dim_date").select($"date".as("posting_date"), $"date_key".as("posting_date_key")),
        Seq("posting_date"),
        "left"
      )
      .join(
        spark.table("gold.dim_customer").filter($"is_current" === true)
          .select($"customer_id", $"customer_key"),
        Seq("customer_id"),
        "left"
      )
      .join(
        spark.table("gold.dim_account").filter($"is_current" === true)
          .select($"account_id", $"account_key"),
        Seq("account_id"),
        "left"
      )
      .withColumn("transaction_key", monotonically_increasing_id())
      .withColumn("item_count", lit(1)) // Placeholder
      .withColumn("account_balance_before", lit(null).cast("decimal(15,2)"))
      .withColumn("account_balance_after", lit(null).cast("decimal(15,2)"))
      .withColumn("record_source", lit("Bronze"))
      .withColumn("load_date", current_date())
      .select(
        $"transaction_key",
        $"transaction_id",
        $"transaction_number",
        $"transaction_date_key",
        $"posting_date_key",
        $"customer_key",
        $"account_key",
        $"transaction_type",
        $"channel",
        $"status",
        $"total_amount",
        $"account_balance_before",
        $"account_balance_after",
        $"item_count",
        $"record_source",
        $"load_date"
      )

    IcebergWriter.appendToTable(factDF, "gold", "fact_transaction", Seq("transaction_date_key"))

    val rowCount = factDF.count()
    println(s"   ✅ Loaded $rowCount transactions to Fact_Transaction")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD FACT_ACCOUNT_BALANCE (Snapshot Fact)                      │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadFactAccountBalance()(implicit spark: SparkSession): Unit = {

    println("\n📊 Loading Fact_Account_Balance...")

    import spark.implicits._

    // Read from PIT account (already has daily snapshots)
    val snapshotDF = spark.sql("""
      SELECT
        a.account_id,
        a.snapshot_date,
        a.balance,
        a.available_balance,
        a.account_status,
        ca.customer_id,
        a.product_id,
        a.branch_id
      FROM silver.pit_account a
      JOIN bronze.link_customer_account ca ON a.account_hash_key = ca.account_hash_key
    """)

    // Lookup dimension keys
    val factDF = snapshotDF
      .join(
        spark.table("gold.dim_date").select($"date".as("snapshot_date"), $"date_key".as("snapshot_date_key")),
        Seq("snapshot_date"),
        "left"
      )
      .join(
        spark.table("gold.dim_customer").filter($"is_current" === true)
          .select($"customer_id", $"customer_key"),
        Seq("customer_id"),
        "left"
      )
      .join(
        spark.table("gold.dim_account").filter($"is_current" === true)
          .select($"account_id", $"account_key"),
        Seq("account_id"),
        "left"
      )
      .join(
        spark.table("gold.dim_product").select($"product_id", $"product_key"),
        Seq("product_id"),
        "left"
      )
      .join(
        spark.table("gold.dim_branch").select($"branch_id", $"branch_key"),
        Seq("branch_id"),
        "left"
      )
      .withColumn("balance_change", lit(0).cast("decimal(15,2)")) // Placeholder
      .withColumn("transaction_count", lit(0))
      .withColumn("deposit_amount", lit(0).cast("decimal(15,2)"))
      .withColumn("withdrawal_amount", lit(0).cast("decimal(15,2)"))
      .withColumn("record_source", lit("Silver-PIT"))
      .withColumn("load_date", current_date())

    IcebergWriter.appendToTable(factDF, "gold", "fact_account_balance", Seq("snapshot_date_key"))

    val rowCount = factDF.count()
    println(s"   ✅ Loaded $rowCount snapshots to Fact_Account_Balance")
  }
}

