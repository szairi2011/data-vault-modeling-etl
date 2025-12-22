package silver

/**
 * ========================================================================
 * BUSINESS VAULT ETL - SILVER LAYER ORCHESTRATION
 * ========================================================================
 *
 * PURPOSE:
 * Generate Point-in-Time (PIT) snapshots and Bridge tables from Raw Vault.
 *
 * LEARNING OBJECTIVES:
 * - How to generate daily snapshots efficiently
 * - Window functions for temporal queries
 * - Pre-joining strategy implementation
 * - Business rule application
 * - Performance optimization techniques
 *
 * ETL FLOW:
 * Raw Vault (Bronze)
 *     ↓
 * Read Hub + Satellites with temporal logic
 *     ↓
 * Generate snapshot date range
 *     ↓
 * For each snapshot date, get latest satellite values
 *     ↓
 * Write to PIT tables (Silver)
 *
 * Raw Vault Links
 *     ↓
 * Join Hubs + current Satellites
 *     ↓
 * Apply business rules (primary account, etc.)
 *     ↓
 * Write to Bridge tables (Silver)
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import bronze.utils.{IcebergWriter, LoadMetadata}
import java.time.LocalDate

object BusinessVaultETL {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ MAIN ENTRY POINT                                                │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def main(args: Array[String]): Unit = {

    println("""
         |╔════════════════════════════════════════════════════════════════╗
         |║      DATA VAULT 2.0 - BUSINESS VAULT ETL (SILVER LAYER)       ║
         |╚════════════════════════════════════════════════════════════════╝
         |""".stripMargin)

    // Parse command-line arguments
    val buildPIT = args.contains("--build-pit") || args.contains("--all")
    val buildBridge = args.contains("--build-bridge") || args.contains("--all")
    val snapshotDate = if (args.contains("--date")) {
      LocalDate.parse(args(args.indexOf("--date") + 1))
    } else {
      LocalDate.now()
    }

    println(s"""
         |Configuration:
         |  Build PIT: $buildPIT
         |  Build Bridge: $buildBridge
         |  Snapshot Date: $snapshotDate
         |""".stripMargin)

    // Initialize Spark Session
    implicit val spark: SparkSession = createSparkSession()

    try {
      // Create Business Vault tables if not exist
      BusinessVaultSchema.createAllTables()

      if (buildPIT || (!buildPIT && !buildBridge)) {
        buildPITTables(snapshotDate)
      }

      if (buildBridge || (!buildPIT && !buildBridge)) {
        buildBridgeTables()
      }

      println("\n✅ Business Vault ETL completed successfully")

    } catch {
      case e: Exception =>
        println(s"\n❌ Business Vault ETL failed: ${e.getMessage}")
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
      .appName("Business Vault ETL - Silver Layer")
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
   * │ BUILD ALL PIT TABLES                                            │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def buildPITTables(snapshotDate: LocalDate)(implicit spark: SparkSession): Unit = {

    println("""
         |┌────────────────────────────────────────────────────────────────┐
         |│ BUILDING POINT-IN-TIME (PIT) TABLES                          │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    buildPITCustomer(snapshotDate)
    buildPITAccount(snapshotDate)
    buildPITTransaction(snapshotDate)
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BUILD PIT_CUSTOMER                                              │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * ALGORITHM:
   * 1. Read Hub_Customer (all customers)
   * 2. Read Sat_Customer (all versions)
   * 3. For the given snapshot_date:
   *    - Find the latest satellite record where valid_from <= snapshot_date
   *    - If no record exists before snapshot_date, skip customer
   * 4. Join hub + selected satellite version
   * 5. Write to PIT_Customer
   *
   * WINDOW FUNCTION LOGIC:
   * SELECT *,
   *   ROW_NUMBER() OVER (
   *     PARTITION BY customer_hash_key
   *     ORDER BY valid_from DESC
   *   ) as rn
   * FROM sat_customer
   * WHERE valid_from <= snapshot_date
   * ```
   * Then filter rn = 1 to get latest version.
   */
  def buildPITCustomer(snapshotDate: LocalDate)(implicit spark: SparkSession): Unit = {

    println(s"\n📸 Building PIT_Customer for $snapshotDate...")

    import spark.implicits._

    val loadId = LoadMetadata.startLoad(
      entityName = "pit_customer",
      recordSource = "Bronze-Sat_Customer",
      loadDate = snapshotDate
    )

    try {
      // Read Hub_Customer
      val hubCustomerDF = spark.table("bronze.hub_customer")

      // Read Sat_Customer (current and historical)
      val satCustomerDF = spark.table("bronze.sat_customer")
        .filter($"valid_from" <= lit(snapshotDate.toString))

      // Window function to get latest satellite version as of snapshot_date
      val windowSpec = Window
        .partitionBy("customer_hash_key")
        .orderBy($"valid_from".desc)

      val latestSatDF = satCustomerDF
        .withColumn("rn", row_number().over(windowSpec))
        .filter($"rn" === 1)
        .drop("rn")

      // Join hub + latest satellite
      val pitCustomerDF = hubCustomerDF
        .join(latestSatDF, Seq("customer_hash_key"), "inner")
        .select(
          $"customer_hash_key",
          $"customer_id",
          lit(snapshotDate.toString).cast("date").as("snapshot_date"),
          $"customer_type",
          $"first_name",
          $"last_name",
          $"company_name",
          $"email",
          $"phone",
          $"date_of_birth",
          $"tax_id",
          $"customer_since",
          $"customer_status",
          $"credit_score",
          col("loyalty_tier").cast("string").as("loyalty_tier"), // May not exist in old data
          $"valid_from",
          $"valid_to",
          current_date().as("load_date"),
          lit("Bronze").as("record_source")
        )

      val rowCount = pitCustomerDF.count()

      // Check if snapshot already exists
      val existingCount = spark.sql(
        s"""
           |SELECT COUNT(*) as cnt
           |FROM silver.pit_customer
           |WHERE snapshot_date = DATE('$snapshotDate')
           |""".stripMargin
      ).collect()(0).getLong(0)

      if (existingCount > 0) {
        println(s"⚠️  Snapshot for $snapshotDate already exists ($existingCount records)")
        println("   Use --overwrite to replace existing snapshot")
      } else {
        // Write to PIT table
        IcebergWriter.appendToTable(
          pitCustomerDF,
          "silver",
          "pit_customer",
          Seq("snapshot_date")
        )

        println(s"✅ Created PIT_Customer snapshot with $rowCount records")
      }

      LoadMetadata.completeLoad(loadId, rowCount, rowCount)

    } catch {
      case e: Exception =>
        LoadMetadata.failLoad(loadId, e.getMessage)
        throw e
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BUILD PIT_ACCOUNT                                               │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def buildPITAccount(snapshotDate: LocalDate)(implicit spark: SparkSession): Unit = {

    println(s"\n📸 Building PIT_Account for $snapshotDate...")

    import spark.implicits._

    val loadId = LoadMetadata.startLoad(
      entityName = "pit_account",
      recordSource = "Bronze-Sat_Account",
      loadDate = snapshotDate
    )

    try {
      val hubAccountDF = spark.table("bronze.hub_account")

      val satAccountDF = spark.table("bronze.sat_account")
        .filter($"valid_from" <= lit(snapshotDate.toString))

      val windowSpec = Window
        .partitionBy("account_hash_key")
        .orderBy($"valid_from".desc)

      val latestSatDF = satAccountDF
        .withColumn("rn", row_number().over(windowSpec))
        .filter($"rn" === 1)
        .drop("rn")

      val pitAccountDF = hubAccountDF
        .join(latestSatDF, Seq("account_hash_key"), "inner")
        .select(
          $"account_hash_key",
          $"account_id",
          lit(snapshotDate.toString).cast("date").as("snapshot_date"),
          $"account_number",
          $"account_type",
          $"product_id",
          $"branch_id",
          $"balance",
          $"available_balance",
          $"currency_code",
          $"interest_rate",
          $"credit_limit",
          $"opened_date",
          $"account_status",
          $"valid_from",
          $"valid_to",
          current_date().as("load_date"),
          lit("Bronze").as("record_source")
        )

      val rowCount = pitAccountDF.count()

      val existingCount = spark.sql(
        s"""
           |SELECT COUNT(*) as cnt
           |FROM silver.pit_account
           |WHERE snapshot_date = DATE('$snapshotDate')
           |""".stripMargin
      ).collect()(0).getLong(0)

      if (existingCount == 0) {
        IcebergWriter.appendToTable(
          pitAccountDF,
          "silver",
          "pit_account",
          Seq("snapshot_date")
        )
        println(s"✅ Created PIT_Account snapshot with $rowCount records")
      } else {
        println(s"⚠️  Snapshot for $snapshotDate already exists")
      }

      LoadMetadata.completeLoad(loadId, rowCount, rowCount)

    } catch {
      case e: Exception =>
        LoadMetadata.failLoad(loadId, e.getMessage)
        throw e
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BUILD PIT_TRANSACTION                                           │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def buildPITTransaction(snapshotDate: LocalDate)(implicit spark: SparkSession): Unit = {

    println(s"\n📸 Building PIT_Transaction for $snapshotDate...")

    import spark.implicits._

    val loadId = LoadMetadata.startLoad(
      entityName = "pit_transaction",
      recordSource = "Bronze-Sat_Transaction",
      loadDate = snapshotDate
    )

    try {
      val hubTransactionDF = spark.table("bronze.hub_transaction")

      val satTransactionDF = spark.table("bronze.sat_transaction")
        .filter($"valid_from" <= lit(snapshotDate.toString))

      val windowSpec = Window
        .partitionBy("transaction_hash_key")
        .orderBy($"valid_from".desc)

      val latestSatDF = satTransactionDF
        .withColumn("rn", row_number().over(windowSpec))
        .filter($"rn" === 1)
        .drop("rn")

      val pitTransactionDF = hubTransactionDF
        .join(latestSatDF, Seq("transaction_hash_key"), "inner")
        .select(
          $"transaction_hash_key",
          $"transaction_id",
          lit(snapshotDate.toString).cast("date").as("snapshot_date"),
          $"transaction_number",
          $"transaction_type",
          $"transaction_date",
          $"posting_date",
          $"total_amount",
          $"description",
          $"channel",
          $"status",
          $"valid_from",
          $"valid_to",
          current_date().as("load_date"),
          lit("Bronze").as("record_source")
        )

      val rowCount = pitTransactionDF.count()

      val existingCount = spark.sql(
        s"""
           |SELECT COUNT(*) as cnt
           |FROM silver.pit_transaction
           |WHERE snapshot_date = DATE('$snapshotDate')
           |""".stripMargin
      ).collect()(0).getLong(0)

      if (existingCount == 0) {
        IcebergWriter.appendToTable(
          pitTransactionDF,
          "silver",
          "pit_transaction",
          Seq("snapshot_date")
        )
        println(s"✅ Created PIT_Transaction snapshot with $rowCount records")
      } else {
        println(s"⚠️  Snapshot for $snapshotDate already exists")
      }

      LoadMetadata.completeLoad(loadId, rowCount, rowCount)

    } catch {
      case e: Exception =>
        LoadMetadata.failLoad(loadId, e.getMessage)
        throw e
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BUILD ALL BRIDGE TABLES                                         │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def buildBridgeTables()(implicit spark: SparkSession): Unit = {

    println("""
         |┌────────────────────────────────────────────────────────────────┐
         |│ BUILDING BRIDGE TABLES                                        │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    buildBridgeCustomerAccount()
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BUILD BRIDGE_CUSTOMER_ACCOUNT                                   │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * BUSINESS RULES APPLIED:
   * 1. Primary Account: First account opened by customer
   * 2. Customer Name: Derived from first_name/last_name OR company_name
   * 3. Only include ACTIVE customers and accounts
   */
  def buildBridgeCustomerAccount()(implicit spark: SparkSession): Unit = {

    println("\n🌉 Building Bridge_Customer_Account...")

    import spark.implicits._

    val loadId = LoadMetadata.startLoad(
      entityName = "bridge_customer_account",
      recordSource = "Bronze-Link_Customer_Account",
      loadDate = LocalDate.now()
    )

    try {
      // Read link
      val linkDF = spark.table("bronze.link_customer_account")

      // Read hubs
      val hubCustomerDF = spark.table("bronze.hub_customer")
      val hubAccountDF = spark.table("bronze.hub_account")

      // Read current satellites
      val currentCustomerDF = spark.table("bronze.sat_customer")
        .filter($"valid_to".isNull) // Current records only

      val currentAccountDF = spark.table("bronze.sat_account")
        .filter($"valid_to".isNull)

      // Join everything
      val bridgeDF = linkDF
        .join(hubCustomerDF, Seq("customer_hash_key"))
        .join(hubAccountDF, Seq("account_hash_key"))
        .join(currentCustomerDF, Seq("customer_hash_key"))
        .join(currentAccountDF, Seq("account_hash_key"))

      // Apply business rules
      val enrichedBridgeDF = bridgeDF
        .withColumn("customer_name",
          when($"customer_type" === "INDIVIDUAL",
            concat($"first_name", lit(" "), $"last_name"))
          .otherwise($"company_name")
        )
        .withColumn("relationship_start_date", $"opened_date")
        .withColumn("current_balance", $"balance")

      // Determine primary account (first account opened)
      val windowSpec = Window
        .partitionBy("customer_id")
        .orderBy($"opened_date".asc)

      val finalBridgeDF = enrichedBridgeDF
        .withColumn("account_rank", row_number().over(windowSpec))
        .withColumn("is_primary_account", $"account_rank" === 1)
        .select(
          $"link_customer_account_hash_key",
          $"customer_hash_key",
          $"customer_id",
          $"account_hash_key",
          $"account_id",
          $"customer_name",
          currentCustomerDF("customer_status").as("customer_status"),
          currentCustomerDF("customer_type").as("customer_type"),
          $"account_number",
          currentAccountDF("account_type").as("account_type"),
          currentAccountDF("account_status").as("account_status"),
          $"current_balance",
          $"relationship_start_date",
          $"is_primary_account",
          current_date().as("load_date"),
          lit("Bronze").as("record_source")
        )

      val rowCount = finalBridgeDF.count()

      // Overwrite entire bridge (full refresh pattern)
      println("   Using full refresh strategy (overwrite)")

      finalBridgeDF.write
        .format("iceberg")
        .mode("overwrite")
        .save("silver.bridge_customer_account")

      println(s"✅ Created Bridge_Customer_Account with $rowCount relationships")

      LoadMetadata.completeLoad(loadId, rowCount, rowCount)

    } catch {
      case e: Exception =>
        LoadMetadata.failLoad(loadId, e.getMessage)
        throw e
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BATCH GENERATE HISTORICAL PIT SNAPSHOTS                        │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Backfill PIT tables with historical snapshots.
   *
   * USAGE:
   * ```scala
   * // Generate last 30 days of snapshots
   * BusinessVaultETL.generateHistoricalPIT(30)
   * ```
   */
  def generateHistoricalPIT(daysBack: Int)(implicit spark: SparkSession): Unit = {

    println(s"""
         |┌────────────────────────────────────────────────────────────────┐
         |│ GENERATING HISTORICAL PIT SNAPSHOTS ($daysBack days)
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    val startDate = LocalDate.now().minusDays(daysBack.toLong)
    val endDate = LocalDate.now()

    var currentDate = startDate
    var processedDays = 0

    while (currentDate.isBefore(endDate) || currentDate.isEqual(endDate)) {
      println(s"\n📅 Processing date: $currentDate")

      try {
        buildPITCustomer(currentDate)
        buildPITAccount(currentDate)
        buildPITTransaction(currentDate)
        processedDays += 1
      } catch {
        case e: Exception =>
          println(s"❌ Failed to generate PIT for $currentDate: ${e.getMessage}")
      }

      currentDate = currentDate.plusDays(1)
    }

    println(s"""
         |
         |✅ Historical PIT generation complete
         |   Days processed: $processedDays
         |   Date range: $startDate to $endDate
         |""".stripMargin)
  }
}
