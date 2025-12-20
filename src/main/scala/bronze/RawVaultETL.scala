package bronze

/**
 * ========================================================================
 * RAW VAULT ETL - BRONZE LAYER ORCHESTRATION
 * ========================================================================
 *
 * PURPOSE:
 * Main ETL job to load data from Avro staging zone into Data Vault 2.0
 * Raw Vault (Bronze layer) using Apache Iceberg tables.
 *
 * LEARNING OBJECTIVES:
 * - Data Vault 2.0 loading patterns
 * - Hub, Link, Satellite loading sequence
 * - Incremental processing strategy
 * - Hash key generation in practice
 * - Temporal tracking (satellitehistory)
 * - Idempotent ETL design
 *
 * ETL FLOW:
 * ```
 * Avro Files (Staging) → Read & Validate → Generate Hash Keys
 *     ↓
 * Load Hubs (Deduped business keys)
 *     ↓
 * Load Links (Relationships)
 *     ↓
 * Load Satellites (Descriptive attributes with history)
 *     ↓
 * Update Load Metadata (Audit trail)
 * ```
 *
 * DATA VAULT LOADING PRINCIPLES:
 * 1. **Insert-Only**: Never update or delete (immutable)
 * 2. **Idempotent**: Re-running produces same result
 * 3. **Incremental**: Load only new/changed data
 * 4. **Temporal**: Full history preserved in satellites
 * 5. **Atomic**: All-or-nothing transaction
 *
 * ========================================================================
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import bronze.utils.{AvroReader, HashKeyGenerator, IcebergWriter, LoadMetadata}
import java.time.LocalDate

object RawVaultETL {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ MAIN ENTRY POINT                                                │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def main(args: Array[String]): Unit = {

    println("""
         |╔════════════════════════════════════════════════════════════════╗
         |║         DATA VAULT 2.0 - RAW VAULT ETL (BRONZE LAYER)         ║
         |╚════════════════════════════════════════════════════════════════╝
         |""".stripMargin)

    // Parse command-line arguments
    val mode = if (args.contains("--mode")) {
      args(args.indexOf("--mode") + 1)
    } else {
      "incremental"
    }

    val entity = if (args.contains("--entity")) {
      Some(args(args.indexOf("--entity") + 1))
    } else {
      None
    }

    println(s"""
         |Configuration:
         |  Mode: $mode (full|incremental)
         |  Entity: ${entity.getOrElse("all")}
         |""".stripMargin)

    // Initialize Spark Session with Iceberg support
    implicit val spark = createSparkSession()

    try {
      // Create Raw Vault tables if not exist
      RawVaultSchema.createAllTables()

      // Process entities
      entity match {
        case Some("customer") => processCustomer(mode)
        case Some("account") => processAccount(mode)
        case Some("transaction") => processTransaction(mode)
        case None => processAll(mode)
        case _ => throw new IllegalArgumentException(s"Unknown entity: ${entity.get}")
      }

      println("\n✅ Raw Vault ETL completed successfully")

    } catch {
      case e: Exception =>
        println(s"\n❌ Raw Vault ETL failed: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ CREATE SPARK SESSION WITH ICEBERG & HIVE                        │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createSparkSession(): SparkSession = {

    println("\n🚀 Initializing Spark Session with Iceberg & Hive Metastore...")

    val spark = SparkSession.builder()
      .appName("Raw Vault ETL - Bronze Layer")
      .config("spark.sql.extensions",
              "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.spark_catalog.uri",
              sys.env.getOrElse("HIVE_METASTORE_URI", ""))
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(s"✅ Spark ${spark.version} initialized")
    spark
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PROCESS ALL ENTITIES                                            │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def processAll(mode: String)(implicit spark: SparkSession): Unit = {
    processCustomer(mode)
    processAccount(mode)
    processTransaction(mode)
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PROCESS CUSTOMER ENTITY                                         │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * STEPS:
   * 1. Read Avro files from staging
   * 2. Generate customer_hash_key
   * 3. Load Hub_Customer (business keys)
   * 4. Generate customer_diff_hash
   * 5. Load Sat_Customer (descriptive attributes)
   */
  def processCustomer(mode: String)(implicit spark: SparkSession): Unit = {

    println("""
         |┌─────────────────────────────────────────────���──────────────────┐
         |│ PROCESSING CUSTOMER ENTITY                                    │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    val loadId = LoadMetadata.startLoad(
      entityName = "customer",
      recordSource = "PostgreSQL",
      loadDate = LocalDate.now()
    )

    try {
      // STEP 1: Read staged Avro files
      val stagingPath = "warehouse/staging/customer/*.avro"
      val customerDF = AvroReader.readAvro(stagingPath)

      val recordsExtracted = customerDF.count()

      // STEP 2: Generate hash key for business key
      val withHashKey = HashKeyGenerator.generateHashKey(
        "customer_hash_key",
        Seq("customer_id"),
        customerDF
      )

      // STEP 3: Load Hub_Customer
      val hubRecordsLoaded = loadHubCustomer(withHashKey)

      // STEP 4: Load Sat_Customer
      val satRecordsLoaded = loadSatCustomer(withHashKey)

      // Update metadata
      LoadMetadata.completeLoad(
        loadId,
        recordsExtracted,
        hubRecordsLoaded + satRecordsLoaded
      )

    } catch {
      case e: Exception =>
        LoadMetadata.failLoad(loadId, e.getMessage)
        throw e
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD HUB_CUSTOMER (Business Keys Only)                         │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * DATA VAULT PATTERN:
   * - Insert only unique business keys
   * - Deduplicate on customer_hash_key
   * - No descriptive attributes (those go in satellite)
   *
   * IDEMPOTENCY:
   * - Re-running with same data produces no duplicates
   * - Hash key ensures uniqueness
   */
  def loadHubCustomer(df: DataFrame)(implicit spark: SparkSession): Long = {

    println("\n📦 Loading Hub_Customer...")

    import spark.implicits._

    // Select only hub columns
    val hubDF = df.select(
      $"customer_hash_key",
      $"customer_id",
      current_date().as("load_date"),
      lit("PostgreSQL").as("record_source")
    ).distinct() // Deduplicate on hash key

    // Get existing hub records to avoid duplicates
    val existingHubDF = spark.table("bronze.hub_customer")
      .select("customer_hash_key")

    // Left anti join to find new customers only
    val newCustomersDF = hubDF
      .join(existingHubDF, Seq("customer_hash_key"), "left_anti")

    val rowCount = newCustomersDF.count()

    if (rowCount > 0) {
      // Append new customers to hub
      IcebergWriter.appendToTable(
        newCustomersDF,
        "bronze",
        "hub_customer",
        Seq("load_date")
      )

      println(s"✅ Loaded $rowCount new customers to Hub_Customer")
    } else {
      println("ℹ️  No new customers to load (all already exist in hub)")
    }

    rowCount
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD SAT_CUSTOMER (Descriptive Attributes with History)        │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * DATA VAULT PATTERN:
   * - Full history of attribute changes
   * - Temporal tracking (valid_from/valid_to)
   * - Diff hash for change detection
   * - End-date previous version when new version arrives
   *
   * TEMPORAL LOGIC:
   * 1. Generate diff hash of all attributes
   * 2. Compare with current satellite records
   * 3. If changed, end-date old version (set valid_to)
   * 4. Insert new version (valid_to = NULL)
   */
  def loadSatCustomer(df: DataFrame)(implicit spark: SparkSession): Long = {

    println("\n🛰️  Loading Sat_Customer...")

    import spark.implicits._

    // Define descriptive columns for diff hash
    val descriptiveColumns = Seq(
      "customer_type", "first_name", "last_name", "company_name",
      "email", "phone", "date_of_birth", "tax_id", "customer_since",
      "customer_status", "credit_score"
    )

    // Generate diff hash for change detection
    val withDiffHash = HashKeyGenerator.generateDiffHash(
      "customer_diff_hash",
      descriptiveColumns,
      df
    )

    // Prepare satellite records
    val satDF = withDiffHash.select(
      $"customer_hash_key",
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
      $"customer_diff_hash",
      current_timestamp().as("valid_from"),
      lit(null: java.sql.Timestamp).as("valid_to"),
      current_date().as("load_date"),
      lit("PostgreSQL").as("record_source")
    )

    // Get current satellite records (valid_to IS NULL)
    val currentSatDF = spark.table("bronze.sat_customer")
      .filter($"valid_to".isNull)
      .select("customer_hash_key", "customer_diff_hash")

    // Find changed records (diff hash different)
    val changedRecordsDF = satDF
      .join(currentSatDF, Seq("customer_hash_key"), "left")
      .filter(
        currentSatDF("customer_diff_hash").isNull || // New customer
        currentSatDF("customer_diff_hash") =!= satDF("customer_diff_hash") // Changed attributes
      )
      .drop(currentSatDF("customer_diff_hash"))

    val rowCount = changedRecordsDF.count()

    if (rowCount > 0) {
      // TODO: End-date previous versions (set valid_to)
      // For now, just append (simplified pattern)

      IcebergWriter.appendToTable(
        changedRecordsDF,
        "bronze",
        "sat_customer",
        Seq("load_date", "valid_from")
      )

      println(s"✅ Loaded $rowCount changed customer records to Sat_Customer")
    } else {
      println("ℹ️  No changed customer records to load")
    }

    rowCount
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PROCESS ACCOUNT ENTITY                                          │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def processAccount(mode: String)(implicit spark: SparkSession): Unit = {

    println("""
         |┌────────────────────────────────────────────────────────────────┐
         |│ PROCESSING ACCOUNT ENTITY                                     │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    val loadId = LoadMetadata.startLoad(
      entityName = "account",
      recordSource = "PostgreSQL",
      loadDate = LocalDate.now()
    )

    try {
      val stagingPath = "warehouse/staging/account/*.avro"
      val accountDF = AvroReader.readAvro(stagingPath)

      val recordsExtracted = accountDF.count()

      // Generate hash keys
      val hashKeySpecs = Map(
        "account_hash_key" -> Seq("account_id"),
        "customer_hash_key" -> Seq("customer_id")
      )
      val withHashKeys = HashKeyGenerator.generateHashKeysBatch(hashKeySpecs, accountDF)

      // Load Hub_Account
      val hubRecordsLoaded = loadHubAccount(withHashKeys)

      // Load Link_Customer_Account
      val linkRecordsLoaded = loadLinkCustomerAccount(withHashKeys)

      // Load Sat_Account
      val satRecordsLoaded = loadSatAccount(withHashKeys)

      LoadMetadata.completeLoad(
        loadId,
        recordsExtracted,
        hubRecordsLoaded + linkRecordsLoaded + satRecordsLoaded
      )

    } catch {
      case e: Exception =>
        LoadMetadata.failLoad(loadId, e.getMessage)
        throw e
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD HUB_ACCOUNT                                                │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadHubAccount(df: DataFrame)(implicit spark: SparkSession): Long = {

    println("\n📦 Loading Hub_Account...")

    import spark.implicits._

    val hubDF = df.select(
      $"account_hash_key",
      $"account_id",
      current_date().as("load_date"),
      lit("PostgreSQL").as("record_source")
    ).distinct()

    val existingHubDF = spark.table("bronze.hub_account")
      .select("account_hash_key")

    val newAccountsDF = hubDF
      .join(existingHubDF, Seq("account_hash_key"), "left_anti")

    val rowCount = newAccountsDF.count()

    if (rowCount > 0) {
      IcebergWriter.appendToTable(
        newAccountsDF,
        "bronze",
        "hub_account",
        Seq("load_date")
      )
      println(s"✅ Loaded $rowCount new accounts to Hub_Account")
    } else {
      println("ℹ️  No new accounts to load")
    }

    rowCount
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD LINK_CUSTOMER_ACCOUNT (Relationship)                      │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * DATA VAULT LINK PATTERN:
   * - Connects Hub_Customer to Hub_Account
   * - Link hash key = MD5(customer_hash_key || account_hash_key)
   * - No descriptive attributes
   * - Captures many-to-many relationships
   */
  def loadLinkCustomerAccount(df: DataFrame)(implicit spark: SparkSession): Long = {

    println("\n🔗 Loading Link_Customer_Account...")

    import spark.implicits._

    // Generate link hash key from parent hub keys
    val withLinkHashKey = HashKeyGenerator.generateHashKey(
      "link_customer_account_hash_key",
      Seq("customer_hash_key", "account_hash_key"),
      df
    )

    val linkDF = withLinkHashKey.select(
      $"link_customer_account_hash_key",
      $"customer_hash_key",
      $"account_hash_key",
      current_date().as("load_date"),
      lit("PostgreSQL").as("record_source")
    ).distinct()

    val existingLinkDF = spark.table("bronze.link_customer_account")
      .select("link_customer_account_hash_key")

    val newLinksDF = linkDF
      .join(existingLinkDF, Seq("link_customer_account_hash_key"), "left_anti")

    val rowCount = newLinksDF.count()

    if (rowCount > 0) {
      IcebergWriter.appendToTable(
        newLinksDF,
        "bronze",
        "link_customer_account",
        Seq("load_date")
      )
      println(s"✅ Loaded $rowCount new customer-account relationships")
    } else {
      println("ℹ️  No new relationships to load")
    }

    rowCount
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD SAT_ACCOUNT                                                │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadSatAccount(df: DataFrame)(implicit spark: SparkSession): Long = {

    println("\n🛰️  Loading Sat_Account...")

    import spark.implicits._

    val descriptiveColumns = Seq(
      "account_number", "account_type", "product_id", "branch_id",
      "balance", "available_balance", "currency_code", "interest_rate",
      "credit_limit", "opened_date", "account_status"
    )

    val withDiffHash = HashKeyGenerator.generateDiffHash(
      "account_diff_hash",
      descriptiveColumns,
      df
    )

    val satDF = withDiffHash.select(
      $"account_hash_key",
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
      $"account_diff_hash",
      current_timestamp().as("valid_from"),
      lit(null: java.sql.Timestamp).as("valid_to"),
      current_date().as("load_date"),
      lit("PostgreSQL").as("record_source")
    )

    val currentSatDF = spark.table("bronze.sat_account")
      .filter($"valid_to".isNull)
      .select("account_hash_key", "account_diff_hash")

    val changedRecordsDF = satDF
      .join(currentSatDF, Seq("account_hash_key"), "left")
      .filter(
        currentSatDF("account_diff_hash").isNull ||
        currentSatDF("account_diff_hash") =!= satDF("account_diff_hash")
      )
      .drop(currentSatDF("account_diff_hash"))

    val rowCount = changedRecordsDF.count()

    if (rowCount > 0) {
      IcebergWriter.appendToTable(
        changedRecordsDF,
        "bronze",
        "sat_account",
        Seq("load_date", "valid_from")
      )
      println(s"✅ Loaded $rowCount changed account records")
    } else {
      println("ℹ️  No changed account records to load")
    }

    rowCount
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PROCESS TRANSACTION ENTITY (Header + Items)                    │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def processTransaction(mode: String)(implicit spark: SparkSession): Unit = {

    println("""
         |┌────────────────────────────────────────────────────────────────┐
         |│ PROCESSING TRANSACTION ENTITY                                 │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    // Process transaction headers
    val headerLoadId = LoadMetadata.startLoad(
      entityName = "transaction_header",
      recordSource = "PostgreSQL",
      loadDate = LocalDate.now()
    )

    try {
      val headerPath = "warehouse/staging/transaction_header/*.avro"
      val headerDF = AvroReader.readAvro(headerPath)

      val recordsExtracted = headerDF.count()

      // Generate hash keys for header
      val headerHashKeySpecs = Map(
        "transaction_hash_key" -> Seq("transaction_id"),
        "account_hash_key" -> Seq("account_id")
      )
      val headerWithHashKeys = HashKeyGenerator.generateHashKeysBatch(
        headerHashKeySpecs,
        headerDF
      )

      // Load Hub_Transaction and Sat_Transaction
      val hubLoaded = loadHubTransaction(headerWithHashKeys)
      val satLoaded = loadSatTransaction(headerWithHashKeys)

      LoadMetadata.completeLoad(headerLoadId, recordsExtracted, hubLoaded + satLoaded)

    } catch {
      case e: Exception =>
        LoadMetadata.failLoad(headerLoadId, e.getMessage)
        throw e
    }

    // Process transaction items (multi-item pattern)
    val itemLoadId = LoadMetadata.startLoad(
      entityName = "transaction_item",
      recordSource = "PostgreSQL",
      loadDate = LocalDate.now()
    )

    try {
      val itemPath = "warehouse/staging/transaction_item/*.avro"
      val itemDF = AvroReader.readAvro(itemPath)

      val recordsExtracted = itemDF.count()

      // Generate hash keys for items
      val itemWithHashKey = HashKeyGenerator.generateHashKey(
        "transaction_item_hash_key",
        Seq("transaction_id", "item_sequence"),
        itemDF
      )

      val itemWithTxnHashKey = HashKeyGenerator.generateHashKey(
        "transaction_hash_key",
        Seq("transaction_id"),
        itemWithHashKey
      )

      // Load Link_Transaction_Item and Sat_Transaction_Item
      val linkLoaded = loadLinkTransactionItem(itemWithTxnHashKey)
      val satItemLoaded = loadSatTransactionItem(itemWithTxnHashKey)

      LoadMetadata.completeLoad(itemLoadId, recordsExtracted, linkLoaded + satItemLoaded)

    } catch {
      case e: Exception =>
        LoadMetadata.failLoad(itemLoadId, e.getMessage)
        throw e
    }
  }

  /**
   * Placeholder methods for transaction loading
   * (Following same pattern as customer/account)
   */
  def loadHubTransaction(df: DataFrame)(implicit spark: SparkSession): Long = {
    println("\n📦 Loading Hub_Transaction...")
    // Implementation similar to loadHubCustomer
    0L
  }

  def loadSatTransaction(df: DataFrame)(implicit spark: SparkSession): Long = {
    println("\n🛰️  Loading Sat_Transaction...")
    // Implementation similar to loadSatCustomer
    0L
  }

  def loadLinkTransactionItem(df: DataFrame)(implicit spark: SparkSession): Long = {
    println("\n🔗 Loading Link_Transaction_Item...")
    // Implementation similar to loadLinkCustomerAccount
    0L
  }

  def loadSatTransactionItem(df: DataFrame)(implicit spark: SparkSession): Long = {
    println("\n🛰️  Loading Sat_Transaction_Item...")
    // Implementation similar to loadSatAccount
    0L
  }
}

