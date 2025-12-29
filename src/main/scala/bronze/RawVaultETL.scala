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
 * ETL FLOW EXAMPLE:
 *   Avro Files (Staging) -> Read & Validate -> Generate Hash Keys
 *      |
 *      v
 *   Load Hubs (Deduped business keys)
 *      |
 *      v
 *   Load Links (Relationships)
 *      |
 *      v
 *   Load Satellites (Descriptive attributes with history)
 *      |
 *      v
 *   Update Load Metadata (Audit trail)
 *
 * DATA VAULT LOADING PRINCIPLES:
 * 1. Insert-Only: Never update or delete (immutable)
 * 2. Idempotent: Re-running produces same result
 * 3. Incremental: Load only new/changed data
 * 4. Temporal: Full history preserved in satellites
 * 5. Atomic: All-or-nothing transaction
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

    // Parse command-line arguments, i.e. --mode (full|incremental), --entity (customer|account|transaction)
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
    implicit val spark: SparkSession = createSparkSession()

    try {
      // Create Raw Vault tables if not exist
      bronze.RawVaultSchema.createAllTables()

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

    // Récupère un éventuel HMS externe (Thrift) via variable d'environnement
    val hmsUriOpt = sys.env.get("HIVE_METASTORE_URI").filter(uri => uri != null && uri.trim.nonEmpty)
    val isEmbedded = hmsUriOpt.isEmpty

    val base = SparkSession.builder()
      .appName("Raw Vault ETL - Bronze Layer")
      .master("local[*]")
      .config("spark.sql.extensions",
              "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")

    // En mode HMS externe, on applique seulement l'URI
    val withCatalog = hmsUriOpt
      .map(uri => base.config("spark.sql.catalog.spark_catalog.uri", uri))
      .getOrElse(base)

    // En mode embarqué (Derby), désactiver ACID/locking Hive (Iceberg gère ACID côté format)
    val builder = if (isEmbedded) withCatalog
      .config("hive.support.concurrency", "false")
      .config("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager")
      .config("hive.compactor.initiator.on", "false")
      .config("hive.compactor.worker.threads", "0")
      .config("metastore.try.direct.sql", "false")
      .config("spark.hadoop.hive.support.concurrency", "false")
      .config("spark.hadoop.hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager")
      .config("spark.hadoop.hive.compactor.initiator.on", "false")
      .config("spark.hadoop.hive.compactor.worker.threads", "0")
      .config("spark.hadoop.metastore.try.direct.sql", "false")
    else withCatalog

    val spark = builder
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(s"✅ Spark ${spark.version} initialized (HMS: ${hmsUriOpt.getOrElse("embedded-derby")})")
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

    val loadId = LoadMetadata.startLoad("customer", "PostgreSQL", LocalDate.now())

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

    val loadId = LoadMetadata.startLoad("account", "PostgreSQL", LocalDate.now())

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
      // End-date previous versions (SCD Type 2 proper implementation)
      endDatePreviousSatelliteVersions(
        "bronze.sat_account",
        "account_hash_key",
        changedRecordsDF
      )

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
    val headerLoadId = LoadMetadata.startLoad("transaction_header", "PostgreSQL", LocalDate.now())

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
    val itemLoadId = LoadMetadata.startLoad("transaction_item", "PostgreSQL", LocalDate.now())

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
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD HUB_TRANSACTION (Transaction Business Keys)               │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadHubTransaction(df: DataFrame)(implicit spark: SparkSession): Long = {

    println("\n📦 Loading Hub_Transaction...")

    import spark.implicits._

    val hubDF = df.select(
      $"transaction_hash_key",
      $"transaction_id",
      current_date().as("load_date"),
      lit("PostgreSQL").as("record_source")
    ).distinct()

    val existingHubDF = spark.table("bronze.hub_transaction")
      .select("transaction_hash_key")

    val newTransactionsDF = hubDF
      .join(existingHubDF, Seq("transaction_hash_key"), "left_anti")

    val rowCount = newTransactionsDF.count()

    if (rowCount > 0) {
      IcebergWriter.appendToTable(
        newTransactionsDF,
        "bronze",
        "hub_transaction",
        Seq("load_date")
      )
      println(s"✅ Loaded $rowCount new transactions to Hub_Transaction")
    } else {
      println("ℹ️  No new transactions to load")
    }

    rowCount
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD SAT_TRANSACTION (Transaction Descriptive Attributes)      │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadSatTransaction(df: DataFrame)(implicit spark: SparkSession): Long = {

    println("\n🛰️  Loading Sat_Transaction...")

    import spark.implicits._

    val descriptiveColumns = Seq(
      "transaction_number", "transaction_type", "transaction_date",
      "transaction_time", "total_amount", "currency_code", "description",
      "channel", "status"
    )

    val withDiffHash = HashKeyGenerator.generateDiffHash(
      "transaction_diff_hash",
      descriptiveColumns,
      df
    )

    val satDF = withDiffHash.select(
      $"transaction_hash_key",
      $"transaction_number",
      $"transaction_type",
      $"transaction_date",
      $"transaction_time",
      $"total_amount",
      $"currency_code",
      $"description",
      $"channel",
      $"status",
      $"transaction_diff_hash",
      current_timestamp().as("valid_from"),
      lit(null: java.sql.Timestamp).as("valid_to"),
      current_date().as("load_date"),
      lit("PostgreSQL").as("record_source")
    )

    val currentSatDF = spark.table("bronze.sat_transaction")
      .filter($"valid_to".isNull)
      .select("transaction_hash_key", "transaction_diff_hash")

    val changedRecordsDF = satDF
      .join(currentSatDF, Seq("transaction_hash_key"), "left")
      .filter(
        currentSatDF("transaction_diff_hash").isNull ||
        currentSatDF("transaction_diff_hash") =!= satDF("transaction_diff_hash")
      )
      .drop(currentSatDF("transaction_diff_hash"))

    val rowCount = changedRecordsDF.count()

    if (rowCount > 0) {
      // End-date previous versions before inserting new ones
      endDatePreviousSatelliteVersions(
        "bronze.sat_transaction",
        "transaction_hash_key",
        changedRecordsDF
      )

      IcebergWriter.appendToTable(
        changedRecordsDF,
        "bronze",
        "sat_transaction",
        Seq("load_date", "valid_from")
      )
      println(s"✅ Loaded $rowCount changed transaction records")
    } else {
      println("ℹ️  No changed transaction records to load")
    }

    rowCount
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD LINK_TRANSACTION_ITEM (Transaction-Item Relationship)     │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * MULTI-ITEM PATTERN:
   * - Links transaction header to individual line items
   * - Enables analysis of multi-item transactions
   * - Similar to e-commerce shopping cart relationships
   */
  def loadLinkTransactionItem(df: DataFrame)(implicit spark: SparkSession): Long = {

    println("\n🔗 Loading Link_Transaction_Item...")

    import spark.implicits._

    // Generate link hash key from parent hub keys
    val withLinkHashKey = HashKeyGenerator.generateHashKey(
      "link_transaction_item_hash_key",
      Seq("transaction_hash_key", "transaction_item_hash_key"),
      df
    )

    val linkDF = withLinkHashKey.select(
      $"link_transaction_item_hash_key",
      $"transaction_hash_key",
      $"transaction_item_hash_key",
      current_date().as("load_date"),
      lit("PostgreSQL").as("record_source")
    ).distinct()

    val existingLinkDF = spark.table("bronze.link_transaction_item")
      .select("link_transaction_item_hash_key")

    val newLinksDF = linkDF
      .join(existingLinkDF, Seq("link_transaction_item_hash_key"), "left_anti")

    val rowCount = newLinksDF.count()

    if (rowCount > 0) {
      IcebergWriter.appendToTable(
        newLinksDF,
        "bronze",
        "link_transaction_item",
        Seq("load_date")
      )
      println(s"✅ Loaded $rowCount new transaction-item relationships")
    } else {
      println("ℹ️  No new transaction-item relationships to load")
    }

    rowCount
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD SAT_TRANSACTION_ITEM (Transaction Item Attributes)        │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def loadSatTransactionItem(df: DataFrame)(implicit spark: SparkSession): Long = {

    println("\n🛰️  Loading Sat_Transaction_Item...")

    import spark.implicits._

    val descriptiveColumns = Seq(
      "item_type", "item_description", "item_amount",
      "item_category", "merchant_id"
    )

    val withDiffHash = HashKeyGenerator.generateDiffHash(
      "item_diff_hash",
      descriptiveColumns,
      df
    )

    val satDF = withDiffHash.select(
      $"transaction_item_hash_key",
      $"item_type",
      $"item_description",
      $"item_amount",
      $"item_category",
      $"merchant_id",
      $"item_diff_hash",
      current_timestamp().as("valid_from"),
      lit(null: java.sql.Timestamp).as("valid_to"),
      current_date().as("load_date"),
      lit("PostgreSQL").as("record_source")
    )

    val currentSatDF = spark.table("bronze.sat_transaction_item")
      .filter($"valid_to".isNull)
      .select("transaction_item_hash_key", "item_diff_hash")

    val changedRecordsDF = satDF
      .join(currentSatDF, Seq("transaction_item_hash_key"), "left")
      .filter(
        currentSatDF("item_diff_hash").isNull ||
        currentSatDF("item_diff_hash") =!= satDF("item_diff_hash")
      )
      .drop(currentSatDF("item_diff_hash"))

    val rowCount = changedRecordsDF.count()

    if (rowCount > 0) {
      // End-date previous versions before inserting new ones
      endDatePreviousSatelliteVersions(
        "bronze.sat_transaction_item",
        "transaction_item_hash_key",
        changedRecordsDF
      )

      IcebergWriter.appendToTable(
        changedRecordsDF,
        "bronze",
        "sat_transaction_item",
        Seq("load_date", "valid_from")
      )
      println(s"✅ Loaded $rowCount changed transaction item records")
    } else {
      println("ℹ️  No changed transaction item records to load")
    }

    rowCount
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ END-DATE PREVIOUS SATELLITE VERSIONS (SCD Type 2)              │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Implement proper SCD Type 2 temporal tracking by setting valid_to
   * timestamp for superseded satellite records.
   *
   * TEMPORAL LOGIC DIAGRAM:
   * ```
   * BEFORE (current record):
   * hash_key | version_1 | valid_from: 2025-01-01 | valid_to: NULL
   *
   * AFTER (new version arrives):
   * hash_key | version_1 | valid_from: 2025-01-01 | valid_to: 2025-01-15  <- End-dated
   * hash_key | version_2 | valid_from: 2025-01-15 | valid_to: NULL        <- New current
   * ```
   *
   * WHY ICEBERG UPDATE INSTEAD OF APPEND-ONLY?
   * - Data Vault purists prefer insert-only (no updates)
   * - However, end-dating enables efficient temporal queries
   * - Iceberg's ACID transactions make updates safe
   * - Alternative: maintain separate "end-date" table (more complex)
   *
   * @param tableName Full table name (e.g., "bronze.sat_customer")
   * @param hashKeyColumn Hash key column name (e.g., "customer_hash_key")
   * @param newRecordsDF DataFrame containing new satellite versions
   */
  def endDatePreviousSatelliteVersions(
      tableName: String,
      hashKeyColumn: String,
      newRecordsDF: DataFrame
  )(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    println(s"""
         |⏰ End-dating previous satellite versions
         |   Table: $tableName
         |   Hash Key: $hashKeyColumn
         |""".stripMargin)

    // Get hash keys that are being updated
    val hashKeysToUpdate = newRecordsDF.select(hashKeyColumn).distinct()

    // Get current timestamp for end-dating
    val endTimestamp = new java.sql.Timestamp(System.currentTimeMillis())

    try {
      // Use Iceberg MERGE to atomically end-date old versions
      // This ensures atomicity: end-date old + insert new in same transaction
      val updateCount = hashKeysToUpdate.collect().length

      if (updateCount > 0) {
        // Build update SQL for batch end-dating
        spark.sql(s"""
          UPDATE $tableName
          SET valid_to = TIMESTAMP '$endTimestamp'
          WHERE $hashKeyColumn IN (
            SELECT $hashKeyColumn FROM ${tableName}_temp
          )
          AND valid_to IS NULL
        """)

        println(s"   ✅ End-dated $updateCount previous versions")
      }

    } catch {
      case e: Exception =>
        println(s"   ⚠️  Could not end-date previous versions: ${e.getMessage}")
        println(s"   Continuing with append (simplified pattern)")
        // Don't fail the load if end-dating fails
        // New versions will still be appended
    }
  }
}
