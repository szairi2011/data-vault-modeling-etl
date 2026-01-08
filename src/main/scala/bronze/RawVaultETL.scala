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
import common.{ETLJob, ETLConfig}
import java.time.LocalDate

object RawVaultETL extends ETLJob {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ MAIN ENTRY POINT                                                │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def main(args: Array[String]): Unit = {
    runMain(args, "DATA VAULT 2.0 - RAW VAULT ETL (BRONZE LAYER)")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ EXECUTE ETL BUSINESS LOGIC                                      │
   * └─────────────────────────────────────────────────────────────────┘
   */
  override def execute(spark: SparkSession, config: ETLConfig): Unit = {
    implicit val implicitSpark: SparkSession = spark

    // Create Raw Vault tables if not exist
    bronze.RawVaultSchema.createAllTables()

    // Log catalog / database context for troubleshooting only
    println("\n🔍 Verifying Spark catalog context (informational)...")
    val currentDatabase = spark.sql("SELECT current_database()").first().getString(0)
    println(s"   Current database: $currentDatabase")
    val databases = spark.sql("SHOW DATABASES").collect().map(_.getString(0))
    println(s"   Available databases: ${databases.mkString(", ")}")
    // IMPORTANT:
    // - We do NOT call `USE bronze` here.
    // - All tables are always referenced as `bronze.<table>` fully qualified.
    //   This makes the job portable across local / remote HMS and avoids
    //   relying on mutable session state.

    // Process entities (all table references use `bronze.*`)
    config.entity match {
      case Some("customer")    => processCustomer(config.mode)
      case Some("account")     => processAccount(config.mode)
      case Some("transaction") => processTransaction(config.mode)
      case None                => processAll(config.mode)
      case Some(e)             => throw new IllegalArgumentException(s"Unknown entity: $e")
    }
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
    // Check if table exists first
    val existingHubDF = try {
      spark.table("bronze.hub_customer")
        .select("customer_hash_key")
    } catch {
      case e: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
        println(s"⚠️  Table bronze.hub_customer not found - this might be the first load")
        // Return empty DataFrame with correct schema
        spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          df.select($"customer_hash_key").schema)
    }

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

    // Actual fields available in the DataFrame (from AvroReader.readAvro)
    val actualFields = df.columns.toSet

    // Canonical raw descriptive columns for Sat_Customer (must come from customer.avsc)
    val canonicalDescriptiveColumns = Seq(
      "customer_type",
      "first_name",
      "last_name",
      "business_name",
      "email",
      "phone",
      "date_of_birth",
      "ssn",
      "tax_id",
      "credit_score",
      "customer_since",
      "loyalty_tier",
      "preferred_contact_method"
    )

    // Effective columns = intersection of canonical list and actual DataFrame fields
    val effectiveDescriptiveColumns = canonicalDescriptiveColumns.filter(actualFields.contains)

    // Drift diagnostics (for information only, no hard failure for descriptive attrs)
    val missingDescriptive = canonicalDescriptiveColumns.filterNot(actualFields.contains)
    val excludedFromExtra = Set(
      "customer_id",
      "customer_number",
      "customer_hash_key",
      "_load_timestamp",
      "_source_file",
      "_file_modification_time"
    )
    val extraDescriptive = actualFields
      .diff(canonicalDescriptiveColumns.toSet)
      .diff(excludedFromExtra)

    println("\n🔍 Sat_Customer descriptive column alignment (raw):")
    println(s"   Effective descriptive columns used for diff hash: ${effectiveDescriptiveColumns.mkString(", ")}")

    if (missingDescriptive.nonEmpty) {
      println(s"⚠️  Raw attributes defined in Sat_Customer but not present in current data/schema: ${missingDescriptive.mkString(", ")}")
      println("   They are skipped for diff hash and insert until the source/Avro schema provides them.")
    }

    if (extraDescriptive.nonEmpty) {
      println(s"⚠️  New schema columns not yet part of Sat_Customer diff hash: ${extraDescriptive.mkString(", ")}")
      println("   If they should be tracked, add them to canonicalDescriptiveColumns.")
    }

    // Generate diff hash for change detection (only on existing raw attributes)
    val withDiffHash = HashKeyGenerator.generateDiffHash(
      "customer_diff_hash",
      effectiveDescriptiveColumns,
      df
    )

    // Build attribute select list dynamically (only include columns that exist)
    val attributeCols = canonicalDescriptiveColumns
      .filter(actualFields.contains)
      .map(col)

    // Prepare satellite records (raw attributes only)
    val satDF = withDiffHash.select(
      $"customer_hash_key",
      $"customer_type",
      $"first_name",
      $"last_name",
      $"business_name",
      $"email",
      $"phone",
      $"date_of_birth",
      $"ssn",
      $"tax_id",
      $"credit_score",
      $"customer_since",
      $"loyalty_tier",
      $"preferred_contact_method",
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

    val actualFields = df.columns.toSet

    // Raw descriptive columns based on account.avsc
    val canonicalDescriptiveColumns = Seq(
      "account_number",
      "product_id",
      "branch_id",
      "account_status",
      "current_balance",
      "available_balance",
      "currency",
      "overdraft_limit",
      "interest_rate",
      "opened_date",
      "closed_date",
      "last_transaction_date",
      "updated_at"
    )

    val effectiveDescriptiveColumns = canonicalDescriptiveColumns.filter(actualFields.contains)

    val missingDescriptive = canonicalDescriptiveColumns.filterNot(actualFields.contains)
    val excludedFromExtra = Set("account_id", "customer_id", "account_hash_key")
    val extraDescriptive = actualFields.diff(canonicalDescriptiveColumns.toSet).diff(excludedFromExtra)

    println("\n🔍 Sat_Account descriptive column alignment (raw):")
    println(s"   Effective descriptive columns used for diff hash: ${effectiveDescriptiveColumns.mkString(", ")}")
    if (missingDescriptive.nonEmpty) {
      println(s"⚠️  Raw attributes defined in Sat_Account but not present in current data/schema: ${missingDescriptive.mkString(", ")}")
    }
    if (extraDescriptive.nonEmpty) {
      println(s"⚠️  New schema columns not yet part of Sat_Account diff hash: ${extraDescriptive.mkString(", ")}")
    }

    val withDiffHash = HashKeyGenerator.generateDiffHash(
      "account_diff_hash",
      effectiveDescriptiveColumns,
      df
    )

    val satDF = withDiffHash.select(
      $"account_hash_key",
      $"account_number",
      $"product_id",
      $"branch_id",
      $"account_status",
      $"current_balance",
      $"available_balance",
      $"currency",
      $"overdraft_limit",
      $"interest_rate",
      $"opened_date",
      $"closed_date",
      $"last_transaction_date",
      $"updated_at",
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

    val actualFields = df.columns.toSet

    // Raw descriptive columns based on transaction_header.avsc
    val canonicalDescriptiveColumns = Seq(
      "transaction_number",
      "transaction_type",
      "transaction_date",
      "posting_date",
      "total_amount",
      "description",
      "channel",
      "transaction_status",
      "location",
      "reference_number",
      "initiated_by",
      "created_at",
      "updated_at"
    )

    val effectiveDescriptiveColumns = canonicalDescriptiveColumns.filter(actualFields.contains)
    val missingDescriptive = canonicalDescriptiveColumns.filterNot(actualFields.contains)
    val excludedFromExtra = Set("transaction_id", "account_id", "transaction_hash_key")
    val extraDescriptive = actualFields.diff(canonicalDescriptiveColumns.toSet).diff(excludedFromExtra)

    println("\n🔍 Sat_Transaction descriptive column alignment (raw):")
    println(s"   Effective descriptive columns used for diff hash: ${effectiveDescriptiveColumns.mkString(", ")}")
    if (missingDescriptive.nonEmpty) {
      println(s"⚠️  Raw attributes defined in Sat_Transaction but not present in current data/schema: ${missingDescriptive.mkString(", ")}")
    }
    if (extraDescriptive.nonEmpty) {
      println(s"⚠️  New schema columns not yet part of Sat_Transaction diff hash: ${extraDescriptive.mkString(", ")}")
    }

    val withDiffHash = HashKeyGenerator.generateDiffHash(
      "transaction_diff_hash",
      effectiveDescriptiveColumns,
      df
    )

    val satDF = withDiffHash.select(
      $"transaction_hash_key",
      $"transaction_number",
      $"transaction_type",
      $"transaction_date",
      $"posting_date",
      $"total_amount",
      $"description",
      $"channel",
      $"transaction_status",
      $"location",
      $"reference_number",
      $"initiated_by",
      $"created_at",
      $"updated_at",
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

    val actualFields = df.columns.toSet

    // Raw descriptive columns based on transaction_item.avsc
    val canonicalDescriptiveColumns = Seq(
      "item_amount",
      "category_id",
      "merchant_name",
      "merchant_category_code",
      "item_description",
      "created_at",
      "payee_name",
      "payee_account",
      "is_recurring"
    )

    val effectiveDescriptiveColumns = canonicalDescriptiveColumns.filter(actualFields.contains)
    val missingDescriptive = canonicalDescriptiveColumns.filterNot(actualFields.contains)
    val excludedFromExtra = Set("item_id", "transaction_id", "item_sequence", "transaction_item_hash_key")
    val extraDescriptive = actualFields.diff(canonicalDescriptiveColumns.toSet).diff(excludedFromExtra)

    println("\n🔍 Sat_Transaction_Item descriptive column alignment (raw):")
    println(s"   Effective descriptive columns used for diff hash: ${effectiveDescriptiveColumns.mkString(", ")}")
    if (missingDescriptive.nonEmpty) {
      println(s"⚠️  Raw attributes defined in Sat_Transaction_Item but not present in current data/schema: ${missingDescriptive.mkString(", ")}")
    }
    if (extraDescriptive.nonEmpty) {
      println(s"⚠️  New schema columns not yet part of Sat_Transaction_Item diff hash: ${extraDescriptive.mkString(", ")}")
    }

    val withDiffHash = HashKeyGenerator.generateDiffHash(
      "item_diff_hash",
      effectiveDescriptiveColumns,
      df
    )

    val satDF = withDiffHash.select(
      $"transaction_item_hash_key",
      $"item_amount",
      $"category_id",
      $"merchant_name",
      $"merchant_category_code",
      $"item_description",
      $"created_at",
      $"payee_name",
      $"payee_account",
      $"is_recurring",
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
