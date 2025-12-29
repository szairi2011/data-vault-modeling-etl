package bronze

/**
 * ========================================================================
 * RAW VAULT SCHEMA - DATA VAULT 2.0 TABLE DEFINITIONS
 * ========================================================================
 *
 * PURPOSE:
 * Defines and creates all Data Vault 2.0 tables in the Bronze layer
 * (Raw Vault) using Apache Iceberg table format.
 *
 * DATA VAULT 2.0 ENTITIES:
 * - Hubs: Business entities (Customer, Account, Transaction, Transaction Item)
 * - Links: Relationships between hubs (Customer-Account, Transaction-Item)
 * - Satellites: Descriptive attributes with full history tracking
 *
 * ICEBERG TABLE BENEFITS:
 * - ACID transactions (atomic writes, consistent reads)
 * - Schema evolution (add columns without breaking queries)
 * - Time travel (query historical snapshots)
 * - Partition evolution (change partitioning without rewriting data)
 * - Hidden partitioning (partition pruning without WHERE clauses)
 *
 * PARTITIONING STRATEGY:
 * - All tables partitioned by load_date (daily ingestion pattern)
 * - Satellites also use valid_from for temporal queries
 * - Iceberg transforms: days(timestamp) for automatic bucketing
 * ========================================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object RawVaultSchema {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ CREATE ALL RAW VAULT TABLES                                     │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createAllTables()(implicit spark: SparkSession): Unit = {

    println("""
         |╔════════════════════════════════════════════════════════════════╗
         |║      CREATING DATA VAULT 2.0 RAW VAULT SCHEMA (BRONZE)        ║
         |╚════════════════════════════════════════════════════════════════╝
         |""".stripMargin)

    // Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    println("✅ Database 'bronze' ready")

    // Create Hubs (Business entities - business keys only)
    createHubCustomer()
    createHubAccount()
    createHubTransaction()
    createHubTransactionItem()

    // Create Links (Relationships between hubs)
    createLinkCustomerAccount()
    createLinkTransactionItem()

    // Create Satellites (Descriptive attributes with history)
    createSatCustomer()
    createSatAccount()
    createSatTransaction()
    createSatTransactionItem()

    // Create Load Metadata table (Audit trail)
    createLoadMetadata()

    println("\n✅ All Raw Vault tables created successfully")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ HUB_CUSTOMER - Customer Business Keys                          │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * DATA VAULT HUB PATTERN:
   * - Stores only unique business keys (customer_id)
   * - Hash key for deterministic joins
   * - No descriptive attributes (those go in satellites)
   * - Insert-only (immutable)
   */
  def createHubCustomer()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.hub_customer (
        customer_hash_key STRING COMMENT 'MD5 hash of customer_id',
        customer_id INT COMMENT 'Business key from source system',
        load_date DATE COMMENT 'Date when record was loaded into vault',
        record_source STRING COMMENT 'Source system identifier'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'Data Vault Hub: Customer business keys only'
      )
    """)
    println("  📦 Created Hub_Customer")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ HUB_ACCOUNT - Account Business Keys                            │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createHubAccount()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.hub_account (
        account_hash_key STRING COMMENT 'MD5 hash of account_id',
        account_id INT COMMENT 'Business key from source system',
        load_date DATE COMMENT 'Date when record was loaded into vault',
        record_source STRING COMMENT 'Source system identifier'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'Data Vault Hub: Account business keys only'
      )
    """)
    println("  📦 Created Hub_Account")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ HUB_TRANSACTION - Transaction Business Keys                    │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createHubTransaction()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.hub_transaction (
        transaction_hash_key STRING COMMENT 'MD5 hash of transaction_id',
        transaction_id INT COMMENT 'Business key from source system',
        load_date DATE COMMENT 'Date when record was loaded into vault',
        record_source STRING COMMENT 'Source system identifier'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'Data Vault Hub: Transaction business keys only'
      )
    """)
    println("  📦 Created Hub_Transaction")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ HUB_TRANSACTION_ITEM - Transaction Item Business Keys          │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * MULTI-ITEM PATTERN:
   * - Composite business key: transaction_id + item_sequence
   * - Enables modeling of transactions with multiple line items
   * - Similar to e-commerce shopping cart pattern
   */
  def createHubTransactionItem()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.hub_transaction_item (
        transaction_item_hash_key STRING COMMENT 'MD5 hash of transaction_id + item_sequence',
        transaction_id INT COMMENT 'Parent transaction business key',
        item_sequence INT COMMENT 'Item sequence number within transaction',
        load_date DATE COMMENT 'Date when record was loaded into vault',
        record_source STRING COMMENT 'Source system identifier'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'Data Vault Hub: Transaction item business keys (multi-item pattern)'
      )
    """)
    println("  📦 Created Hub_Transaction_Item")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LINK_CUSTOMER_ACCOUNT - Customer-Account Relationship          │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * DATA VAULT LINK PATTERN:
   * - Connects Hub_Customer to Hub_Account
   * - Link hash key = MD5(customer_hash_key || account_hash_key)
   * - No descriptive attributes
   * - Captures many-to-many relationships
   */
  def createLinkCustomerAccount()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.link_customer_account (
        link_customer_account_hash_key STRING COMMENT 'MD5 hash of customer_hash_key + account_hash_key',
        customer_hash_key STRING COMMENT 'Foreign key to Hub_Customer',
        account_hash_key STRING COMMENT 'Foreign key to Hub_Account',
        load_date DATE COMMENT 'Date when relationship was established',
        record_source STRING COMMENT 'Source system identifier'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'Data Vault Link: Customer-Account many-to-many relationship'
      )
    """)
    println("  🔗 Created Link_Customer_Account")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LINK_TRANSACTION_ITEM - Transaction-Item Relationship          │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createLinkTransactionItem()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.link_transaction_item (
        link_transaction_item_hash_key STRING COMMENT 'MD5 hash of transaction_hash_key + transaction_item_hash_key',
        transaction_hash_key STRING COMMENT 'Foreign key to Hub_Transaction',
        transaction_item_hash_key STRING COMMENT 'Foreign key to Hub_Transaction_Item',
        load_date DATE COMMENT 'Date when relationship was established',
        record_source STRING COMMENT 'Source system identifier'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'Data Vault Link: Transaction-Item relationship (multi-item pattern)'
      )
    """)
    println("  🔗 Created Link_Transaction_Item")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ SAT_CUSTOMER - Customer Descriptive Attributes                  │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * DATA VAULT SATELLITE PATTERN:
   * - Full history of attribute changes
   * - Temporal tracking: valid_from, valid_to
   * - Diff hash for change detection
   * - Current records have valid_to = NULL
   */
  def createSatCustomer()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.sat_customer (
        customer_hash_key STRING COMMENT 'Foreign key to Hub_Customer',
        customer_type STRING COMMENT 'INDIVIDUAL or BUSINESS',
        first_name STRING COMMENT 'First name (for individuals)',
        last_name STRING COMMENT 'Last name (for individuals)',
        company_name STRING COMMENT 'Company name (for business)',
        email STRING COMMENT 'Email address',
        phone STRING COMMENT 'Phone number',
        date_of_birth DATE COMMENT 'Date of birth (for individuals)',
        tax_id STRING COMMENT 'Tax identification number',
        customer_since DATE COMMENT 'Date customer relationship started',
        customer_status STRING COMMENT 'ACTIVE, INACTIVE, SUSPENDED',
        credit_score INT COMMENT 'Credit score (if available)',
        customer_diff_hash STRING COMMENT 'MD5 hash of all descriptive columns for change detection',
        valid_from TIMESTAMP COMMENT 'Start of validity period for this version',
        valid_to TIMESTAMP COMMENT 'End of validity period (NULL for current version)',
        load_date DATE COMMENT 'Date when record was loaded into vault',
        record_source STRING COMMENT 'Source system identifier'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'Data Vault Satellite: Customer descriptive attributes with full history (SCD Type 2)'
      )
    """)
    println("  🛰️  Created Sat_Customer")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ SAT_ACCOUNT - Account Descriptive Attributes                    │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createSatAccount()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.sat_account (
        account_hash_key STRING COMMENT 'Foreign key to Hub_Account',
        account_number STRING COMMENT 'Account number (display)',
        account_type STRING COMMENT 'CHECKING, SAVINGS, CREDIT, LOAN',
        product_id INT COMMENT 'Product identifier',
        branch_id INT COMMENT 'Branch where account was opened',
        balance DECIMAL(15,2) COMMENT 'Current balance',
        available_balance DECIMAL(15,2) COMMENT 'Available balance (balance - holds)',
        currency_code STRING COMMENT 'Currency code (USD, EUR, etc.)',
        interest_rate DECIMAL(5,4) COMMENT 'Annual interest rate',
        credit_limit DECIMAL(15,2) COMMENT 'Credit limit (for credit accounts)',
        opened_date DATE COMMENT 'Date account was opened',
        account_status STRING COMMENT 'ACTIVE, CLOSED, FROZEN',
        account_diff_hash STRING COMMENT 'MD5 hash of all descriptive columns',
        valid_from TIMESTAMP COMMENT 'Start of validity period',
        valid_to TIMESTAMP COMMENT 'End of validity period (NULL for current)',
        load_date DATE COMMENT 'Date when record was loaded',
        record_source STRING COMMENT 'Source system identifier'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'Data Vault Satellite: Account descriptive attributes with history'
      )
    """)
    println("  🛰️  Created Sat_Account")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ SAT_TRANSACTION - Transaction Descriptive Attributes           │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createSatTransaction()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.sat_transaction (
        transaction_hash_key STRING COMMENT 'Foreign key to Hub_Transaction',
        transaction_number STRING COMMENT 'Transaction number (display)',
        transaction_type STRING COMMENT 'DEPOSIT, WITHDRAWAL, TRANSFER, PAYMENT',
        transaction_date DATE COMMENT 'Date of transaction',
        transaction_time TIMESTAMP COMMENT 'Timestamp of transaction',
        total_amount DECIMAL(15,2) COMMENT 'Total transaction amount',
        currency_code STRING COMMENT 'Currency code',
        description STRING COMMENT 'Transaction description',
        channel STRING COMMENT 'BRANCH, ATM, ONLINE, MOBILE',
        status STRING COMMENT 'PENDING, COMPLETED, FAILED, REVERSED',
        transaction_diff_hash STRING COMMENT 'MD5 hash of all descriptive columns',
        valid_from TIMESTAMP COMMENT 'Start of validity period',
        valid_to TIMESTAMP COMMENT 'End of validity period (NULL for current)',
        load_date DATE COMMENT 'Date when record was loaded',
        record_source STRING COMMENT 'Source system identifier'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'Data Vault Satellite: Transaction header attributes with history'
      )
    """)
    println("  🛰️  Created Sat_Transaction")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ SAT_TRANSACTION_ITEM - Transaction Item Attributes             │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * MULTI-ITEM PATTERN:
   * - Each transaction can have multiple line items
   * - Sum of item_amount should equal transaction total_amount
   * - Supports complex transaction scenarios (e.g., split payments)
   */
  def createSatTransactionItem()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.sat_transaction_item (
        transaction_item_hash_key STRING COMMENT 'Foreign key to Hub_Transaction_Item',
        item_type STRING COMMENT 'DEBIT, CREDIT, FEE, TAX',
        item_description STRING COMMENT 'Item description',
        item_amount DECIMAL(15,2) COMMENT 'Item amount',
        item_category STRING COMMENT 'Category code',
        merchant_id INT COMMENT 'Merchant identifier (if applicable)',
        item_diff_hash STRING COMMENT 'MD5 hash of all descriptive columns',
        valid_from TIMESTAMP COMMENT 'Start of validity period',
        valid_to TIMESTAMP COMMENT 'End of validity period (NULL for current)',
        load_date DATE COMMENT 'Date when record was loaded',
        record_source STRING COMMENT 'Source system identifier'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'Data Vault Satellite: Transaction item line details (multi-item pattern)'
      )
    """)
    println("  🛰️  Created Sat_Transaction_Item")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOAD_METADATA - ETL Audit Trail                                │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Track all ETL load operations for auditing and troubleshooting.
   * Essential for data lineage and compliance.
   */
  def createLoadMetadata()(implicit spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS bronze.load_metadata (
        load_id BIGINT COMMENT 'Unique load identifier',
        entity_name STRING COMMENT 'Entity being loaded (customer, account, etc.)',
        record_source STRING COMMENT 'Source system',
        load_date DATE COMMENT 'Date of load',
        load_start_timestamp TIMESTAMP COMMENT 'When load started',
        load_end_timestamp TIMESTAMP COMMENT 'When load completed',
        load_status STRING COMMENT 'SUCCESS, FAILED, IN_PROGRESS',
        records_extracted BIGINT COMMENT 'Number of records read from source',
        records_loaded BIGINT COMMENT 'Number of records written to vault',
        error_message STRING COMMENT 'Error message if failed',
        load_duration_seconds INT COMMENT 'Duration in seconds'
      )
      USING iceberg
      PARTITIONED BY (load_date)
      TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'comment' = 'ETL load metadata for audit trail and monitoring'
      )
    """)
    println("  📋 Created Load_Metadata")
  }
}
