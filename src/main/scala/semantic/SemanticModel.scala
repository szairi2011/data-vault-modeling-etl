package semantic

/**
 * ========================================================================
 * SEMANTIC MODEL - BUSINESS VIEWS LAYER
 * ========================================================================
 *
 * PURPOSE:
 * Create business-friendly views for analytics and BI tools.
 *
 * LEARNING OBJECTIVES:
 * - Semantic layer design principles
 * - View abstraction benefits
 * - Business metric calculations
 * - Complex aggregations simplified
 *
 * WHY SEMANTIC LAYER:
 * 1. **Simplification**: Hide complexity from end users
 * 2. **Consistency**: Single version of truth for metrics
 * 3. **Performance**: Pre-aggregated metrics
 * 4. **Business Language**: Use business terms, not technical names
 * 5. **Governance**: Controlled access to certified data
 *
 * ARCHITECTURE:
 * ```
 * Gold Layer (Dimensional Model)
 *     ↓
 * Semantic Views (This Layer)
 *     ↓
 * BI Tools (Tableau, PowerBI, Looker)
 * Ad-hoc Queries (Analysts)
 * Applications (APIs)
 * ```
 *
 * VIEW TYPES:
 * 1. **Entity Views**: Customer 360, Product Catalog
 * 2. **Metric Views**: Monthly Revenue, Customer Lifetime Value
 * 3. **Analytical Views**: Cohort Analysis, Funnel Analysis
 * 4. **Snapshot Views**: Daily KPIs, Management Dashboard
 *
 * ========================================================================
 */

import org.apache.spark.sql.SparkSession

object SemanticModel {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ MAIN ENTRY POINT                                                │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def main(args: Array[String]): Unit = {

    println("""
         |╔════════════════════════════════════════════════════════════════╗
         |║         DATA VAULT 2.0 - SEMANTIC MODEL (VIEWS)               ║
         |╚════════════════════════════════════════════════════════════════╝
         |""".stripMargin)

    implicit val spark = createSparkSession()

    try {
      createAllViews()
      println("\n✅ All semantic views created successfully")

      // Show available views
      showAvailableViews()

    } catch {
      case e: Exception =>
        println(s"\n❌ Semantic Model creation failed: ${e.getMessage}")
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
      .appName("Semantic Model - Views")
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
   * │ CREATE ALL SEMANTIC VIEWS                                       │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createAllViews()(implicit spark: SparkSession): Unit = {

    println("\n📊 Creating semantic views...")

    createCustomer360View()
    createMonthlyTransactionSummaryView()
    createAccountBalanceTrendView()
    createLoyaltyAnalysisView()
    createProductPerformanceView()
    createBranchPerformanceView()
    createCustomerSegmentationView()
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ CUSTOMER 360 VIEW                                               │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Complete customer view with all accounts, balances, and activities.
   *
   * USE CASES:
   * - Customer service representatives
   * - Marketing campaigns
   * - Risk assessment
   */
  def createCustomer360View()(implicit spark: SparkSession): Unit = {

    println("   📋 Creating v_customer_360...")

    spark.sql("""
      CREATE OR REPLACE VIEW gold.v_customer_360 AS
      SELECT
        -- Customer identification
        c.customer_id,
        c.full_name,
        c.customer_type,
        c.email,
        c.phone,

        -- Customer demographics
        c.age,
        c.date_of_birth,
        c.customer_since,
        c.tenure_years,

        -- Customer status
        c.customer_status,
        c.credit_score,
        c.credit_category,
        c.loyalty_tier,

        -- Account aggregations
        COUNT(DISTINCT a.account_key) as total_accounts,
        COUNT(DISTINCT CASE WHEN a.account_status = 'ACTIVE' THEN a.account_key END) as active_accounts,

        -- Balance aggregations (current balances from fact)
        COALESCE(SUM(CASE WHEN a.account_status = 'ACTIVE' THEN fab.balance END), 0) as total_balance,
        COALESCE(SUM(CASE WHEN a.account_type = 'CHECKING' THEN fab.balance END), 0) as checking_balance,
        COALESCE(SUM(CASE WHEN a.account_type = 'SAVINGS' THEN fab.balance END), 0) as savings_balance,
        COALESCE(SUM(CASE WHEN a.account_type = 'CREDIT_CARD' THEN fab.balance END), 0) as credit_card_balance,
        COALESCE(SUM(CASE WHEN a.account_type = 'LOAN' THEN fab.balance END), 0) as loan_balance,

        -- Transaction activity (last 30 days)
        COUNT(DISTINCT ft.transaction_key) as transactions_30d,
        COALESCE(SUM(ft.total_amount), 0) as transaction_volume_30d,

        -- Audit
        c.effective_date as customer_version_effective_date,
        c.is_current as is_current_customer_version

      FROM gold.dim_customer c
      LEFT JOIN gold.dim_account a
        ON c.customer_key = a.customer_key
        AND a.is_current = TRUE
      LEFT JOIN gold.fact_account_balance fab
        ON a.account_key = fab.account_key
        AND fab.snapshot_date = CURRENT_DATE
      LEFT JOIN gold.fact_transaction ft
        ON c.customer_key = ft.customer_key
        AND ft.transaction_date_key >= CAST(DATE_FORMAT(DATE_SUB(CURRENT_DATE, 30), 'yyyyMMdd') AS INT)

      WHERE c.is_current = TRUE

      GROUP BY
        c.customer_id, c.full_name, c.customer_type, c.email, c.phone,
        c.age, c.date_of_birth, c.customer_since, c.tenure_years,
        c.customer_status, c.credit_score, c.credit_category, c.loyalty_tier,
        c.effective_date, c.is_current
    """)

    println("      ✅ Created v_customer_360")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ MONTHLY TRANSACTION SUMMARY VIEW                                │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Monthly aggregated transaction metrics.
   *
   * USE CASES:
   * - Financial reporting
   * - Trend analysis
   * - Budget vs. actuals
   */
  def createMonthlyTransactionSummaryView()(implicit spark: SparkSession): Unit = {

    println("   📋 Creating v_monthly_transaction_summary...")

    spark.sql("""
      CREATE OR REPLACE VIEW gold.v_monthly_transaction_summary AS
      SELECT
        d.year,
        d.month,
        d.month_name,
        d.quarter,

        -- Transaction counts
        COUNT(DISTINCT ft.transaction_key) as total_transactions,
        COUNT(DISTINCT ft.customer_key) as unique_customers,
        COUNT(DISTINCT ft.account_key) as unique_accounts,

        -- Transaction volumes by type
        COUNT(DISTINCT CASE WHEN ft.transaction_type = 'DEPOSIT' THEN ft.transaction_key END) as deposits_count,
        COUNT(DISTINCT CASE WHEN ft.transaction_type = 'WITHDRAWAL' THEN ft.transaction_key END) as withdrawals_count,
        COUNT(DISTINCT CASE WHEN ft.transaction_type = 'PAYMENT' THEN ft.transaction_key END) as payments_count,
        COUNT(DISTINCT CASE WHEN ft.transaction_type = 'TRANSFER' THEN ft.transaction_key END) as transfers_count,

        -- Transaction amounts
        SUM(ft.total_amount) as total_amount,
        SUM(CASE WHEN ft.transaction_type = 'DEPOSIT' THEN ft.total_amount ELSE 0 END) as deposit_amount,
        SUM(CASE WHEN ft.transaction_type = 'WITHDRAWAL' THEN ft.total_amount ELSE 0 END) as withdrawal_amount,
        SUM(CASE WHEN ft.transaction_type = 'PAYMENT' THEN ft.total_amount ELSE 0 END) as payment_amount,

        -- Channel distribution
        COUNT(DISTINCT CASE WHEN ft.channel = 'ONLINE' THEN ft.transaction_key END) as online_transactions,
        COUNT(DISTINCT CASE WHEN ft.channel = 'MOBILE' THEN ft.transaction_key END) as mobile_transactions,
        COUNT(DISTINCT CASE WHEN ft.channel = 'BRANCH' THEN ft.transaction_key END) as branch_transactions,
        COUNT(DISTINCT CASE WHEN ft.channel = 'ATM' THEN ft.transaction_key END) as atm_transactions,

        -- Averages
        AVG(ft.total_amount) as avg_transaction_amount,

        -- Date range
        MIN(d.date) as month_start_date,
        MAX(d.date) as month_end_date

      FROM gold.fact_transaction ft
      JOIN gold.dim_date d ON ft.transaction_date_key = d.date_key

      GROUP BY d.year, d.month, d.month_name, d.quarter
      ORDER BY d.year DESC, d.month DESC
    """)

    println("      ✅ Created v_monthly_transaction_summary")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ ACCOUNT BALANCE TREND VIEW                                      │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Daily balance trends for accounts.
   *
   * USE CASES:
   * - Balance forecasting
   * - Liquidity analysis
   * - Customer behavior patterns
   */
  def createAccountBalanceTrendView()(implicit spark: SparkSession): Unit = {

    println("   📋 Creating v_account_balance_trend...")

    spark.sql("""
      CREATE OR REPLACE VIEW gold.v_account_balance_trend AS
      SELECT
        d.date,
        d.year,
        d.month,
        d.day_name,
        d.is_weekend,
        d.is_business_day,

        a.account_type,
        p.product_category,
        b.region,

        -- Balance aggregations
        COUNT(DISTINCT fab.account_key) as account_count,
        SUM(fab.balance) as total_balance,
        AVG(fab.balance) as avg_balance,
        MIN(fab.balance) as min_balance,
        MAX(fab.balance) as max_balance,

        -- Change metrics
        SUM(fab.balance_change) as total_balance_change,
        AVG(fab.balance_change) as avg_balance_change,

        -- Activity metrics
        SUM(fab.transaction_count) as transaction_count,
        SUM(fab.deposit_amount) as deposit_amount,
        SUM(fab.withdrawal_amount) as withdrawal_amount

      FROM gold.fact_account_balance fab
      JOIN gold.dim_date d ON fab.snapshot_date_key = d.date_key
      JOIN gold.dim_account a ON fab.account_key = a.account_key
      JOIN gold.dim_product p ON fab.product_key = p.product_key
      JOIN gold.dim_branch b ON fab.branch_key = b.branch_key

      WHERE a.is_current = TRUE

      GROUP BY
        d.date, d.year, d.month, d.day_name, d.is_weekend, d.is_business_day,
        a.account_type, p.product_category, b.region

      ORDER BY d.date DESC
    """)

    println("      ✅ Created v_account_balance_trend")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ LOYALTY ANALYSIS VIEW                                           │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Analyze loyalty tier distribution and performance.
   * Demonstrates schema evolution benefit (loyalty_tier added later).
   *
   * USE CASES:
   * - Loyalty program effectiveness
   * - Tier migration analysis
   * - Customer value segmentation
   */
  def createLoyaltyAnalysisView()(implicit spark: SparkSession): Unit = {

    println("   📋 Creating v_loyalty_analysis...")

    spark.sql("""
      CREATE OR REPLACE VIEW gold.v_loyalty_analysis AS
      SELECT
        c.loyalty_tier,
        c.customer_type,

        -- Customer counts
        COUNT(DISTINCT c.customer_key) as customer_count,

        -- Demographics
        AVG(c.age) as avg_age,
        AVG(c.tenure_years) as avg_tenure_years,
        AVG(c.credit_score) as avg_credit_score,

        -- Account metrics
        COUNT(DISTINCT a.account_key) as total_accounts,
        AVG(CASE WHEN a.is_current THEN 1 ELSE 0 END) as avg_accounts_per_customer,

        -- Balance metrics (from latest snapshot)
        AVG(fab.balance) as avg_balance,
        SUM(fab.balance) as total_balance,
        PERCENTILE(fab.balance, 0.5) as median_balance,

        -- Transaction metrics (last 90 days)
        AVG(txn_summary.transaction_count) as avg_transactions_90d,
        AVG(txn_summary.transaction_volume) as avg_transaction_volume_90d

      FROM gold.dim_customer c
      LEFT JOIN gold.dim_account a
        ON c.customer_key = a.customer_key
        AND a.is_current = TRUE
      LEFT JOIN gold.fact_account_balance fab
        ON a.account_key = fab.account_key
        AND fab.snapshot_date = CURRENT_DATE
      LEFT JOIN (
        SELECT
          customer_key,
          COUNT(*) as transaction_count,
          SUM(total_amount) as transaction_volume
        FROM gold.fact_transaction
        WHERE transaction_date_key >= CAST(DATE_FORMAT(DATE_SUB(CURRENT_DATE, 90), 'yyyyMMdd') AS INT)
        GROUP BY customer_key
      ) txn_summary ON c.customer_key = txn_summary.customer_key

      WHERE c.is_current = TRUE
        AND c.loyalty_tier IS NOT NULL

      GROUP BY c.loyalty_tier, c.customer_type
      ORDER BY
        CASE c.loyalty_tier
          WHEN 'PLATINUM' THEN 1
          WHEN 'GOLD' THEN 2
          WHEN 'SILVER' THEN 3
          WHEN 'STANDARD' THEN 4
          ELSE 5
        END
    """)

    println("      ✅ Created v_loyalty_analysis")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PRODUCT PERFORMANCE VIEW                                        │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createProductPerformanceView()(implicit spark: SparkSession): Unit = {

    println("   📋 Creating v_product_performance...")

    spark.sql("""
      CREATE OR REPLACE VIEW gold.v_product_performance AS
      SELECT
        p.product_id,
        p.product_name,
        p.product_category,
        p.product_subcategory,

        -- Account metrics
        COUNT(DISTINCT a.account_key) as active_accounts,
        COUNT(DISTINCT a.customer_key) as unique_customers,

        -- Balance metrics
        SUM(fab.balance) as total_balance,
        AVG(fab.balance) as avg_balance_per_account,

        -- Growth (month-over-month)
        SUM(fab.balance_change) as balance_change_mtd

      FROM gold.dim_product p
      JOIN gold.dim_account a ON p.product_key = a.product_key AND a.is_current = TRUE
      JOIN gold.fact_account_balance fab
        ON a.account_key = fab.account_key
        AND fab.snapshot_date = CURRENT_DATE

      WHERE p.is_active = TRUE

      GROUP BY
        p.product_id, p.product_name, p.product_category, p.product_subcategory

      ORDER BY total_balance DESC
    """)

    println("      ✅ Created v_product_performance")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ BRANCH PERFORMANCE VIEW                                         │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createBranchPerformanceView()(implicit spark: SparkSession): Unit = {

    println("   📋 Creating v_branch_performance...")

    spark.sql("""
      CREATE OR REPLACE VIEW gold.v_branch_performance AS
      SELECT
        b.branch_id,
        b.branch_name,
        b.city,
        b.state,
        b.region,

        -- Customer metrics
        COUNT(DISTINCT a.customer_key) as total_customers,

        -- Account metrics
        COUNT(DISTINCT a.account_key) as total_accounts,
        COUNT(DISTINCT CASE WHEN a.account_status = 'ACTIVE' THEN a.account_key END) as active_accounts,

        -- Balance metrics
        SUM(fab.balance) as total_balance,
        AVG(fab.balance) as avg_balance_per_account,

        -- Transaction metrics (last 30 days)
        COUNT(DISTINCT ft.transaction_key) as transactions_30d,
        SUM(ft.total_amount) as transaction_volume_30d

      FROM gold.dim_branch b
      JOIN gold.dim_account a ON b.branch_key = a.branch_key AND a.is_current = TRUE
      LEFT JOIN gold.fact_account_balance fab
        ON a.account_key = fab.account_key
        AND fab.snapshot_date = CURRENT_DATE
      LEFT JOIN gold.fact_transaction ft
        ON a.account_key = ft.account_key
        AND ft.transaction_date_key >= CAST(DATE_FORMAT(DATE_SUB(CURRENT_DATE, 30), 'yyyyMMdd') AS INT)

      WHERE b.is_active = TRUE

      GROUP BY
        b.branch_id, b.branch_name, b.city, b.state, b.region

      ORDER BY total_balance DESC
    """)

    println("      ✅ Created v_branch_performance")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ CUSTOMER SEGMENTATION VIEW                                      │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def createCustomerSegmentationView()(implicit spark: SparkSession): Unit = {

    println("   📋 Creating v_customer_segmentation...")

    spark.sql("""
      CREATE OR REPLACE VIEW gold.v_customer_segmentation AS
      SELECT
        -- Segmentation dimensions
        c.customer_type,
        c.loyalty_tier,
        c.credit_category,
        CASE
          WHEN c.age < 25 THEN '18-24'
          WHEN c.age < 35 THEN '25-34'
          WHEN c.age < 45 THEN '35-44'
          WHEN c.age < 55 THEN '45-54'
          WHEN c.age < 65 THEN '55-64'
          ELSE '65+'
        END as age_group,
        CASE
          WHEN c.tenure_years < 1 THEN '<1 year'
          WHEN c.tenure_years < 3 THEN '1-3 years'
          WHEN c.tenure_years < 5 THEN '3-5 years'
          WHEN c.tenure_years < 10 THEN '5-10 years'
          ELSE '10+ years'
        END as tenure_segment,

        -- Metrics
        COUNT(DISTINCT c.customer_key) as customer_count,
        AVG(total_balance) as avg_total_balance,
        AVG(transactions_30d) as avg_transactions_30d

      FROM gold.v_customer_360 c

      GROUP BY
        c.customer_type, c.loyalty_tier, c.credit_category,
        age_group, tenure_segment

      ORDER BY customer_count DESC
    """)

    println("      ✅ Created v_customer_segmentation")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ SHOW AVAILABLE VIEWS                                            │
   * └─────────────────────────────────────────────────────────────────┘
   */
  def showAvailableViews()(implicit spark: SparkSession): Unit = {

    println("""
         |
         |┌────────────────────────────────────────────────────────────────┐
         |│ AVAILABLE SEMANTIC VIEWS                                      │
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    spark.sql("SHOW VIEWS IN gold").show(false)

    println("""
         |📊 Query Examples:
         |
         |-- Customer 360 view
         |SELECT * FROM gold.v_customer_360 WHERE customer_id = 123;
         |
         |-- Monthly trends
         |SELECT * FROM gold.v_monthly_transaction_summary
         |WHERE year = 2025 ORDER BY month;
         |
         |-- Loyalty analysis
         |SELECT * FROM gold.v_loyalty_analysis;
         |
         |-- Product performance
         |SELECT * FROM gold.v_product_performance ORDER BY total_balance DESC;
         |
         |""".stripMargin)
  }
}

