# Semantic Layer - Business Query Interface

## What is a Semantic Layer?

A **semantic layer** is an abstraction that sits between the physical data model and business users. It provides:

1. **Business-friendly names** instead of technical column names
2. **Pre-defined joins** to hide complexity
3. **Calculated metrics** with consistent business logic
4. **Query templates** for common questions

Think of it as a **translation layer** between "data speak" and "business speak".

---

## Problem Without Semantic Layer

### Raw Query (Complex, Error-Prone)

```sql
-- Business question: "Show me total spending per customer by category"
-- Technical query: Requires understanding of star schema, joins, aggregations

SELECT 
    c.first_name || ' ' || c.last_name as customer_name,
    cat.category_name,
    SUM(fi.item_amount) as total_spending
FROM gold.fact_transaction_items fi
JOIN gold.dim_customer c ON fi.customer_sk = c.customer_sk
JOIN gold.dim_category cat ON fi.category_sk = cat.category_sk
JOIN gold.dim_date d ON fi.date_sk = d.date_sk
WHERE c.is_current = TRUE
  AND d.year = 2025
  AND fi.item_amount > 0
GROUP BY c.first_name, c.last_name, cat.category_name
ORDER BY total_spending DESC;
```

**Problems**:
- Users must know table structure
- Risk of incorrect joins
- Inconsistent calculations across reports
- Repeated logic in every query

---

## Solution: Semantic Layer

### Business View (Simple, Consistent)

```sql
-- Same business question, using semantic layer
SELECT 
    customer_name,
    category,
    total_spending
FROM customer_spending_by_category
WHERE year = 2025
ORDER BY total_spending DESC;
```

**Benefits**:
- No joins visible
- Consistent metric definitions
- Type-safe (if using Scala API)
- Self-documenting

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Business Users                         │
│  - Analysts                                              │
│  - Data Scientists                                       │
│  - BI Tools (Tableau, Power BI)                          │
└──────────────────────────────────────────────────────────┘
                          ↓
                          ↓ Simple SQL or API calls
                          ↓
┌──────────────────────────────────────────────────────────┐
│                   SEMANTIC LAYER                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Business Views                                    │  │
│  │  - customer_account_summary                        │  │
│  │  - customer_spending_by_category                   │  │
│  │  - monthly_transaction_trends                      │  │
│  └────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Metrics Catalog                                   │  │
│  │  - customer_lifetime_value                         │  │
│  │  - average_account_balance                         │  │
│  │  - total_deposits, total_withdrawals               │  │
│  └────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Query Interface (Scala API)                       │  │
│  │  - Type-safe query methods                         │  │
│  │  - Parameter validation                            │  │
│  │  - Result set mapping                              │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
                          ↓
                          ↓ SQL queries with joins
                          ↓
┌──────────────────────────────────────────────────────────┐
│              GOLD LAYER (Dimensional Model)               │
│  - dim_customer, dim_account, dim_product                │
│  - fact_transactions, fact_transaction_items             │
│  - fact_account_balance_daily                            │
└──────────────────────────────────────────────────────────┘
```

---

## Component 1: Business Views

### Definition

Business views are pre-defined SQL views that encapsulate common joins and transformations.

### Example: Customer Account Summary

```scala
// Register view in Spark catalog
spark.sql("""
  CREATE OR REPLACE VIEW customer_account_summary AS
  SELECT 
    c.customer_sk,
    c.first_name,
    c.last_name,
    c.email,
    c.customer_type,
    c.loyalty_tier,
    a.account_sk,
    a.account_number,
    a.account_status,
    p.product_name,
    p.product_type,
    a.current_balance,
    a.available_balance,
    b.branch_name,
    b.city,
    b.state
  FROM gold.dim_customer c
  INNER JOIN gold.dim_account a ON c.customer_sk = a.customer_sk
  INNER JOIN gold.dim_product p ON a.product_sk = p.product_sk
  INNER JOIN gold.dim_branch b ON a.branch_sk = b.branch_sk
  WHERE c.is_current = TRUE
    AND a.account_status = 'ACTIVE'
""")
```

### Usage

```scala
// Business user query - simple!
spark.sql("""
  SELECT 
    first_name, 
    last_name, 
    COUNT(*) as account_count,
    SUM(current_balance) as total_balance
  FROM customer_account_summary
  WHERE loyalty_tier = 'GOLD'
  GROUP BY first_name, last_name
  ORDER BY total_balance DESC
  LIMIT 10
""").show()
```

---

## Component 2: Metrics Catalog

### Definition

A catalog of business metrics with consistent definitions and calculations.

### Metric Types

#### 1. Additive Metrics
Can be summed across all dimensions

```scala
case class MetricDefinition(
  name: String,
  sql: String,
  description: String,
  category: String
)

val totalDeposits = MetricDefinition(
  name = "total_deposits",
  sql = "SUM(CASE WHEN transaction_type = 'DEPOSIT' THEN amount ELSE 0 END)",
  description = "Sum of all deposit transactions",
  category = "transaction"
)

val totalWithdrawals = MetricDefinition(
  name = "total_withdrawals",
  sql = "SUM(CASE WHEN transaction_type = 'WITHDRAWAL' THEN amount ELSE 0 END)",
  description = "Sum of all withdrawal transactions",
  category = "transaction"
)
```

#### 2. Semi-Additive Metrics
Cannot be summed across time dimension

```scala
val accountBalance = MetricDefinition(
  name = "account_balance",
  sql = "LAST_VALUE(current_balance) OVER (PARTITION BY account_sk ORDER BY date_sk)",
  description = "Current account balance (last value in period)",
  category = "balance"
)
```

#### 3. Non-Additive Metrics
Ratios and percentages

```scala
val averageAccountBalance = MetricDefinition(
  name = "average_account_balance",
  sql = "AVG(current_balance)",
  description = "Average balance across all accounts",
  category = "balance"
)

val savingsRate = MetricDefinition(
  name = "savings_rate",
  sql = "SUM(deposits) / SUM(income) * 100",
  description = "Percentage of income saved",
  category = "customer"
)
```

### Using Metrics

```scala
// Apply metric to fact table
spark.sql(s"""
  SELECT 
    c.customer_type,
    ${totalDeposits.sql} as total_deposits,
    ${totalWithdrawals.sql} as total_withdrawals,
    ${averageAccountBalance.sql} as avg_balance
  FROM gold.fact_transactions f
  JOIN gold.dim_customer c ON f.customer_sk = c.customer_sk
  WHERE c.is_current = TRUE
  GROUP BY c.customer_type
""").show()
```

---

## Component 3: Query Interface

### Type-Safe Scala API

```scala
object QueryInterface {
  
  /**
   * Get customer account summary with filters
   * 
   * @param loyaltyTier Optional filter by loyalty tier
   * @param minBalance Optional minimum balance filter
   * @return DataFrame with customer account details
   */
  def getCustomerAccountSummary(
    loyaltyTier: Option[String] = None,
    minBalance: Option[Double] = None
  )(implicit spark: SparkSession): DataFrame = {
    
    var query = """
      SELECT * FROM customer_account_summary
      WHERE 1=1
    """
    
    loyaltyTier.foreach { tier =>
      query += s" AND loyalty_tier = '$tier'"
    }
    
    minBalance.foreach { balance =>
      query += s" AND current_balance >= $balance"
    }
    
    spark.sql(query)
  }
  
  /**
   * Get top customers by transaction volume
   * 
   * @param topN Number of customers to return
   * @param startDate Optional start date filter
   * @param endDate Optional end date filter
   * @return DataFrame with customer transaction volume
   */
  def getTopCustomersByVolume(
    topN: Int = 10,
    startDate: Option[String] = None,
    endDate: Option[String] = None
  )(implicit spark: SparkSession): DataFrame = {
    
    var dateFilter = "1=1"
    
    startDate.foreach { start =>
      dateFilter += s" AND d.date_key >= '$start'"
    }
    
    endDate.foreach { end =>
      dateFilter += s" AND d.date_key <= '$end'"
    }
    
    spark.sql(s"""
      SELECT 
        c.first_name,
        c.last_name,
        c.email,
        SUM(f.transaction_amount) as total_volume,
        COUNT(f.transaction_sk) as transaction_count,
        AVG(f.transaction_amount) as avg_transaction_amount
      FROM gold.fact_transactions f
      INNER JOIN gold.dim_customer c ON f.customer_sk = c.customer_sk
      INNER JOIN gold.dim_date d ON f.date_sk = d.date_sk
      WHERE c.is_current = TRUE
        AND $dateFilter
      GROUP BY c.first_name, c.last_name, c.email
      ORDER BY total_volume DESC
      LIMIT $topN
    """)
  }
  
  /**
   * Get spending by category for a customer
   * 
   * @param customerEmail Customer email to filter
   * @param year Optional year filter
   * @return DataFrame with spending breakdown by category
   */
  def getCustomerSpendingByCategory(
    customerEmail: String,
    year: Option[Int] = None
  )(implicit spark: SparkSession): DataFrame = {
    
    val yearFilter = year.map(y => s" AND d.year = $y").getOrElse("")
    
    spark.sql(s"""
      SELECT 
        cat.category_name,
        cat.parent_category_name,
        SUM(fi.item_amount) as total_spending,
        COUNT(DISTINCT f.transaction_sk) as transaction_count,
        AVG(fi.item_amount) as avg_item_amount
      FROM gold.fact_transaction_items fi
      INNER JOIN gold.fact_transactions f ON fi.transaction_sk = f.transaction_sk
      INNER JOIN gold.dim_customer c ON fi.customer_sk = c.customer_sk
      INNER JOIN gold.dim_category cat ON fi.category_sk = cat.category_sk
      INNER JOIN gold.dim_date d ON fi.date_sk = d.date_sk
      WHERE c.email = '$customerEmail'
        AND c.is_current = TRUE
        $yearFilter
      GROUP BY cat.category_name, cat.parent_category_name
      ORDER BY total_spending DESC
    """)
  }
}
```

### Usage Examples

```scala
implicit val spark: SparkSession = // ... initialized spark session

// Example 1: Get gold tier customers with high balances
val goldCustomers = QueryInterface.getCustomerAccountSummary(
  loyaltyTier = Some("GOLD"),
  minBalance = Some(10000.0)
)
goldCustomers.show()

// Example 2: Top 20 customers by transaction volume in 2025
val topCustomers = QueryInterface.getTopCustomersByVolume(
  topN = 20,
  startDate = Some("2025-01-01"),
  endDate = Some("2025-12-31")
)
topCustomers.show()

// Example 3: Spending breakdown for specific customer
val spending = QueryInterface.getCustomerSpendingByCategory(
  customerEmail = "customer123@email.com",
  year = Some(2025)
)
spending.show()
```

---

## Real-World Business Questions

### Question 1: Customer 360 View

**Business Question**: "Show me everything about customer John Smith"

**Semantic Layer Query**:
```scala
def getCustomer360(customerEmail: String)(implicit spark: SparkSession): Map[String, DataFrame] = {
  Map(
    "profile" -> spark.sql(s"""
      SELECT * FROM customer_account_summary
      WHERE email = '$customerEmail'
    """),
    
    "transactions" -> spark.sql(s"""
      SELECT 
        transaction_date,
        transaction_type,
        transaction_amount,
        account_number,
        description
      FROM customer_transaction_history
      WHERE email = '$customerEmail'
      ORDER BY transaction_date DESC
      LIMIT 100
    """),
    
    "spending_by_category" -> QueryInterface.getCustomerSpendingByCategory(customerEmail),
    
    "balances" -> spark.sql(s"""
      SELECT 
        account_number,
        product_name,
        current_balance,
        available_balance
      FROM customer_account_summary
      WHERE email = '$customerEmail'
    """)
  )
}
```

### Question 2: Monthly Transaction Trends

**Business Question**: "Show me transaction trends for the last 12 months"

**Semantic Layer Query**:
```scala
spark.sql("""
  CREATE OR REPLACE VIEW monthly_transaction_trends AS
  SELECT 
    d.year_month,
    SUM(CASE WHEN f.transaction_type = 'DEPOSIT' THEN f.transaction_amount ELSE 0 END) as total_deposits,
    SUM(CASE WHEN f.transaction_type = 'WITHDRAWAL' THEN f.transaction_amount ELSE 0 END) as total_withdrawals,
    SUM(CASE WHEN f.transaction_type = 'TRANSFER' THEN f.transaction_amount ELSE 0 END) as total_transfers,
    COUNT(DISTINCT f.transaction_sk) as transaction_count,
    COUNT(DISTINCT f.customer_sk) as active_customers
  FROM gold.fact_transactions f
  INNER JOIN gold.dim_date d ON f.date_sk = d.date_sk
  WHERE d.date_key >= ADD_MONTHS(CURRENT_DATE(), -12)
  GROUP BY d.year_month
  ORDER BY d.year_month
""")

// Usage
spark.sql("SELECT * FROM monthly_transaction_trends").show()
```

### Question 3: Product Performance

**Business Question**: "Which products are most profitable?"

**Semantic Layer Query**:
```scala
spark.sql("""
  CREATE OR REPLACE VIEW product_performance AS
  SELECT 
    p.product_name,
    p.product_type,
    COUNT(DISTINCT a.account_sk) as account_count,
    SUM(CASE WHEN a.account_status = 'ACTIVE' THEN 1 ELSE 0 END) as active_accounts,
    AVG(a.current_balance) as avg_balance,
    SUM(a.current_balance) as total_balance,
    COUNT(DISTINCT f.transaction_sk) as transaction_count,
    SUM(f.transaction_amount) as transaction_volume
  FROM gold.dim_product p
  LEFT JOIN gold.dim_account a ON p.product_sk = a.product_sk
  LEFT JOIN gold.fact_transactions f ON a.account_sk = f.account_sk
  GROUP BY p.product_name, p.product_type
  ORDER BY total_balance DESC
""")

// Usage
spark.sql("SELECT * FROM product_performance").show()
```

### Question 4: Customer Segmentation

**Business Question**: "Segment customers by behavior"

**Semantic Layer Query**:
```scala
spark.sql("""
  CREATE OR REPLACE VIEW customer_segments AS
  SELECT 
    c.customer_sk,
    c.first_name,
    c.last_name,
    c.loyalty_tier,
    COUNT(DISTINCT a.account_sk) as account_count,
    SUM(a.current_balance) as total_balance,
    COUNT(DISTINCT f.transaction_sk) as transaction_count,
    SUM(f.transaction_amount) as transaction_volume,
    CASE 
      WHEN SUM(a.current_balance) > 100000 THEN 'High Value'
      WHEN SUM(a.current_balance) > 50000 THEN 'Medium Value'
      ELSE 'Low Value'
    END as value_segment,
    CASE 
      WHEN COUNT(DISTINCT f.transaction_sk) > 100 THEN 'High Activity'
      WHEN COUNT(DISTINCT f.transaction_sk) > 50 THEN 'Medium Activity'
      ELSE 'Low Activity'
    END as activity_segment
  FROM gold.dim_customer c
  LEFT JOIN gold.dim_account a ON c.customer_sk = a.customer_sk
  LEFT JOIN gold.fact_transactions f ON a.account_sk = f.account_sk
  WHERE c.is_current = TRUE
  GROUP BY c.customer_sk, c.first_name, c.last_name, c.loyalty_tier
""")

// Usage - Find high-value, low-activity customers for re-engagement
spark.sql("""
  SELECT * FROM customer_segments
  WHERE value_segment = 'High Value'
    AND activity_segment = 'Low Activity'
""").show()
```

---

## Integration with BI Tools

### Tableau

```scala
// Export view as Tableau data source
spark.sql("SELECT * FROM customer_account_summary")
  .write
  .format("parquet")
  .mode("overwrite")
  .save("/exports/tableau/customer_account_summary")
```

### Power BI

```scala
// Create ODBC-compatible view
spark.sql("""
  CREATE OR REPLACE VIEW powerbi_customer_summary AS
  SELECT 
    CAST(customer_sk AS STRING) as customer_id,
    first_name,
    last_name,
    email,
    account_count,
    total_balance,
    loyalty_tier
  FROM customer_account_summary
""")
```

### Jupyter Notebooks

```python
# Python API for data scientists
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Use semantic layer views
customer_data = spark.sql("""
  SELECT * FROM customer_account_summary
  WHERE loyalty_tier IN ('GOLD', 'PLATINUM')
""")

# Convert to Pandas for analysis
df = customer_data.toPandas()

# Machine learning, visualization, etc.
import matplotlib.pyplot as plt
df.groupby('loyalty_tier')['total_balance'].mean().plot(kind='bar')
```

---

## Best Practices

### 1. Naming Conventions

```scala
// Good: Clear, business-friendly names
customer_account_summary
monthly_transaction_trends
customer_lifetime_value

// Bad: Technical, cryptic names
cust_acct_smry
mon_txn_trnd
clv
```

### 2. View Documentation

```scala
spark.sql("""
  CREATE OR REPLACE VIEW customer_account_summary 
  COMMENT 'Customer profile with all active accounts and current balances. 
           Updated: Daily. 
           Owner: Analytics Team. 
           Usage: Customer 360 dashboards, account analysis'
  AS
  SELECT ...
""")
```

### 3. Performance Optimization

```scala
// Materialize frequently-used views
spark.sql("""
  CREATE TABLE gold.customer_account_summary_mat
  USING iceberg
  AS SELECT * FROM customer_account_summary
""")

// Refresh nightly
spark.sql("""
  INSERT OVERWRITE gold.customer_account_summary_mat
  SELECT * FROM customer_account_summary
""")
```

### 4. Security and Access Control

```scala
// Row-level security example
spark.sql("""
  CREATE OR REPLACE VIEW customer_account_summary_secure AS
  SELECT * FROM customer_account_summary
  WHERE branch_id IN (
    SELECT branch_id FROM user_branch_access
    WHERE user_id = current_user()
  )
""")
```

---

## Benefits Summary

| Benefit | Description | Example |
|---------|-------------|---------|
| **Simplicity** | Hide complex joins | `SELECT * FROM customer_summary` vs 5-table join |
| **Consistency** | Single source of truth | All reports use same `customer_lifetime_value` calculation |
| **Reusability** | Write once, use everywhere | `customer_account_summary` view used by 10+ dashboards |
| **Maintainability** | Change in one place | Update view definition, all queries updated |
| **Performance** | Pre-computed joins | Materialized views for fast queries |
| **Governance** | Centralized access control | Row-level security in views |
| **Self-Service** | Business users can query | No SQL expertise required |

---

## Example: Complete Semantic Layer Implementation

```scala
package semantic

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SemanticLayer {
  
  // Initialize semantic layer
  def initialize(spark: SparkSession): Unit = {
    registerViews(spark)
    registerMetrics(spark)
  }
  
  // Register all business views
  private def registerViews(spark: SparkSession): Unit = {
    spark.sql("""
      CREATE OR REPLACE VIEW customer_account_summary AS
      SELECT ...
    """)
    
    spark.sql("""
      CREATE OR REPLACE VIEW monthly_transaction_trends AS
      SELECT ...
    """)
    
    spark.sql("""
      CREATE OR REPLACE VIEW customer_spending_by_category AS
      SELECT ...
    """)
    
    println("✓ Business views registered")
  }
  
  // Register metrics catalog
  private def registerMetrics(spark: SparkSession): Unit = {
    // Store metrics as catalog table
    val metrics = Seq(
      ("total_deposits", "SUM(CASE WHEN transaction_type = 'DEPOSIT' THEN amount ELSE 0 END)", "transaction"),
      ("total_withdrawals", "SUM(CASE WHEN transaction_type = 'WITHDRAWAL' THEN amount ELSE 0 END)", "transaction"),
      ("customer_lifetime_value", "SUM(transaction_amount)", "customer")
    ).toDF("metric_name", "sql_expression", "category")
    
    metrics.write
      .format("iceberg")
      .mode("overwrite")
      .save("metadata.metrics_catalog")
    
    println("✓ Metrics catalog registered")
  }
  
  // Query interface
  def query(viewName: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql(s"SELECT * FROM $viewName")
  }
}

// Usage
object Main extends App {
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Semantic Layer Demo")
    .master("local[*]")
    .getOrCreate()
  
  // Initialize semantic layer
  SemanticLayer.initialize(spark)
  
  // Query using semantic layer
  val customerSummary = SemanticLayer.query("customer_account_summary")
  customerSummary.show(10)
  
  spark.stop()
}
```

---

**Next Steps**:
1. Review the [Architecture Guide](03_architecture.md) to understand the complete data flow
2. Check [ERD Models](02_erm_models.md) for detailed table structures
3. Follow [Setup Guide](01_setup_guide.md) to implement the POC

---

**Key Takeaway**: The semantic layer is your **business translation layer** that makes technical data accessible to everyone in the organization.

