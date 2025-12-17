# Data Vault 2.0 Banking POC - Architecture Overview

## System Architecture

This document describes the complete architecture of the Banking Data Vault 2.0 POC, including data flow, technology stack, and design decisions.

---

## High-Level Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                    SEMANTIC LAYER (Query Interface)                 │
│  - Business Views                                                   │
│  - Metrics Catalog                                                  │
│  - Query Abstraction                                                │
└────────────────────────────────────────────────────────────────────┘
                              ↑
                              │
┌────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Dimensional Model)                   │
│  - Star Schema                                                      │
│  - Fact Tables: Transactions, Account Balances                      │
│  - Dimensions: Customer, Account, Product, Branch, Date, Category   │
│  - Storage: Apache Iceberg                                          │
└────────────────────────────────────────────────────────────────────┘
                              ↑
                              │ Spark ETL
                              │
┌────────────────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Business Vault)                      │
│  - Point-in-Time (PIT) Tables                                       │
│  - Bridge Tables                                                    │
│  - Reference Tables                                                 │
│  - Business Rules Applied                                           │
│  - Storage: Apache Iceberg                                          │
└────────────────────────────────────────────────────────────────────┘
                              ↑
                              │ Spark ETL
                              │
┌────────────────────────────────────────────────────────────────────┐
│                   BRONZE LAYER (Raw Vault)                          │
│  - Hubs (Business Keys)                                             │
│  - Links (Relationships)                                            │
│  - Satellites (Attributes)                                          │
│  - Immutable Historical Data                                        │
│  - Storage: Apache Iceberg                                          │
└────────────────────────────────────────────────────────────────────┘
                              ↑
                              │ CDC via NiFi
                              │
┌────────────────────────────────────────────────────────────────────┐
│                    SOURCE SYSTEM (PostgreSQL)                       │
│  - 3NF Normalized Schema                                            │
│  - OLTP Database                                                    │
│  - CDC Tracking (updated_at timestamps)                             │
│  - Business Keys for Integration                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

### Data Storage

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Source System** | PostgreSQL 12+ | Operational database (OLTP) |
| **Data Warehouse** | Apache Iceberg 1.4 | ACID table format for Bronze/Silver/Gold |
| **Metastore** | Apache Hive (Derby) | Table metadata and schema management |
| **File System** | Local File System | Development environment storage |

### Data Processing

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **ETL Engine** | Apache Spark 3.5 | Distributed data processing |
| **Programming** | Scala 2.12 | Type-safe functional programming |
| **CDC Ingestion** | Apache NiFi 1.x | Change Data Capture pipeline |
| **Build Tool** | SBT 1.9 | Dependency management and compilation |

### Why Apache Iceberg?

1. **ACID Transactions**: Guaranteed consistency for concurrent reads/writes
2. **Schema Evolution**: Add/modify columns without rewriting data
3. **Time Travel**: Query historical versions of tables
4. **Hidden Partitioning**: Automatic partition management
5. **Snapshot Isolation**: Multiple readers don't block writers
6. **Metadata Management**: Efficient pruning and filtering

---

## Data Flow Patterns

### Pattern 1: Initial Load (Full Extract)

```
Source System
     ↓
  NiFi Query
     ↓
Extract All Records
     ↓
Transform to Raw Vault
     ↓
Load Hubs → Links → Satellites
     ↓
Build Business Vault (PIT, Bridges)
     ↓
Build Dimensional Model
```

### Pattern 2: Incremental Load (CDC)

```
Source System (changes tracked via updated_at)
     ↓
  NiFi CDC Detection
     ↓
Extract Changed Records Only
     ↓
Check Hub Existence (if not exists, insert)
     ↓
Insert New Satellite Records (Type 2 SCD)
     ↓
Rebuild/Update Business Vault
     ↓
Update Dimensional Model (SCD Type 2)
```

### Pattern 3: Query Flow

```
Business User Query
     ↓
Semantic Layer (abstracts complexity)
     ↓
Gold Layer (Star Schema - fast)
     ↓
Results Returned
```

---

## Layer Details

### Bronze Layer: Raw Vault

**Purpose**: Immutable storage of all source data with full history

**Design Principles**:
- Insert-only (no updates or deletes)
- Hash keys for performance
- Business keys preserved
- Full audit trail (load timestamps, source system)

**Table Types**:

1. **Hubs** - Store business keys
   - `hub_customer_hk` (PK) = MD5(customer_number)
   - `customer_number` (Business Key)
   - `load_date`, `record_source`

2. **Links** - Store relationships
   - `link_customer_account_hk` (PK) = MD5(hub_customer_hk || hub_account_hk)
   - `hub_customer_hk` (FK)
   - `hub_account_hk` (FK)
   - `load_date`, `record_source`

3. **Satellites** - Store descriptive attributes
   - `hub_customer_hk` (PK, FK)
   - `load_date` (PK) - Creates versions
   - `first_name`, `last_name`, `email`, etc.
   - `hash_diff` = MD5(all attributes) - For change detection

**Storage Configuration**:
```scala
// Iceberg table properties for Raw Vault
format-version = 2
write.format.default = parquet
write.parquet.compression-codec = snappy
write.metadata.compression-codec = gzip
write.metadata.metrics.default = full
```

### Silver Layer: Business Vault

**Purpose**: Analytics-ready constructs with business logic applied

**Design Principles**:
- Derived from Raw Vault
- Pre-computed joins for performance
- Business rules and calculations
- Still auditable back to Raw Vault

**Table Types**:

1. **PIT (Point-in-Time) Tables**
   - Daily snapshots of entity states
   - Efficiently answers "as-of" queries
   - Example: "What did customer look like on 2025-01-15?"

2. **Bridge Tables**
   - Pre-joined many-to-many relationships
   - Denormalized for performance
   - Example: Customer with all their accounts and balances

3. **Reference Tables**
   - Business hierarchies (product families, category trees)
   - Calculated classifications
   - Lookup tables for enrichment

**Rebuild Strategy**:
- PIT tables: Full rebuild daily (snapshot-based)
- Bridges: Full rebuild or incremental merge
- Reference: Full rebuild when source changes

### Gold Layer: Dimensional Model

**Purpose**: Business-friendly star schema for BI and analytics

**Design Principles**:
- Denormalized for query performance
- Business-friendly naming
- Type 2 SCD for dimension history
- Surrogate keys for dimension tables

**Table Types**:

1. **Fact Tables**
   - `fact_transactions`: One row per transaction
   - `fact_transaction_items`: One row per transaction item
   - `fact_account_balance_daily`: Daily balance snapshots
   - Measures: Amounts, counts, balances

2. **Dimension Tables**
   - `dim_customer`: Customer master (Type 2 SCD)
   - `dim_account`: Account master
   - `dim_product`: Product catalog
   - `dim_branch`: Branch locations
   - `dim_date`: Calendar dimension
   - `dim_category`: Transaction categories

**Surrogate Key Generation**:
```scala
// Monotonically increasing ID for dimensions
dim_customer.customer_sk = monotonically_increasing_id()

// Natural key preserved for traceability
dim_customer.customer_bk = hub_customer_hk
```

### Semantic Layer

**Purpose**: Business-friendly query interface hiding technical complexity

**Components**:

1. **Business Views**
   - Pre-defined joins for common queries
   - Example: `customer_account_summary`
   - Hides star schema complexity

2. **Metrics Catalog**
   - Calculated measures with business logic
   - Example: `customer_lifetime_value`, `average_account_balance`
   - Consistent definitions across organization

3. **Query Interface**
   - Scala object with parameterized query methods
   - SQL views registered in Spark catalog
   - Type-safe result sets

---

## Hash Key Strategy

### Why Hash Keys?

1. **Performance**: Fixed-length keys (32 chars) vs variable-length business keys
2. **Composite Keys**: Single hash for multi-column business keys
3. **Confidentiality**: Hash conceals sensitive business keys
4. **Consistency**: Same business key always produces same hash

### Hash Key Generation

```scala
import org.apache.spark.sql.functions._

// Hub hash key (single business key)
val hub_customer_hk = md5(col("customer_number").cast("string"))

// Link hash key (multiple hub keys)
val link_customer_account_hk = md5(
  concat_ws("||", col("hub_customer_hk"), col("hub_account_hk"))
)

// Satellite hash diff (all attributes)
val hash_diff = md5(
  concat_ws("|",
    col("first_name"), col("last_name"), col("email"),
    col("phone"), col("date_of_birth"), col("ssn"),
    col("loyalty_tier"), col("preferred_contact_method")
  )
)
```

### Hash Collision Risk

MD5 produces 128-bit hash (2^128 possible values):
- **Collision probability**: Negligible for typical datasets
- **Birthday paradox**: Would need ~2^64 records for 50% collision chance
- **Mitigation**: Use SHA-256 if collision concerns exist

---

## CDC (Change Data Capture) Strategy

### Timestamp-Based CDC

**Implementation**:
```sql
-- Source system tracks changes via updated_at
CREATE TRIGGER update_customer_updated_at 
BEFORE UPDATE ON customer
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- NiFi query extracts changes
SELECT * FROM customer 
WHERE updated_at > ${last_run_timestamp}
ORDER BY updated_at;
```

**Advantages**:
- Simple to implement
- Works with any database
- No additional infrastructure

**Disadvantages**:
- Deletes not captured (soft delete workaround needed)
- Schema changes require trigger updates
- Clock skew issues in distributed systems

### Alternative: Transaction Log CDC

**Tools**: Debezium, Maxwell, AWS DMS

**Advantages**:
- Captures deletes
- No source system modification
- Near real-time

**Disadvantages**:
- More complex setup
- Database-specific implementation
- Higher operational overhead

---

## Schema Evolution Handling

### Scenario: Adding New Column to Source

```sql
-- Day 1: Source system schema change
ALTER TABLE customer ADD COLUMN loyalty_tier VARCHAR(20);
```

### Impact by Layer

**Bronze Layer (Raw Vault)**:
```scala
// Automatic schema merging in Spark + Iceberg
val newData = sourceDF.select(
  col("customer_number"),
  col("first_name"),
  col("last_name"),
  col("email"),
  col("loyalty_tier")  // New column automatically included
)

// Iceberg handles schema evolution
newData.writeTo("bronze.sat_customer")
  .option("merge-schema", "true")
  .append()

// Result: New column added to satellite
// Old records have NULL for loyalty_tier
// New records have actual values
```

**Silver Layer (Business Vault)**:
```scala
// PIT table rebuild includes new column
val pitCustomer = hubCustomer
  .join(satCustomer, "hub_customer_hk")
  .select(
    col("hub_customer_hk"),
    col("snapshot_date"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("loyalty_tier")  // New column
  )

// Bridges automatically include new attributes
```

**Gold Layer (Dimensional Model)**:
```scala
// Controlled migration - add when business is ready
// Option 1: Add immediately
spark.sql("""
  ALTER TABLE dim_customer 
  ADD COLUMN loyalty_tier VARCHAR(20)
""")

// Option 2: Wait for next release
// Existing dashboards continue working
```

### Key Benefit: No Breaking Changes

- **Raw Vault**: Captures everything automatically
- **Business Vault**: Rebuilds include new data
- **Dimensional Model**: Changes on your schedule
- **Reports**: Keep working until you're ready to use new field

---

## Performance Optimization

### Iceberg Partitioning

```scala
// Partition fact tables by date for query performance
spark.sql("""
  CREATE TABLE gold.fact_transactions (...)
  USING iceberg
  PARTITIONED BY (days(transaction_date))
""")

// Query with partition pruning
spark.sql("""
  SELECT * FROM gold.fact_transactions
  WHERE transaction_date >= '2025-01-01'  -- Only scans relevant partitions
""")
```

### PIT Table Optimization

```scala
// Partition PIT by snapshot_date
spark.sql("""
  CREATE TABLE silver.pit_customer (...)
  USING iceberg
  PARTITIONED BY (snapshot_date)
""")

// Query specific date = single partition scan
spark.sql("""
  SELECT * FROM silver.pit_customer
  WHERE snapshot_date = '2025-01-15'  -- Fast!
""")
```

### Bridge Table Materialization

```scala
// Pre-compute joins to avoid runtime overhead
val bridge = customer
  .join(account, "customer_id")
  .join(product, "product_id")
  .join(branch, "branch_id")
  .select(/* all needed columns */)

// Query bridge directly (no joins needed)
bridge.filter("customer_id = 123").show()
```

---

## Monitoring and Observability

### Data Quality Checks

```scala
// Check for null business keys in Hubs
val nullKeys = spark.sql("""
  SELECT COUNT(*) FROM bronze.hub_customer
  WHERE customer_number IS NULL
""")

// Check for hash collisions (should be 0)
val collisions = spark.sql("""
  SELECT hub_customer_hk, COUNT(*) as cnt
  FROM bronze.hub_customer
  GROUP BY hub_customer_hk
  HAVING COUNT(*) > 1
""")

// Check satellite version counts
val versions = spark.sql("""
  SELECT hub_customer_hk, COUNT(*) as version_count
  FROM bronze.sat_customer
  GROUP BY hub_customer_hk
  ORDER BY version_count DESC
  LIMIT 10
""")
```

### ETL Metrics

```scala
// Track load statistics
case class LoadStats(
  layer: String,
  table: String,
  recordsProcessed: Long,
  recordsInserted: Long,
  startTime: Timestamp,
  endTime: Timestamp,
  durationSeconds: Long,
  status: String
)

// Log to monitoring table
loadStats.write
  .format("iceberg")
  .mode("append")
  .save("metadata.etl_load_stats")
```

---

## Deployment Architecture

### Local Development

```
├── PostgreSQL (localhost:5432)
│   └── banking_source database
├── Spark Local Mode
│   └── All processing in JVM
├── Iceberg Tables
│   └── Local file system (C:/warehouse)
└── Hive Metastore
    └── Derby embedded database
```

### Production (Example)

```
├── PostgreSQL (RDS/CloudSQL)
│   └── Source system
├── Apache NiFi Cluster
│   └── CDC ingestion
├── Spark on Kubernetes/EMR/Databricks
│   └── Distributed processing
├── Iceberg Tables
│   └── S3/GCS/ADLS (object storage)
└── Hive Metastore
    └── PostgreSQL/MySQL external metastore
```

---

## Data Governance

### Auditability

Every record tracks:
- **load_date**: When data was loaded
- **record_source**: Which source system
- **hash_diff**: What changed (satellites only)

### Traceability

```sql
-- Trace dimensional record back to source
SELECT 
    d.customer_sk,           -- Gold layer surrogate key
    d.customer_bk,           -- Business Vault hash key
    h.customer_number,       -- Bronze layer business key
    -- Source system query:
    -- SELECT * FROM customer WHERE customer_number = 'CUS-000123'
FROM gold.dim_customer d
JOIN bronze.hub_customer h ON d.customer_bk = h.hub_customer_hk
WHERE d.customer_sk = 12345;
```

### Compliance (GDPR, CCPA)

```sql
-- Right to be forgotten: Delete all customer data
-- Step 1: Soft delete in Gold layer
UPDATE gold.dim_customer 
SET is_current = FALSE, expiry_date = CURRENT_DATE
WHERE customer_bk = 'ABC123';

-- Step 2: Raw Vault retains for audit (with PII masked)
-- Satellites can be expired or encrypted
```

---

## Next Steps

1. **Review**: [ERD Models](02_erm_models.md) for detailed table structures
2. **Implement**: [Setup Guide](01_setup_guide.md) to run the POC
3. **Query**: [Semantic Layer Guide](04_semantic_layer.md) for business queries

---

**Questions?**
- Data Vault modeling: See [ERD Models](02_erm_models.md)
- Setup issues: See [Setup Guide](01_setup_guide.md)
- Query examples: See [Semantic Layer Guide](04_semantic_layer.md)

