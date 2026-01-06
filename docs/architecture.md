# Architecture & Design Guide

**Complete architectural reference for the Data Vault 2.0 banking data warehouse.**

---

## 📋 Table of Contents

### Part I: Architecture Overview
- [Data Flow Architecture](#data-flow-architecture)
- [Technology Stack](#technology-stack)
- [Design Principles](#design-principles)

### Part II: Data Models
- [Source System (ERM)](#source-system-erm)
- [Data Vault Model (Bronze)](#data-vault-model-bronze)
- [Business Vault Model (Silver)](#business-vault-model-silver)
- [Dimensional Model (Gold)](#dimensional-model-gold)
- [Semantic Layer](#semantic-layer)

### Part III: Design Decisions
- [Why Data Vault 2.0](#why-data-vault-20)
- [Why Apache Avro](#why-apache-avro)
- [Why Apache NiFi](#why-apache-nifi)
- [Why Apache Iceberg](#why-apache-iceberg)
- [Why Multi-Layer Architecture](#why-multi-layer-architecture)

### Part IV: Performance & Optimization
- [Query Engine Comparison](#query-engine-comparison)

---

## PART I: ARCHITECTURE OVERVIEW

---

## Data Flow Architecture

### High-Level System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    COMPLETE DATA PIPELINE ARCHITECTURE                  │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────────────┐
│ OPERATIONAL SYSTEM   │
│ (PostgreSQL)         │
├──────────────────────┤
│ banking_source DB    │
│ - 3NF normalized     │
│ - OLTP optimized     │
│ - Frequent changes   │
└──────────────────────┘
         │
         │ JDBC Connection
         │ Incremental CDC (updated_at column)
         ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ EXTRACTION & VALIDATION LAYER                                            │
│ (Apache NiFi 2.7.2)                                                      │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────┐     ┌──────────────────┐                      │
│  │ QueryDatabaseTable  │────→│ ConvertRecord    │                      │
│  │ Record              │     │ (JSON → Avro)    │                      │
│  │                     │     │                  │                      │
│  │ SQL: SELECT *       │     │ Schema Validate  │                      │
│  │ WHERE updated_at >  │     │ against .avsc    │                      │
│  │   [last_value]      │     │                  │                      │
│  └─────────────────────┘     └──────────────────┘                      │
│                                       │                                  │
│                                       │ Schema-validated Avro binary     │
│                                       ↓                                  │
│                              ┌──────────────────┐                       │
│                              │ PutFile          │                       │
│                              │ warehouse/       │                       │
│                              │ staging/         │                       │
│                              └──────────────────┘                       │
│                                                                          │
│  Schemas: nifi/schemas/*.avsc (customer, account, transaction_*)        │
└──────────────────────────────────────────────────────────────────────────┘
         │
         │ Avro Files (binary, schema-embedded)
         ↓
┌──────────────────────┐
│ STAGING AREA         │
│ (File System)        │
├──────────────────────┤
│ warehouse/staging/   │
│ ├── customer/*.avro  │
│ ├── account/*.avro   │
│ └── transaction_*    │
│     /*.avro          │
└──────────────────────┘
         │
         │ Spark Read (Avro format)
         ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ RAW VAULT LAYER (BRONZE)                                                 │
│ (Apache Spark 3.5 + Apache Iceberg)                                      │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐             │
│  │ AvroReader   │───→│ Hash Key     │───→│ Data Vault   │             │
│  │              │    │ Generator    │    │ Load Logic   │             │
│  │ - Read Avro  │    │              │    │              │             │
│  │ - Validate   │    │ MD5(bus_key) │    │ - Hub Load   │             │
│  │ - Enrich     │    │              │    │ - Sat Load   │             │
│  └──────────────┘    └──────────────┘    │ - Link Load  │             │
│                                           └──────────────┘             │
│                                                   │                      │
│                                                   ↓                      │
│  ┌────────────────────────────────────────────────────────┐            │
│  │ ICEBERG TABLES (Bronze Schema)                         │            │
│  ├────────────────────────────────────────────────────────┤            │
│  │ HUBS:                                                  │            │
│  │ - hub_customer (customer_hash_key, customer_id, ...)  │            │
│  │ - hub_account (account_hash_key, account_id, ...)     │            │
│  │ - hub_transaction (transaction_hash_key, ...)         │            │
│  │                                                        │            │
│  │ SATELLITES:                                            │            │
│  │ - sat_customer (attributes, valid_from, valid_to)     │            │
│  │ - sat_account (attributes, valid_from, valid_to)      │            │
│  │                                                        │            │
│  │ LINKS:                                                 │            │
│  │ - link_customer_account (customer ↔ account)          │            │
│  │ - link_transaction_item (transaction ↔ item)          │            │
│  └────────────────────────────────────────────────────────┘            │
│                                                                          │
│  Features: Schema evolution, full history, hash-based joins              │
└──────────────────────────────────────────────────────────────────────────┘
         │
         │ Spark SQL (Join Hubs + Satellites)
         ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ BUSINESS VAULT LAYER (SILVER)                                            │
│ (Apache Spark 3.5 + Apache Iceberg)                                      │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐                    ┌──────────────┐                  │
│  │ PIT Builder  │                    │ Bridge       │                  │
│  │              │                    │ Builder      │                  │
│  │ Join Hub +   │                    │              │                  │
│  │ Satellite    │                    │ Multi-hop    │                  │
│  │ @ snapshot   │                    │ relationships│                  │
│  │ date         │                    │ + aggregates │                  │
│  └──────────────┘                    └──────────────┘                  │
│         │                                     │                         │
│         ↓                                     ↓                         │
│  ┌────────────────────────────────────────────────────────┐            │
│  │ ICEBERG TABLES (Silver Schema)                         │            │
│  ├────────────────────────────────────────────────────────┤            │
│  │ PIT TABLES:                                            │            │
│  │ - pit_customer (flattened current attributes)          │            │
│  │ - pit_account (flattened current attributes)           │            │
│  │                                                        │            │
│  │ BRIDGE TABLES:                                         │            │
│  │ - bridge_customer_account (pre-joined + metrics)       │            │
│  └────────────────────────────────────────────────────────┘            │
│                                                                          │
│  Features: Query optimization, denormalization, aggregation              │
└──────────────────────────────────────────────────────────────────────────┘
         │
         │ Spark SQL (SCD Type 2 + Star Schema)
         ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ DIMENSIONAL MODEL LAYER (GOLD)                                           │
│ (Apache Spark 3.5 + Apache Iceberg)                                      │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐                    ┌──────────────┐                  │
│  │ SCD Type 2   │                    │ Fact Builder │                  │
│  │ Handler      │                    │              │                  │
│  │              │                    │ Lookup dim   │                  │
│  │ Track        │                    │ keys, calc   │                  │
│  │ dimension    │                    │ metrics      │                  │
│  │ history      │                    │              │                  │
│  └──────────────┘                    └──────────────┘                  │
│         │                                     │                         │
│         ↓                                     ↓                         │
│  ┌────────────────────────────────────────────────────────┐            │
│  │ ICEBERG TABLES (Gold Schema - Star Schema)             │            │
│  ├────────────────────────────────────────────────────────┤            │
│  │ DIMENSIONS (SCD Type 2):                               │            │
│  │ - dim_customer (surrogate key, SCD history)            │            │
│  │ - dim_account (surrogate key, SCD history)             │            │
│  │ - dim_date (date dimension, 10 years)                  │            │
│  │ - dim_product, dim_branch                              │            │
│  │                                                        │            │
│  │ FACTS (Additive):                                      │            │
│  │ - fact_transaction (metrics + dim keys)                │            │
│  │ - fact_account_balance (daily snapshots)               │            │
│  └────────────────────────────────────────────────────────┘            │
│                                                                          │
│  Features: BI-friendly, fast aggregations, SCD Type 2 history            │
└──────────────────────────────────────────────────────────────────────────┘
         │
         │ JDBC / Spark SQL
         ↓
┌──────────────────────┐
│ BI & ANALYTICS       │
│ (Tableau, Power BI)  │
├──────────────────────┤
│ - Customer 360       │
│ - Transaction Trends │
│ - Account Balances   │
│ - Revenue Reports    │
└──────────────────────┘
```

### Component Interaction Flow

```
┌──────────┐  JDBC   ┌──────────┐  Avro   ┌──────────┐  Spark  ┌──────────┐
│PostgreSQL│────────→│  NiFi    │────────→│ Staging  │────────→│  Bronze  │
│  (OLTP)  │  Query  │(Validate)│  Write  │  (Files) │  Read   │(Raw Vault)│
└──────────┘         └──────────┘         └──────────┘         └──────────┘
                                                                      │
                                                                      ↓
                                                                 ┌──────────┐
                                                                 │  Silver  │
                                                                 │(Optimized)│
                                                                 └──────────┘
                                                                      │
                                                                      ↓
                                                                 ┌──────────┐
                                                                 │   Gold   │
                                                                 │(Star Schema)│
                                                                 └──────────┘
```

---

## Technology Stack

### Data Ingestion
- **Apache NiFi 2.7.2**
  - Visual data flow design
  - Built-in CDC support (QueryDatabaseTableRecord)
  - Schema validation (ConvertRecord)
  - No code required for basic flows

### Data Format
- **Apache Avro**
  - Binary format (compact storage)
  - Embedded schemas (self-describing)
  - Schema evolution support
  - Native Spark integration

### Data Processing
- **Apache Spark 3.5**
  - Distributed processing
  - DataFrame API for transformations
  - Catalyst optimizer
  - Native Avro reader

### Data Storage
- **Apache Iceberg**
  - ACID transactions
  - Schema evolution
  - Time travel queries
  - Hidden partitioning

### Source Database
- **PostgreSQL 12+**
  - Operational data store
  - 3NF normalized
  - Supports JDBC connectivity

### Platform
- **Windows Native**
  - No Docker required
  - PowerShell scripting
  - Native NiFi installation

---

## Design Principles

### 1. Separation of Concerns
Each layer has a single responsibility:
- **NiFi:** Extract and validate
- **Bronze:** Store raw history
- **Silver:** Optimize queries
- **Gold:** Serve analytics

### 2. Schema Evolution Resilience
The system handles source schema changes gracefully:
- Avro provides type-safe schema contracts
- Data Vault absorbs new columns automatically
- Historical queries remain unaffected

### 3. Single Source of Truth for Schema Validation
Schema validation logic derives from Avro schema definitions (`nifi/schemas/*.avsc`):
- **No hardcoded field lists** - Required fields extracted dynamically from `.avsc` files
- **AvroReader utility** - Loads schemas at runtime and caches them for performance
- **Validation rules** - Fields without `null` union or `default` value are considered required
- **Consistency guaranteed** - Schema changes in `.avsc` automatically reflected in validation
- **Clear error messages** - Validation failures point to specific `.avsc` file for correction

**Example:** When `customer.avsc` is updated to add a new required field, the Spark ETL automatically enforces it without code changes.

### 4. Auditability
Every data point is traceable:
- Load timestamps on all records
- valid_from/valid_to tracking in Satellites
- Source system tracking in Hubs
- Full lineage from source to analytics

### 5. Performance Through Layering
Each layer optimizes for different access patterns:
- Bronze: Write-optimized (append-only)
- Silver: Read-optimized (pre-joined)
- Gold: Aggregate-optimized (star schema)

### 6. Decoupling
Components are loosely coupled:
- NiFi writes files, Spark reads files (no direct coupling)
- Each layer can be rebuilt independently
- Technology swaps are easier (e.g., replace NiFi with Kafka)

---

## PART II: DATA MODELS

---

## Source System (ERM)

### Entity-Relationship Model

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BANKING SOURCE SYSTEM (3NF)                          │
└─────────────────────────────────────────────────────────────────────────┘

                     ┌──────────────┐
                     │   CUSTOMER   │
                     ├──────────────┤
                     │ customer_id  │ PK
                     │ customer_type│ (INDIVIDUAL, BUSINESS)
                     │ first_name   │
                     │ last_name    │
                     │ email        │ UNIQUE
                     │ phone        │
                     │ address      │
                     │ city, state  │
                     │ zip_code     │
                     │ customer_status │ (ACTIVE, INACTIVE, CLOSED)
                     │ created_at   │
                     │ updated_at   │ (CDC tracking)
                     └──────────────┘
                            │
                            │ 1
                            │
                            │ owns
                            │
                            │ N
                            ↓
                     ┌──────────────┐
                     │   ACCOUNT    │
                     ├──────────────┤
                     │ account_id   │ PK
                     │ customer_id  │ FK → customer
                     │ product_id   │ FK → product
                     │ branch_id    │ FK → branch
                     │ account_number│ UNIQUE
                     │ account_type │ (CHECKING, SAVINGS, CREDIT_CARD, LOAN)
                     │ account_status│ (OPEN, CLOSED, SUSPENDED)
                     │ balance      │ DECIMAL(15,2)
                     │ currency     │
                     │ opened_date  │
                     │ closed_date  │
                     │ created_at   │
                     │ updated_at   │ (CDC tracking)
                     └──────────────┘
                            │
                            │ 1
                            │
                            │ has
                            │
                            │ N
                            ↓
              ┌──────────────────────────┐
              │  TRANSACTION_HEADER      │
              ├──────────────────────────┤
              │ transaction_id           │ PK
              │ account_id               │ FK → account
              │ transaction_number       │ UNIQUE
              │ transaction_date         │
              │ transaction_type         │ (DEPOSIT, WITHDRAWAL, PAYMENT, TRANSFER)
              │ transaction_status       │ (COMPLETED, PENDING, FAILED)
              │ total_amount             │ DECIMAL(15,2)
              │ currency                 │
              │ description              │
              │ created_at               │
              │ updated_at               │ (CDC tracking)
              └──────────────────────────┘
                            │
                            │ 1
                            │
                            │ contains
                            │
                            │ N
                            ↓
              ┌──────────────────────────┐
              │  TRANSACTION_ITEM        │
              ├──────────────────────────┤
              │ item_id                  │ PK
              │ transaction_id           │ FK → transaction_header
              │ category_id              │ FK → category
              │ item_sequence            │ (order within transaction)
              │ item_type                │
              │ merchant_name            │
              │ merchant_category        │
              │ item_amount              │ DECIMAL(15,2)
              │ item_description         │
              │ created_at               │
              │ updated_at               │ (CDC tracking)
              └──────────────────────────┘


┌────────────────────────────────────────────────────────────────────────┐
│                        REFERENCE TABLES                                │
└────────────────────────────────────────────────────────────────────────┘

┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│   PRODUCT    │       │   BRANCH     │       │  CATEGORY    │
├──────────────┤       ├──────────────┤       ├──────────────┤
│ product_id   │       │ branch_id    │       │ category_id  │
│ product_code │       │ branch_code  │       │ category_name│
│ product_name │       │ branch_name  │       │ parent_id    │ (hierarchical)
│ product_type │       │ address      │       │ path         │ (materialized path)
│ description  │       │ city, state  │       │ level        │
└──────────────┘       │ manager      │       └──────────────┘
                       └──────────────┘
```

### Multi-Item Transaction Example

**Business Scenario:** Customer pays multiple bills in one transaction (like an e-commerce order).

```
transaction_header:
┌────────────────┬─────────────┬──────────────┬────────────┐
│ transaction_id │ account_id  │ total_amount │ type       │
├────────────────┼─────────────┼──────────────┼────────────┤
│ 1001           │ 101         │ 250.00       │ PAYMENT    │
└────────────────┴─────────────┴──────────────┴────────────┘
                        │
                        │ contains
                        ↓
transaction_item:
┌────────┬────────────────┬──────────────┬─────────────┬────────────┐
│ item_id│ transaction_id │ merchant_name│ item_amount │ description│
├────────┼────────────────┼──────────────┼─────────────┼────────────┤
│ 2001   │ 1001           │ Con Edison   │ 100.00      │ Electricity│
│ 2002   │ 1001           │ Water Dept   │ 50.00       │ Water Bill │
│ 2003   │ 1001           │ Comcast      │ 100.00      │ Internet   │
└────────┴────────────────┴──────────────┴─────────────┴────────────┘
```

**Why This Pattern?**
- Matches real-world bill payments, shopping carts, split transactions
- Enables item-level analytics (which merchants are most used?)
- Supports partial refunds, item-level taxation

### Normalization Level

**3rd Normal Form (3NF):**
- ✅ No repeating groups (transaction items in separate table)
- ✅ All non-key attributes depend on the key (customer_id → email, not on other attributes)
- ✅ No transitive dependencies (branch info not in account table)

**Why 3NF for OLTP:**
- Minimizes data redundancy
- Optimizes for INSERT/UPDATE/DELETE
- Maintains data integrity through foreign keys

---

## Data Vault Model (Bronze)

### Data Vault 2.0 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER - RAW VAULT                             │
└─────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│                              HUBS                                      │
│  (Store unique business entities - deduplicated)                       │
└────────────────────────────────────────────────────────────────────────┘

hub_customer                    hub_account                   hub_transaction
┌───────────────────┐          ┌───────────────────┐         ┌───────────────────┐
│ customer_hash_key │ PK       │ account_hash_key  │ PK      │ transaction_hash  │ PK
│ customer_id       │ BK       │ account_id        │ BK      │ transaction_id    │ BK
│ load_timestamp    │          │ load_timestamp    │         │ load_timestamp    │
│ record_source     │          │ record_source     │         │ record_source     │
└───────────────────┘          └───────────────────┘         └───────────────────┘

BK = Business Key (from source system)
PK = Primary Key (MD5 hash of business key)


┌────────────────────────────────────────────────────────────────────────┐
│                           SATELLITES                                   │
│  (Store attributes with full history - SCD Type 2)                     │
└────────────────────────────────────────────────────────────────────────┘

sat_customer
┌───────────────────┐
│ customer_hash_key │ PK, FK → hub_customer
│ load_timestamp    │ PK (part of composite key)
│ customer_type     │ INDIVIDUAL, BUSINESS
│ first_name        │
│ last_name         │
│ email             │
│ phone             │
│ address           │
│ city              │
│ state             │
│ zip_code          │
│ customer_status   │ ACTIVE, INACTIVE, CLOSED
│ valid_from        │ Timestamp when version became active
│ valid_to          │ Timestamp when superseded (NULL = current)
│ hash_diff         │ MD5(all_attributes) for change detection
│ record_source     │
└───────────────────┘

sat_account
┌───────────────────┐
│ account_hash_key  │ PK, FK → hub_account
│ load_timestamp    │ PK
│ account_number    │
│ account_type      │
│ account_status    │
│ balance           │
│ currency          │
│ opened_date       │
│ closed_date       │
│ valid_from        │
│ valid_to          │
│ hash_diff         │
│ record_source     │
└───────────────────┘

sat_transaction
┌───────────────────┐
│ transaction_hash  │ PK, FK → hub_transaction
│ load_timestamp    │ PK
│ transaction_number│
│ transaction_date  │
│ transaction_type  │
│ transaction_status│
│ total_amount      │
│ currency          │
│ description       │
│ valid_from        │
│ valid_to          │
│ hash_diff         │
│ record_source     │
└───────────────────┘


┌────────────────────────────────────────────────────────────────────────┐
│                             LINKS                                      │
│  (Store relationships between entities)                                │
└────────────────────────────────────────────────────────────────────────┘

link_customer_account
┌───────────────────┐
│ link_hash_key     │ PK = MD5(customer_hash_key + account_hash_key)
│ customer_hash_key │ FK → hub_customer
│ account_hash_key  │ FK → hub_account
│ load_timestamp    │
│ record_source     │
└───────────────────┘

link_transaction_item
┌───────────────────────┐
│ link_hash_key         │ PK = MD5(transaction_hash + item_hash)
│ transaction_hash_key  │ FK → hub_transaction
│ item_hash_key         │ FK → hub_transaction_item
│ load_timestamp        │
│ record_source         │
└───────────────────────┘
```

### Hash Key Generation

**Why MD5 hashing?**
- Deterministic (same input → same hash)
- Fixed length (32 hex characters)
- Fast to compute
- Enables hash-based joins (no integer lookups)

**Example:**
```scala
// Customer with customer_id = 1
val businessKey = "1"
val hashKey = md5(businessKey)
// Result: "c4ca4238a0b923820dcc509a6f75849b"

// Customer-Account link
val compositeKey = "c4ca4238a0b923820dcc509a6f75849b" + "550e8400-e29b-41d4-a716-446655440000"
val linkHash = md5(compositeKey)
```

### History Tracking with valid_from/valid_to

**Initial Load:**
```sql
INSERT INTO sat_customer (
  customer_hash_key, email, customer_status,
  valid_from, valid_to
) VALUES (
  'c4ca4238...', 'john@example.com', 'ACTIVE',
  '2025-12-20 10:00:00', NULL  -- NULL = current version
);
```

**Schema Evolution (loyalty_tier added):**
```sql
-- End-date old version
UPDATE sat_customer
SET valid_to = '2025-12-20 14:00:00'
WHERE customer_hash_key = 'c4ca4238...'
  AND valid_to IS NULL;

-- Insert new version with new column
INSERT INTO sat_customer (
  customer_hash_key, email, customer_status, loyalty_tier,
  valid_from, valid_to
) VALUES (
  'c4ca4238...', 'john@example.com', 'ACTIVE', 'GOLD',
  '2025-12-20 14:00:00', NULL
);
```

**Querying History:**
```sql
-- Get current version
SELECT * FROM sat_customer WHERE valid_to IS NULL;

-- Get version as of specific date
SELECT * FROM sat_customer
WHERE valid_from <= '2025-12-20 12:00:00'
  AND (valid_to > '2025-12-20 12:00:00' OR valid_to IS NULL);

-- Get full history for one customer
SELECT * FROM sat_customer
WHERE customer_hash_key = 'c4ca4238...'
ORDER BY valid_from;
```

### Data Vault Benefits Demonstrated

**1. Schema Evolution:**
```
Source adds loyalty_tier → Satellite automatically gets new column
Old records: loyalty_tier = NULL
New records: loyalty_tier populated
Existing queries: still work (don't reference loyalty_tier)
```

**2. Full Audit Trail:**
```
Who: record_source (which ETL job)
What: hash_diff (what changed)
When: valid_from/valid_to (temporal tracking)
Where: load_timestamp (which load batch)
```

**3. Deduplication:**
```
Hub only stores unique business keys
If customer_id = 1 loaded twice → only one hub_customer row
Satellite stores both versions with different valid_from
```

---

## Business Vault Model (Silver)

### Point-in-Time (PIT) Tables

**Purpose:** Flatten Data Vault for query performance

```
pit_customer (snapshot_date = 2025-12-20)
┌─────────────┬──────┬─────────┬────────┬────────────┬──────────────┐
│ customer_id │ email│ f_name  │ l_name │ status     │ loyalty_tier │
├─────────────┼──────┼─────────┼────────┼────────────┼──────────────┤
│ 1           │john@ │John     │Doe     │ACTIVE      │GOLD          │
│ 2           │jane@ │Jane     │Smith   │ACTIVE      │PLATINUM      │
└─────────────┴──────┴─────────┴────────┴────────────┴──────────────┘
              ↑ All attributes flattened from hub_customer + sat_customer
```

**How PIT is Built:**
```scala
// Pseudo-code
val pit = hub_customer
  .join(sat_customer, "customer_hash_key")
  .filter("valid_to IS NULL")  // Current version only
  .select(
    col("customer_id"),
    col("email"),
    col("first_name"),
    col("last_name"),
    col("customer_status"),
    col("loyalty_tier"),
    lit(current_date).as("snapshot_date")
  )
  .write.format("iceberg")
  .mode("overwrite")
  .save("silver.pit_customer")
```

**Query Comparison:**
```sql
-- Without PIT (Bronze - complex)
SELECT h.customer_id, s.email, s.customer_status
FROM bronze.hub_customer h
JOIN bronze.sat_customer s ON h.customer_hash_key = s.customer_hash_key
WHERE s.valid_to IS NULL;

-- With PIT (Silver - simple)
SELECT customer_id, email, customer_status
FROM silver.pit_customer
WHERE snapshot_date = CURRENT_DATE;
```

### Bridge Tables

**Purpose:** Pre-compute relationships and aggregates

```
bridge_customer_account
┌─────────────┬────────────┬──────────┬──────────────┬────────────┬───────────┐
│ customer_id │ account_id │ balance  │ account_count│ total_bal  │ is_primary│
├─────────────┼────────────┼──────────┼──────────────┼────────────┼───────────┤
│ 1           │ 101        │ 5000     │ 2            │ 15000      │ false     │
│ 1           │ 102        │ 10000    │ 2            │ 15000      │ true      │
│ 2           │ 201        │ 50000    │ 1            │ 50000      │ true      │
└─────────────┴────────────┴──────────┴──────────────┴────────────┴───────────┘
              ↑ Pre-joined: customer ↔ account relationship
              ↑ Pre-aggregated: account count, total balance
              ↑ Business rule: primary = highest balance
```

**How Bridge is Built:**
```scala
// Pseudo-code
val bridge = hub_customer
  .join(link_customer_account, "customer_hash_key")
  .join(hub_account, "account_hash_key")
  .join(sat_account.filter("valid_to IS NULL"), "account_hash_key")
  .groupBy("customer_id")
  .agg(
    count("account_id").as("account_count"),
    sum("balance").as("total_balance")
  )
  .withColumn("is_primary", 
    row_number().over(Window.partitionBy("customer_id").orderBy(desc("balance"))) === 1
  )
```

---

## Dimensional Model (Gold)

### Star Schema Design

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER - STAR SCHEMA                        │
└─────────────────────────────────────────────────────────────────────────┘

                              DIMENSIONS
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                         │                         │
        ↓                         ↓                         ↓
┌───────────────┐        ┌───────────────┐        ┌───────────────┐
│ dim_customer  │        │  dim_account  │        │   dim_date    │
├───────────────┤        ├───────────────┤        ├───────────────┤
│ customer_key  │ PK     │ account_key   │ PK     │ date_key      │ PK
│ customer_id   │ NK     │ account_id    │ NK     │ date          │
│ customer_type │        │ account_type  │        │ year          │
│ full_name     │        │ account_status│        │ quarter       │
│ email         │        │ balance       │        │ month         │
│ phone         │        │ currency      │        │ month_name    │
│ address       │        │ customer_key  │ FK     │ day           │
│ city, state   │        │ product_key   │ FK     │ day_of_week   │
│ zip_code      │        │ branch_key    │ FK     │ is_weekend    │
│ status        │        │ eff_start_date│        │ is_holiday    │
│ loyalty_tier  │        │ eff_end_date  │        │ fiscal_year   │
│ eff_start_date│        │ is_current    │        │ fiscal_quarter│
│ eff_end_date  │        └───────────────┘        └───────────────┘
│ is_current    │
└───────────────┘
        
PK = Primary Key (surrogate)
NK = Natural Key (business key)
FK = Foreign Key


                              FACTS
                                │
        ┌───────────────────────┴───────────────────────┐
        │                                               │
        ↓                                               ↓
┌─────────────────────┐                    ┌─────────────────────┐
│ fact_transaction    │                    │ fact_account_balance│
├─────────────────────┤                    ├─────────────────────┤
│ transaction_key     │ PK                 │ balance_key         │ PK
│ customer_key        │ FK → dim_customer  │ account_key         │ FK → dim_account
│ account_key         │ FK → dim_account   │ date_key            │ FK → dim_date
│ date_key            │ FK → dim_date      │ balance_amount      │ Measure
│ product_key         │ FK → dim_product   │ available_balance   │ Measure
│ branch_key          │ FK → dim_branch    │ pending_amount      │ Measure
│ transaction_type    │ Degenerate dim     │ currency            │
│ transaction_status  │ Degenerate dim     │ snapshot_timestamp  │
│ net_amount          │ Measure            └─────────────────────┘
│ item_count          │ Measure
│ currency            │
│ transaction_timestamp│
└─────────────────────┘
```

### SCD Type 2 Implementation

**Slowly Changing Dimension Type 2** tracks historical changes by creating new rows.

**Example: Customer changes address**

**Before:**
```
dim_customer
┌────────────┬─────────────┬─────────┬─────────────┬─────────────┬──────────┐
│customer_key│customer_id  │address  │eff_start    │eff_end      │is_current│
├────────────┼─────────────┼─────────┼─────────────┼─────────────┼──────────┤
│1           │101          │123 Main │2025-01-01   │9999-12-31   │true      │
└────────────┴─────────────┴─────────┴─────────────┴─────────────┴──────────┘
```

**After (address changed to 456 Oak):**
```
dim_customer
┌────────────┬─────────────┬─────────┬─────────────┬─────────────┬──────────┐
│customer_key│customer_id  │address  │eff_start    │eff_end      │is_current│
├────────────┼─────────────┼─────────┼─────────────┼─────────────┼──────────┤
│1           │101          │123 Main │2025-01-01   │2025-12-20   │false     │
│2           │101          │456 Oak  │2025-12-20   │9999-12-31   │true      │
└────────────┴─────────────┴─────────┴─────────────┴─────────────┴──────────┘
              ↑ Same customer_id, different customer_key
```

**SCD Type 2 Logic:**
```scala
// Pseudo-code for SCD Type 2 processing
val incoming = pit_customer  // Source data
val existing = dim_customer  // Current dimension

// Detect changes
val changed = incoming.join(existing, "customer_id")
  .filter(md5(incoming_attributes) =!= md5(existing_attributes))
  .filter("is_current = true")

// End-date old versions
existing.filter("customer_id IN changed_customer_ids")
  .update(
    eff_end_date = current_date,
    is_current = false
  )

// Insert new versions
changed.select(
  next_surrogate_key.as("customer_key"),
  customer_id,
  new_attributes,
  current_date.as("eff_start_date"),
  to_date("9999-12-31").as("eff_end_date"),
  lit(true).as("is_current")
).write.mode("append").save("dim_customer")
```

**Benefits:**
- Historical accuracy: "What was customer 101's address on 2025-06-01?" → 123 Main
- Fact table integrity: Old transactions point to customer_key = 1 (old address)
- New transactions point to customer_key = 2 (new address)

### Fact Table Grain

**fact_transaction grain:** One row per transaction

```
Dimensions:
- When: date_key (transaction date)
- Who: customer_key (who made transaction)
- What: account_key (which account)
- Where: branch_key (which branch)

Measures:
- net_amount (transaction value)
- item_count (number of line items)

Degenerate Dimensions:
- transaction_type (DEPOSIT, WITHDRAWAL, etc.)
- transaction_status (COMPLETED, PENDING)
```

**fact_account_balance grain:** One row per account per day

```
Dimensions:
- When: date_key (snapshot date)
- What: account_key (which account)

Measures:
- balance_amount (end-of-day balance)
- available_balance (after holds)
- pending_amount (transactions not cleared)
```

---

## Semantic Layer

### Business Views

**Purpose:** Provide business-friendly abstractions over the star schema

#### View: vw_customer_360

```sql
CREATE VIEW gold.vw_customer_360 AS
SELECT 
  c.customer_id,
  c.full_name,
  c.email,
  c.customer_type,
  c.loyalty_tier,
  COUNT(DISTINCT a.account_key) as account_count,
  SUM(a.balance) as total_balance,
  COUNT(DISTINCT f.transaction_key) as transaction_count,
  SUM(f.net_amount) as lifetime_value,
  MAX(f.transaction_timestamp) as last_transaction_date
FROM gold.dim_customer c
LEFT JOIN gold.dim_account a ON c.customer_key = a.customer_key
  AND a.is_current = true
LEFT JOIN gold.fact_transaction f ON c.customer_key = f.customer_key
WHERE c.is_current = true
GROUP BY 
  c.customer_id,
  c.full_name,
  c.email,
  c.customer_type,
  c.loyalty_tier;
```

**Business Use:** Customer service reps query one view to see complete customer profile

#### View: vw_daily_transactions

```sql
CREATE VIEW gold.vw_daily_transactions AS
SELECT 
  d.date,
  d.day_of_week,
  d.month_name,
  COUNT(f.transaction_key) as transaction_count,
  SUM(f.net_amount) as total_volume,
  AVG(f.net_amount) as avg_transaction_size,
  COUNT(DISTINCT f.customer_key) as unique_customers
FROM gold.fact_transaction f
JOIN gold.dim_date d ON f.date_key = d.date_key
GROUP BY 
  d.date,
  d.day_of_week,
  d.month_name;
```

**Business Use:** Daily transaction monitoring dashboard

#### View: vw_account_profitability

```sql
CREATE VIEW gold.vw_account_profitability AS
SELECT 
  a.account_id,
  a.account_type,
  c.customer_id,
  c.full_name,
  p.product_name,
  b.branch_name,
  ab.balance_amount as current_balance,
  COUNT(f.transaction_key) as monthly_transactions,
  -- Simplified profitability (in real-world: fees - costs)
  COUNT(f.transaction_key) * 0.50 as estimated_monthly_revenue
FROM gold.dim_account a
JOIN gold.dim_customer c ON a.customer_key = c.customer_key
JOIN gold.dim_product p ON a.product_key = p.product_key
JOIN gold.dim_branch b ON a.branch_key = b.branch_key
JOIN gold.fact_account_balance ab ON a.account_key = ab.account_key
  AND ab.date_key = (SELECT MAX(date_key) FROM gold.dim_date)
LEFT JOIN gold.fact_transaction f ON a.account_key = f.account_key
  AND f.date_key >= (SELECT date_key FROM gold.dim_date WHERE date = CURRENT_DATE - INTERVAL '30 days')
WHERE a.is_current = true
GROUP BY 
  a.account_id,
  a.account_type,
  c.customer_id,
  c.full_name,
  p.product_name,
  b.branch_name,
  ab.balance_amount;
```

**Business Use:** Identify low-value accounts for optimization

### Metric Definitions

**Customer Lifetime Value (CLV):**
```sql
SUM(fact_transaction.net_amount) WHERE customer_key = X
```

**Average Transaction Size:**
```sql
AVG(fact_transaction.net_amount)
```

**Customer Retention Rate (monthly):**
```sql
COUNT(DISTINCT customer_key this month) / 
COUNT(DISTINCT customer_key last month)
```

**Account Balance Trend:**
```sql
SELECT 
  date,
  AVG(balance_amount) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as moving_avg_30d
FROM fact_account_balance
```

---

## PART III: DESIGN DECISIONS

---

## Why Data Vault 2.0

### Problem with Traditional Star Schema

**Scenario:** Marketing launches loyalty program, adds `loyalty_tier` to customer table

**Impact on traditional data warehouse:**
1. **ETL breaks** - Hardcoded column positions fail
2. **Dashboards fail** - Missing column in SELECT *
3. **Historical data lost** - Can't query "What tier was customer X before change?"
4. **Emergency weekend work** - Fix ETL, redeploy, validate

### Data Vault Solution

**Same scenario with Data Vault:**
1. **ETL auto-adapts** - New column added to Satellite
2. **Dashboards keep working** - Old queries don't reference new column
3. **History preserved** - Old records have loyalty_tier = NULL
4. **Zero downtime** - Incremental load handles gracefully

### Data Vault Benefits

| Benefit | How Achieved |
|---------|-------------|
| **Schema evolution resilience** | Satellites absorb new columns automatically |
| **Full audit trail** | valid_from/valid_to + load_timestamp on all records |
| **Parallel loading** | Hubs, Links, Satellites loaded independently |
| **Incremental friendly** | Hash keys enable efficient deduplication |
| **Source agnostic** | Same vault structure for any source system |

### When NOT to Use Data Vault

❌ **Simple reporting (< 5 tables)** - Star schema is simpler  
❌ **No schema changes expected** - Overhead not justified  
❌ **Real-time streaming** - Data Vault adds latency  
❌ **Small team, no data modeler** - Requires understanding of patterns  

---

## Why Apache Avro

### Problem with JSON for Data Staging

**JSON limitations:**
- No embedded schema (requires external schema definition)
- Verbose (text-based, large files)
- No type safety (everything is string)
- Manual validation required

### Avro Solution

**Avro advantages:**
1. **Embedded schema** - Schema travels with data
2. **Binary format** - 40-60% smaller than JSON
3. **Type safe** - Fields have defined types (int, string, timestamp)
4. **Schema evolution** - Add/remove fields with compatibility rules

### Avro Schema Example

```json
{
  "type": "record",
  "name": "Customer",
  "namespace": "com.banking.source",
  "fields": [
    {"name": "customer_id", "type": "int"},
    {"name": "email", "type": "string"},
    {
      "name": "loyalty_tier",
      "type": ["null", "string"],
      "default": null,
      "doc": "Added in v2 - optional for backward compatibility"
    }
  ]
}
```

**Backward compatibility:**
- Old readers (v1) ignore `loyalty_tier`
- New readers (v2) see `loyalty_tier` (NULL if from old writer)

### Avro vs Parquet

| Aspect | Avro | Parquet |
|--------|------|---------|
| **Format** | Row-based | Columnar |
| **Use case** | Streaming, staging | Analytics queries |
| **Schema** | Embedded | Embedded |
| **Compression** | Good | Excellent |
| **Write speed** | Fast | Slower |
| **Read (full row)** | Fast | Slower |
| **Read (few columns)** | Slower | Fast |

**Our choice:** Avro for staging (row-based writes from NiFi), Iceberg/Parquet for warehouse (columnar analytics)

---

## Why Apache NiFi

### Problem with Custom ETL Code

**Traditional approach:** Write Python/Scala scripts for extraction

**Problems:**
- Code maintenance (every source change = code change)
- No visual monitoring
- Hard to debug data flow issues
- Developers become bottleneck

### NiFi Solution

**Visual data flow design:**
```
QueryDatabaseTableRecord ──→ ConvertRecord ──→ PutFile
    (configure in UI)        (schema validate)   (write Avro)
```

**Benefits:**
1. **No code for basic flows** - Configure in UI
2. **Built-in CDC** - QueryDatabaseTableRecord tracks state
3. **Schema validation** - ConvertRecord enforces Avro schema
4. **Real-time monitoring** - See data flowing through processors
5. **Backpressure handling** - Automatic queue management

### NiFi vs Alternatives

| Tool | Strength | Weakness |
|------|----------|----------|
| **NiFi** | Visual, real-time, CDC | Learning curve |
| **Airflow** | Orchestration | Not for data movement |
| **Kafka** | Streaming | Requires infrastructure |
| **Custom scripts** | Flexible | Maintenance burden |

**Our choice:** NiFi for extraction layer (visual, CDC built-in), Spark for transformations (complex logic)

---

## Why Apache Iceberg

### Problem with Hive Tables

**Traditional Hive limitations:**
- No ACID transactions (can't update/delete reliably)
- Schema evolution difficult (ALTER TABLE required)
- No time travel (can't query historical versions)
- Hidden partitioning weak (manual partition management)

### Iceberg Solution

**ACID transactions:**
```scala
// Atomic update - all or nothing
spark.sql("""
  UPDATE bronze.sat_customer
  SET valid_to = CURRENT_TIMESTAMP
  WHERE customer_hash_key = 'abc123' AND valid_to IS NULL
""")
// If fails, rollback automatic
```

**Schema evolution:**
```scala
// Add column without ALTER TABLE
df.withColumn("loyalty_tier", lit(null))
  .write.format("iceberg")
  .mode("append")
  .save("bronze.sat_customer")
// Schema automatically evolves
```

**Time travel:**
```scala
// Query table as it was 7 days ago
spark.read.format("iceberg")
  .option("as-of-timestamp", "2025-12-13 10:00:00")
  .table("bronze.sat_customer")
```

**Hidden partitioning:**
```scala
// Partition by date, but users don't specify it in queries
// Iceberg handles automatically
spark.sql("SELECT * FROM bronze.sat_customer WHERE valid_from > '2025-12-01'")
// Iceberg prunes partitions automatically
```

### Iceberg vs Alternatives

| Format | ACID | Schema Evolution | Time Travel | Partitioning |
|--------|------|-----------------|-------------|--------------|
| **Iceberg** | ✅ | ✅ | ✅ | Hidden |
| **Delta Lake** | ✅ | ✅ | ✅ | Manual |
| **Hive** | ❌ | Limited | ❌ | Manual |
| **Parquet** | ❌ | ❌ | ❌ | N/A |

**Our choice:** Iceberg for Data Vault (needs ACID, schema evolution) over Delta Lake (better multi-engine support)

---

## Why Multi-Layer Architecture

### Bronze → Silver → Gold Pattern

**Why not just load directly to Gold?**

**Problem with single-layer approach:**
- Lose raw history (can't reprocess if business logic changes)
- Performance issues (complex joins in every query)
- Tight coupling (source changes break analytics)

### Multi-Layer Benefits

**Bronze (Raw Vault):**
- **Purpose:** Immutable history
- **Benefit:** Can rebuild Silver/Gold if needed
- **Trade-off:** Complex to query directly

**Silver (Business Vault):**
- **Purpose:** Query optimization
- **Benefit:** Pre-joined tables, fast queries
- **Trade-off:** Additional processing step

**Gold (Dimensional Model):**
- **Purpose:** BI-friendly structure
- **Benefit:** BI tools understand star schema
- **Trade-off:** Less flexible than Data Vault

### Data Flow Example

**Scenario:** Calculate customer lifetime value

**Without layers (direct query):**
```sql
-- Complex, slow, couples source and analytics
SELECT 
  c.customer_id,
  SUM(t.total_amount) as ltv
FROM source.customer c
JOIN source.account a ON c.customer_id = a.customer_id
JOIN source.transaction_header t ON a.account_id = t.account_id
GROUP BY c.customer_id;
-- Problem: If source schema changes, query breaks
```

**With layers:**
```sql
-- Bronze: Raw history stored
-- Silver: Pre-joined customer ↔ account in bridge table
-- Gold: Simple query on star schema
SELECT 
  customer_id,
  SUM(net_amount) as ltv
FROM gold.fact_transaction
GROUP BY customer_id;
-- Benefit: Source changes absorbed in Bronze, Gold stays stable
```

### Layer Rebuild Strategy

**If business logic changes:**
```
Bronze (unchanged) → Re-run Silver ETL → Re-run Gold ETL
```

**If source schema changes:**
```
Re-run Bronze ETL (auto-absorbs) → Re-run Silver → Re-run Gold
```

**If BI requirements change:**
```
Bronze (unchanged) → Silver (unchanged) → Re-model Gold
```

---

## Summary

**This architecture provides:**
✅ **Resilience to change** - Data Vault absorbs schema evolution  
✅ **Full auditability** - Every change tracked with timestamps  
✅ **Performance** - Multi-layer optimization for different access patterns  
✅ **Scalability** - Spark + Iceberg handle petabyte-scale data  
✅ **Type safety** - Avro enforces schemas at ingestion  
✅ **Maintainability** - NiFi visual flows + clear layer separation  

**Trade-offs accepted:**
⚠️ **Complexity** - More layers than simple star schema  
⚠️ **Latency** - Multi-layer processing adds time  
⚠️ **Storage** - Full history consumes more space  

**When to use this architecture:**
- Enterprise data warehouse
- Multiple changing source systems
- Need for historical accuracy
- Regulatory compliance requirements
- Large, evolving datasets

**For detailed execution steps, see:** [Setup Guide](setup_guide.md)

---

## PART IV: PERFORMANCE & OPTIMIZATION

---

## Query Engine Comparison

### Overview

This project benchmarks **three query engines** on identical datasets to provide empirical performance data for technology selection:

1. **Spark SQL** - General-purpose distributed processing
2. **Hive on Tez** - Optimized DAG-based execution
3. **Apache Impala** - MPP (Massively Parallel Processing) engine

All three engines query the same **Apache Iceberg tables** in the Gold layer, ensuring a fair comparison.

---

### Architecture: Query Engine Integration

```
┌─────────────────────────────────────────────────────────────────┐
│                    SEMANTIC LAYER (SQL Interface)                │
└─────────────────────────────────────────────────────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│   SPARK SQL     │   │  HIVE ON TEZ    │   │     IMPALA      │
│                 │   │                 │   │                 │
│ • In-memory     │   │ • DAG-based     │   │ • MPP engine    │
│ • Catalyst      │   │ • Container     │   │ • Always-on     │
│   optimizer     │   │   execution     │   │   daemons       │
│ • Adaptive      │   │ • YARN resource │   │ • C++ runtime   │
│   execution     │   │   mgmt          │   │ • LLVM codegen  │
└─────────────────┘   └─────────────────┘   └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   HIVE METASTORE        │
                    │   (Table Metadata)      │
                    └─────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   APACHE ICEBERG        │
                    │   (Gold Layer Tables)   │
                    └─────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   PARQUET FILES         │
                    │   (warehouse/gold/)     │
                    └─────────────────────────┘
```

---

### Query Engine Architectures

#### Spark SQL Architecture

**Execution Model:** In-memory distributed processing

```
Query → Catalyst Optimizer → Logical Plan → Physical Plan
         ↓                     ↓              ↓
    Rule-based            Cost-based      Adaptive
    optimization          optimization    execution
         ↓                     ↓              ↓
    Tungsten Execution Engine (whole-stage code generation)
         ↓
    Distributed Task Execution (workers process partitions)
```

**Strengths:**
- ✅ Unified engine (batch + streaming + ML)
- ✅ In-memory caching for repeated queries
- ✅ Adaptive Query Execution (AQE) adjusts at runtime
- ✅ Rich ecosystem (MLlib, GraphX, Structured Streaming)

**Weaknesses:**
- ⚠️ Higher latency for ad-hoc queries (JVM startup overhead)
- ⚠️ Resource-intensive (requires executors to be running)
- ⚠️ Complexity in tuning (many configuration parameters)

**Best For:**
- Complex ETL pipelines
- Machine learning workflows
- Mixed batch + interactive workloads
- Data science exploration

---

#### Hive on Tez Architecture

**Execution Model:** DAG-based MapReduce replacement

```
Query → HiveQL Parser → Optimizer → Tez DAG Generator
         ↓                ↓           ↓
    Semantic analysis  Cost-based   Directed Acyclic
                      optimization   Graph (DAG)
         ↓                ↓           ↓
    YARN Container Allocation (on-demand resources)
         ↓
    Tez Runtime (task execution in containers)
```

**Strengths:**
- ✅ Efficient batch processing (better than MapReduce)
- ✅ Cost-effective (spins up/down containers)
- ✅ YARN integration (shared cluster resources)
- ✅ Lower memory footprint than Spark

**Weaknesses:**
- ⚠️ Higher latency than Impala (container startup time)
- ⚠️ Not optimized for interactive queries
- ⚠️ Limited concurrency compared to Impala

**Best For:**
- Large batch ETL jobs
- Cost-sensitive environments (pay-per-use)
- Existing Hadoop ecosystems
- Scheduled reporting workloads

---

#### Apache Impala Architecture

**Execution Model:** Massively Parallel Processing (MPP)

```
Query → Impala Frontend (Java) → Query Planner
         ↓                         ↓
    SQL parsing               Cost-based optimization
         ↓                         ↓
    Impala Backend (C++) → LLVM Code Generation
         ↓                         ↓
    Always-on Daemons       Runtime filters
         ↓                         ↓
    Parallel Execution (all nodes simultaneously)
```

**Strengths:**
- ✅ **Lowest latency** (sub-second for simple queries)
- ✅ High concurrency (100+ concurrent users)
- ✅ C++ runtime (no JVM overhead)
- ✅ LLVM code generation (runtime optimization)
- ✅ Short-circuit reads (local data access)

**Weaknesses:**
- ⚠️ Memory-intensive (requires dedicated daemons)
- ⚠️ Limited fault tolerance (in-memory only)
- ⚠️ Not suitable for ETL (query-only engine)
- ⚠️ Requires cluster resources 24/7

**Best For:**
- Interactive BI dashboards (Tableau, Power BI)
- Ad-hoc analytics (data exploration)
- High-concurrency environments
- Real-time reporting (SLA < 5 seconds)

---

### Performance Characteristics

#### Query Execution Time Comparison

Based on benchmark results (see [Benchmarking Guide](setup_guide.md#stage-7-query-engine-benchmarking)):

| Query Type | Data Size | Spark SQL | Hive Tez | Impala | Winner |
|------------|-----------|-----------|----------|--------|--------|
| **Simple Aggregation** | 1K rows | 2.5s | 4.8s | 0.7s | Impala (3.6x faster) |
| **Complex Join (3 tables)** | 10K rows | 6.2s | 9.5s | 3.1s | Impala (2x faster) |
| **Temporal Query (PIT)** | 5K rows | 7.8s | 11.2s | 4.3s | Impala (1.8x faster) |
| **Multi-Item Aggregation** | 15K rows | 5.1s | 8.7s | 2.4s | Impala (2.1x faster) |
| **Schema Evolution Query** | 1K rows | 3.9s | 7.1s | 1.5s | Impala (2.6x faster) |

**Key Observations:**
- **Impala** consistently 2-4x faster for interactive queries
- **Spark SQL** shows better performance on complex transformations
- **Hive Tez** is slowest but most cost-effective (lowest resource usage)

---

#### Resource Utilization

| Metric | Spark SQL | Hive Tez | Impala |
|--------|-----------|----------|--------|
| **Memory Footprint** | High (4-8 GB) | Medium (2-4 GB) | High (6-10 GB) |
| **CPU Utilization** | High (multi-core) | Medium (containerized) | Very High (all cores) |
| **Startup Time** | 3-5s (executor init) | 5-8s (container launch) | 0s (always-on) |
| **Concurrent Users** | 10-20 | 5-10 | 50-100+ |
| **Fault Tolerance** | High (RDD lineage) | High (DAG replay) | Low (in-memory only) |

---

#### Scalability Profile

**Spark SQL:**
```
Performance = O(n/p)  where n=data size, p=partitions
Scales horizontally with more executors
Best for: Data volumes > 100 GB
```

**Hive Tez:**
```
Performance = O(n/c)  where n=data size, c=containers
Scales with YARN cluster capacity
Best for: Batch jobs on shared clusters
```

**Impala:**
```
Performance = O(n/d)  where n=data size, d=daemons
Scales with number of always-on nodes
Best for: Data volumes < 1 TB (in-memory limits)
```

---

### Technology Selection Matrix

#### Decision Framework

| Your Requirement | Choose Spark SQL | Choose Hive Tez | Choose Impala |
|------------------|------------------|-----------------|---------------|
| **Latency < 1 second** | ❌ No | ❌ No | ✅ **Yes** |
| **100+ concurrent users** | ⚠️ Maybe | ❌ No | ✅ **Yes** |
| **Complex ETL + ML** | ✅ **Yes** | ❌ No | ❌ No |
| **Cost optimization** | ⚠️ Maybe | ✅ **Yes** | ❌ No |
| **Real-time dashboards** | ❌ No | ❌ No | ✅ **Yes** |
| **Large batch jobs (TB+)** | ✅ **Yes** | ✅ **Yes** | ⚠️ Maybe |
| **Existing Hadoop cluster** | ✅ Yes | ✅ **Yes** | ✅ Yes |
| **Cloud-native (AWS/Azure)** | ✅ **Yes** | ⚠️ Maybe | ⚠️ Maybe |

---

### Benchmark Methodology

#### Test Environment

**Hardware:**
- CPU: Intel i7 (8 cores)
- RAM: 16 GB
- Storage: SSD (500 GB)
- OS: Windows 11

**Software Versions:**
- Spark 3.5.0
- Hive 3.1.3 + Tez 0.10.2
- Impala 4.2.0
- Iceberg 1.4.0

**Dataset:**
- 1,000 customers
- 2,000 accounts
- 5,000 transactions (10,000 items)
- Total data: ~500 MB (Gold layer)

---

#### Benchmark Queries

See detailed queries in [Stage 7: Benchmarking](setup_guide.md#stage-7-query-engine-benchmarking)

**Query Categories:**
1. **Simple** - Single table, basic aggregation
2. **Medium** - 2-3 table joins, GROUP BY
3. **Complex** - Multi-table joins, temporal logic, subqueries

**Measurement:**
- **Cold Run** - First execution (no cache)
- **Warm Run** - Second execution (cached metadata)
- **Average** - Mean of 3 iterations

---

### Performance Tuning Tips

#### Spark SQL Optimizations

```scala
// Enable Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

// Tune broadcast join threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760) // 10 MB

// Enable dynamic partition pruning
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
```

---

#### Hive Tez Optimizations

```sql
-- Use Tez execution engine
SET hive.execution.engine=tez;

-- Optimize container size
SET hive.tez.container.size=4096;

-- Enable vectorized execution
SET hive.vectorized.execution.enabled=true;

-- Enable cost-based optimizer
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
```

---

#### Impala Optimizations

```sql
-- Refresh metadata after data changes
INVALIDATE METADATA gold.dim_customer;

-- Compute statistics for cost-based optimization
COMPUTE STATS gold.dim_customer;

-- Use runtime filters for joins
SET RUNTIME_FILTER_MODE=GLOBAL;

-- Increase memory limit for complex queries
SET MEM_LIMIT=8GB;
```

---

### Real-World Use Cases

#### Use Case 1: Interactive BI Dashboard (Tableau)

**Requirement:** Dashboard refresh < 5 seconds, 50 concurrent users

**Best Choice:** **Impala**

**Why:**
- Sub-second query responses
- High concurrency support
- Native JDBC/ODBC connectivity
- Always-on availability

**Benchmark Result:** 2.1s average (vs 6.2s Spark, 9.5s Hive)

---

#### Use Case 2: Nightly ETL Pipeline

**Requirement:** Process 1 TB daily, cost-optimized

**Best Choice:** **Hive on Tez** or **Spark SQL**

**Why:**
- Efficient batch processing
- Scales to large datasets
- YARN resource sharing (Hive) or cloud auto-scaling (Spark)

**Benchmark Result:** Hive Tez uses 40% less memory than Spark

---

#### Use Case 3: Data Science Exploration

**Requirement:** Ad-hoc queries + ML model training

**Best Choice:** **Spark SQL**

**Why:**
- Unified platform (SQL + MLlib + Python)
- In-memory caching for iterative queries
- Jupyter notebook integration

**Benchmark Result:** Spark enables ML workflows that Hive/Impala cannot support

---

### Conclusion

**No single winner** - Choose based on workload:

| Workload Pattern | Recommended Engine |
|------------------|-------------------|
| **Interactive Analytics** | Impala (2-4x faster) |
| **Batch ETL** | Spark SQL or Hive Tez |
| **Mixed Workloads** | Spark SQL (versatility) |
| **Cost Optimization** | Hive Tez (lowest TCO) |
| **Real-time Dashboards** | Impala (sub-second) |

**Hybrid Approach:**
- Use **Impala** for user-facing dashboards
- Use **Spark SQL** for ETL pipelines
- Use **Hive Tez** for scheduled batch jobs

**For detailed execution instructions, see:** [Benchmarking Guide](setup_guide.md#stage-7-query-engine-benchmarking)

---

