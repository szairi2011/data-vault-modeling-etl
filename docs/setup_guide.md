# Complete Setup & Execution Guide

**Your single source for running the complete data pipeline from PostgreSQL to Analytics.**

---

## 📋 Table of Contents

### Part I: Foundation
- [Prerequisites & Tools](#prerequisites--tools)
- [Understanding the Pipeline Flow](#understanding-the-pipeline-flow)

### Part II: Pipeline Execution (Stage by Stage)
- [Stage 1: PostgreSQL Source System](#stage-1-postgresql-source-system)
- [Stage 2: NiFi Data Extraction & Avro Staging](#stage-2-nifi-data-extraction--avro-staging)
- [Stage 3: Bronze Layer (Raw Vault)](#stage-3-bronze-layer-raw-vault)
- [Stage 4: Silver Layer (Business Vault)](#stage-4-silver-layer-business-vault)
- [Stage 5: Gold Layer (Dimensional Model)](#stage-5-gold-layer-dimensional-model)
- [Stage 6: Schema Evolution Scenario](#stage-6-schema-evolution-scenario)

### Part III: Reference
- [Quick Commands](#quick-commands)
- [Troubleshooting](#troubleshooting)
- [Daily Operations](#daily-operations)

---

## PART I: FOUNDATION

---

## Prerequisites & Tools

### Required Software

Verify installations before starting:

```powershell
# Check versions
java -version        # Requirement: 11+ (for Spark)
scala -version       # Requirement: 2.12.x
sbt version          # Requirement: 1.9+
psql --version       # Requirement: 12+
```

### NiFi Setup

**Apache NiFi 2.7.2** must be installed and running:
- URL: `https://localhost:8443/nifi`
- Installation directory example: `C:\nifi\nifi-2.7.2`
- No Docker required (Windows native)

### Project Location

All commands assume you're in the project root:
```powershell
cd C:\Users\sofiane\work\learn-intellij\data-vault-modeling-etl
```

---

## Understanding the Pipeline Flow

### High-Level Overview

Each stage builds on the previous, transforming data step-by-step:

```
STAGE 1: PostgreSQL (Source)
  ↓ banking.customer, banking.account, banking.transaction_*
  ↓ Operational data (3NF normalized, frequent changes)
  
STAGE 2: NiFi + Avro (Extraction & Validation)
  ↓ QueryDatabaseTableRecord → ConvertRecord → PutFile
  ↓ warehouse/staging/*.avro (schema-validated files)
  
STAGE 3: Bronze (Raw Vault - Spark)
  ↓ AvroReader → HashKeyGenerator → Hub/Link/Satellite
  ↓ bronze.hub_*, bronze.sat_*, bronze.link_* (historized)
  
STAGE 4: Silver (Business Vault - Spark)
  ↓ PIT Builder → Bridge Builder
  ↓ silver.pit_*, silver.bridge_* (query optimization)
  
STAGE 5: Gold (Dimensional Model - Spark)
  ↓ SCD Type 2 → Fact Builder
  ↓ gold.dim_*, gold.fact_* (BI-ready star schema)
```

### Why This Architecture?

| Stage | Problem Solved | Benefit |
|-------|---------------|---------|
| **NiFi + Avro** | No schema validation before Spark | Data quality gate, incremental CDC |
| **Bronze** | Source systems change frequently | Resilient to schema changes, full history |
| **Silver** | Data Vault joins are complex | Pre-joined tables for performance |
| **Gold** | BI tools need star schemas | Fast aggregations, SCD Type 2 history |

---

## PART II: PIPELINE EXECUTION

---

## Stage 1: PostgreSQL Source System

### Context from Previous Stage
**N/A** - This is the starting point.

### Purpose of This Stage
Create an operational banking database that simulates a real source system with:
- Normalized tables (3NF)
- Relationships (customers → accounts → transactions)
- Multi-item transactions (like e-commerce orders)
- Data that changes over time (enables CDC testing)

### Actions

#### 1.1: Create Database and Schema

```powershell
# Create database
psql -U postgres -c "CREATE DATABASE banking_source;"

# Create schema
psql -U postgres -d banking_source -c "CREATE SCHEMA banking;"
```

**What just happened:** Created the container for operational banking data.

---

#### 1.2: Create Tables

```powershell
psql -U postgres -d banking_source -f source-system\sql\02_create_tables.sql
```

**Tables created:**
- `banking.customer` - Customer master (individuals and businesses)
- `banking.account` - Accounts (checking, savings, credit cards, loans)
- `banking.transaction_header` - Transaction summaries
- `banking.transaction_item` - Transaction line items (multi-item support)
- `banking.product` - Product catalog
- `banking.branch` - Branch locations
- `banking.category` - Transaction categories (hierarchical)

**Verify:**
```powershell
psql -U postgres -d banking_source -c "\dt banking.*"
```

Expected output: 7 tables listed

---

#### 1.3: Seed Reference Data

```powershell
sbt "runMain seeder.ReferenceDataSeeder"
```

**What this creates:**
- 12 products (checking accounts, savings, credit cards, loans)
- 10 branches (NYC, SF, Chicago, Boston, etc.)
- 19 categories (hierarchical tree: Banking → Deposits → ATM Deposit)

**Verify:**
```sql
psql -U postgres -d banking_source

SELECT * FROM banking.product;
-- Expected: 12 rows

SELECT * FROM banking.category ORDER BY path;
-- Expected: 19 rows with hierarchical paths
```

---

#### 1.4: Seed Transactional Data

```powershell
sbt "runMain seeder.TransactionalDataSeeder"
```

**What this generates:**
- **1,000 customers** (900 individuals, 100 businesses)
- **~2,000 accounts** (1-3 per customer, realistic distributions)
- **5,000 transaction headers** (last 90 days)
- **~10,000 transaction items** (2-3 items per transaction on average)

**Data characteristics:**
- Realistic names (Faker library)
- Valid email addresses
- Account balances: $100 to $500,000
- Transaction amounts: $10 to $10,000
- Multi-item transactions (e.g., bill payment with 3 line items)

**Verify:**
```sql
psql -U postgres -d banking_source

-- Check customer distribution
SELECT customer_type, COUNT(*) FROM banking.customer GROUP BY customer_type;
-- INDIVIDUAL: ~900, BUSINESS: ~100

-- Check multi-item transactions
SELECT 
  th.transaction_number,
  COUNT(ti.item_id) as item_count
FROM banking.transaction_header th
JOIN banking.transaction_item ti ON th.transaction_id = ti.transaction_id
GROUP BY th.transaction_number
HAVING COUNT(ti.item_id) > 1
LIMIT 10;
-- Should see transactions with 2-3 items
```

### Validation Checkpoint
✅ **Database:** banking_source exists  
✅ **Tables:** 7 tables created  
✅ **Data:** 1000 customers, 2000 accounts, 5000 transactions  

### Transition to Next Stage
**You now have:** Operational banking data ready for extraction  
**Next step:** Extract this data with NiFi, validate with Avro schemas, stage for Spark

---

## Stage 2: NiFi Data Extraction & Avro Staging

### Context from Previous Stage
✅ PostgreSQL has 1,000 customers, 2,000 accounts, 5,000 transactions  
✅ Tables are normalized (3NF) with relationships

### Purpose of This Stage
**Problem:** Spark shouldn't read directly from PostgreSQL because:
- No schema validation before ingestion → bad data corrupts warehouse
- No incremental extraction → full scans are expensive
- Direct DB connections don't scale → couples operational and analytical systems

**Solution:** NiFi extracts, validates, and stages data as Avro files:
- **Schema enforcement** at write-time (reject invalid data early)
- **Incremental CDC** via `updated_at` column tracking
- **Decoupled architecture** - Spark reads files, not live DB

### Understanding Avro in This Pipeline

**What is Avro?**
- Binary data format with embedded schema
- Compact (smaller than JSON)
- Self-describing (schema travels with data)
- Supports schema evolution (add/remove fields)

**Why Avro for staging?**
- **Type safety:** NiFi validates against `.avsc` schema before writing
- **Spark compatibility:** Spark reads Avro natively with schema inference
- **Schema evolution:** When source adds columns, Avro handles gracefully

### Actions

#### 2.1: Validate Avro Schemas Exist

```powershell
.\nifi\scripts\validate-nifi-schemas.ps1
```

**Why this matters (connect the dots):**
- Our *pipeline contract* between NiFi and Spark is: **"staged data must match an Avro schema"**.
- If schemas are missing or invalid, everything downstream becomes guesswork:
  - NiFi cannot reliably validate/serialize data.
  - Spark may infer wrong types, or loads may fail later (harder to debug).
- Doing this first is a cheap, fast “quality gate” before we build any flow.

**Output you should expect:** a list of 4 schemas validated successfully.

---

#### 2.2: Create the `customer` Ingestion Flow Manually (NiFi 2.7.2)

**Why this step exists / connection with previous step:**
- In 2.1 we validated the Avro schemas exist (`nifi/schemas/*.avsc`).
- Now we need a NiFi flow that (1) extracts from PostgreSQL, then (2) converts records using the Avro schema, then (3) writes `.avro` files to `warehouse/staging/...`.

**Important (NiFi 2.7.2 reality):**
- Flow definitions are **JSON**.
- The recommended approach is: **build the flow on the canvas**, then **download the flow definition** as JSON.

##### 2.2.1: Create a Process Group
1. Open NiFi UI: https://localhost:8443/nifi
2. Drag **Process Group** to the canvas.
3. Name it: `PostgreSQL to Avro - Customer`.
4. Click **Add**.
5. Double-click the process group to enter it.

##### 2.2.2: Add the processors (inside the process group)
Add these processors (names are suggestions to keep things readable):

1. **QueryDatabaseTableRecord** → name: `QDBTR - customer`
2. **ConvertRecord** → name: `ConvertRecord - JSON to Avro (customer)`
3. **UpdateAttribute** → name: `UpdateAttribute - customer filename`
4. **PutFile** → name: `PutFile - stage customer avro`
5. **LogAttribute** → name: `LogAttribute - customer failure`

##### 2.2.3: Connect them
Create connections:
- `QDBTR - customer` → `ConvertRecord - JSON to Avro (customer)` (**success**)
- `ConvertRecord - JSON to Avro (customer)` → `UpdateAttribute - customer filename` (**success**)
- `UpdateAttribute - customer filename` → `PutFile - stage customer avro` (**success**)
- `ConvertRecord - JSON to Avro (customer)` → `LogAttribute - customer failure` (**failure**)

Then set **Auto-terminate** relationships where appropriate:
- On `PutFile - stage customer avro`: auto-terminate **success** and **failure**.
- On `LogAttribute - customer failure`: auto-terminate **success**.

---

#### 2.3: Create / Enable Controller Services (once)

Controller services are shared building blocks. Create them at the root level (or at least in a scope shared by your flow).

##### 2.3.1: DBCPConnectionPool (PostgreSQL)
Create a `DBCPConnectionPool` service with:
- Database Connection URL: `jdbc:postgresql://localhost:5432/banking_source`
- Database Driver Class Name: `org.postgresql.Driver`
- Database User: `postgres`
- Password: `<your password>`

Enable it.

##### 2.3.2: JsonTreeReader
Create and enable a `JsonTreeReader`.

##### 2.3.3: AvroRecordSetWriter (Customer)
Create and enable an `AvroRecordSetWriter` named `AvroRecordSetWriter-Customer` with:
- Schema Write Strategy: `Embed Avro Schema`
- Schema Access Strategy: `Use 'Schema Text' Property`
- Schema Text: paste the content of `nifi/schemas/customer.avsc`

---

#### 2.4: Configure each processor (customer)

##### 2.4.1: `QDBTR - customer` (QueryDatabaseTableRecord)
- Database Connection Pooling Service: **DBCPConnectionPool**
- Table Name: `banking.customer`
- Maximum-value Columns: `updated_at`
- Record Writer: a JSON writer (use what your NiFi offers for record writer; some setups will use a JsonRecordSetWriter)

##### 2.4.2: `ConvertRecord - JSON to Avro (customer)`
- Record Reader: **JsonTreeReader**
- Record Writer: **AvroRecordSetWriter-Customer**

##### 2.4.3: `UpdateAttribute - customer filename`
Add (or set) property:
- `filename` = `customer_${now():format('yyyyMMdd_HHmmss')}_${UUID()}.avro`

##### 2.4.4: `PutFile - stage customer avro`
- Directory = `C:\Users\sofiane\work\learn-intellij\data-vault-modeling-etl\warehouse\staging\customer`
- Create Missing Directories = `true`
- Conflict Resolution Strategy = `replace`

##### 2.4.5: `LogAttribute - customer failure`
- Log Payload = `true`

---

#### 2.5: Run the customer flow once (smoke test)
1. Go back to the root canvas.
2. Start the `PostgreSQL to Avro - Customer` process group.
3. Verify output files:

```powershell
New-Item -ItemType Directory -Force -Path warehouse\staging\customer | Out-Null
Get-ChildItem warehouse\staging\customer
```

**Why a smoke test before exporting JSON:**
- If the flow is broken, exporting it as a “golden template” just bakes in the problem.
- A successful run proves:
  - DB connectivity works.
  - CDC column is configured.
  - Avro schema can be applied.
  - Output path is writable.

**What you should observe in NiFi UI after a successful run:**

```
In the Process Group view (PostgreSQL to Avro - Customer):
────────────────────────────────────────────────────────────
┌──────────────────────────┐
│ QDBTR - customer         │ ← Shows "1000 In / 1000 Out"
│ ● Running                │
└──────────────────────────┘
            ↓ (success queue shows 1000 FlowFiles briefly)
┌──────────────────────────┐
│ ConvertRecord            │ ← Shows "1000 In / 1000 Out"
│ ● Running                │   (0 to failure = good!)
└──────────────────────────┘
            ↓ (success queue)
┌──────────────────────────┐
│ UpdateAttribute          │ ← Shows "1000 In / 1000 Out"
│ ● Running                │
└──────────────────────────┘
            ↓ (success queue)
┌──────────────────────────┐
│ PutFile                  │ ← Shows "1000 In / 1000 Out"
│ ● Running                │   (files written to disk)
└──────────────────────────┘

After completion (30-60 seconds):
  - All processors show "Stopped" or "Valid" status
  - No data in queues (all processed)
  - Bulletin board (bell icon) shows no errors
```

**How to check provenance (data lineage):**
```
1. Right-click any processor → View data provenance
2. You'll see a timeline of FlowFiles:
   - CREATE event: when QDBTR extracted from DB
   - ATTRIBUTES_MODIFIED: when UpdateAttribute ran
   - CONTENT_MODIFIED: when ConvertRecord wrote Avro
   - SEND: when PutFile wrote to disk
   
3. Click any event → "View details" to see:
   - Input content (JSON record)
   - Output content (Avro binary)
   - Attributes (filename, timestamps, etc.)
```

**Common things you might see (and they're normal):**
```
✓ Queues briefly fill up (1000 FlowFiles) then drain quickly
✓ QDBTR shows "Yielded" after first run (waiting for new data)
✓ PutFile shows "1000 files transferred" in stats
✓ Small yellow warning if JDBC connection pool was slow to initialize (one-time)

✗ Red error indicator on ConvertRecord = schema mismatch (investigate!)
✗ Data stuck in queues for > 5 minutes = configuration issue
```

---

#### 2.6: Download the customer flow definition as JSON (Golden Template)

Now export the flow definition from NiFi (NiFi 2.7.2):
1. Right-click the `PostgreSQL to Avro - Customer` process group.
2. Choose **Download flow definition**.
3. Save it into the repository as:

- `nifi-flows/customer_flow.json`

This JSON file is the canonical flow definition we’ll reuse for other entities.

**Why we download the JSON into the repo:**
- It makes the NiFi configuration reproducible (versioned alongside code).
- It gives you a concrete artifact you can diff/review.
- It becomes the starting point to create the other flows with minimal changes.

**Learning note:**
- Think of this as “infrastructure-as-code” but for NiFi flows.

---

#### 2.7: Reuse the downloaded JSON for other entities (placeholder)

> Placeholder (next step): Use `nifi-flows/customer_flow.json` as a starting point to create `account`, `transaction_header`, and `transaction_item` by uploading the JSON as a new process group and then changing entity-specific settings (table name, output directory, and Avro schema text).

---

### Validation Checkpoint (Stage 2)
✅ **Avro schemas:** validated (`nifi/schemas/*.avsc`)  
✅ **NiFi flow:** customer ingestion flow created manually on canvas  
✅ **Golden template:** `nifi-flows/customer_flow.json` downloaded from NiFi  
✅ **Staging output:** `warehouse/staging/customer/*.avro`

### Transition to Next Stage
**You now have:** Avro-staged customer data (and a reusable flow definition)  
**Next step:** Load into Data Vault structures (Hubs, Links, Satellites)

---

## Stage 3: Bronze Layer (Raw Vault)

### Context from Previous Stage
✅ Avro files in warehouse/staging/customer/*.avro  
✅ Each file contains embedded schema (customer.avsc)  
✅ Data validated by NiFi (schema matches, types correct)  

### Purpose of This Stage
**Problem:** Source systems change. When PostgreSQL adds a `loyalty_tier` column:
- Traditional ETL: Breaks, dashboards fail, emergency weekend work
- Lost history: Can't query "What was this customer's email in January?"

**Solution:** Data Vault provides:
- **Automatic schema absorption:** New columns added to Satellites without breaking queries
- **Full history:** valid_from/valid_to tracking for all attribute changes
- **Audit trail:** Load metadata tracks when/where data came from

### Understanding Data Vault Components

**Hubs** - Store unique entities (business keys)
```sql
-- Example: hub_customer
customer_hash_key    -- MD5(customer_id)
customer_id          -- Business key from source
load_timestamp       -- When first seen
record_source        -- Where it came from
```

**Satellites** - Store attributes with history
```sql
-- Example: sat_customer
customer_hash_key    -- FK to hub_customer
email, first_name, last_name, ...  -- Attributes
valid_from           -- When this version became active
valid_to             -- When superseded (NULL = current)
load_timestamp       -- ETL execution time
```

**Links** - Store relationships
```sql
-- Example: link_customer_account
link_hash_key        -- MD5(customer_hash_key + account_hash_key)
customer_hash_key    -- FK to hub_customer
account_hash_key     -- FK to hub_account
load_timestamp       -- When relationship first seen
```

### Actions

#### 3.1: Create Data Vault Tables

```powershell
sbt "runMain bronze.RawVaultSchema"
```

**What this creates:**

**Hubs (5 tables):**
- `bronze.hub_customer` - Unique customers
- `bronze.hub_account` - Unique accounts
- `bronze.hub_transaction` - Unique transactions
- `bronze.hub_product` - Unique products
- `bronze.hub_branch` - Unique branches

**Satellites (4 tables):**
- `bronze.sat_customer` - Customer attributes with history
- `bronze.sat_account` - Account attributes with history
- `bronze.sat_transaction` - Transaction attributes with history
- `bronze.sat_transaction_item` - Transaction item attributes

**Links (2 tables):**
- `bronze.link_customer_account` - Customer ← → Account relationships
- `bronze.link_transaction_item` - Transaction ← → Item relationships

**Metadata:**
- `bronze.load_metadata` - ETL execution tracking

**Table format:** Apache Iceberg (supports ACID, schema evolution, time travel)

---

#### 3.2: Load Avro Data into Data Vault

```powershell
sbt "runMain bronze.RawVaultETL --mode full"
```

**What happens (detailed walkthrough):**

##### Step 1: Read Avro Files
```
📖 READING AVRO FILES
   Path: warehouse/staging/customer/*.avro
   Validation: Enabled

Processing:
  1. Spark reads all .avro files in directory
  2. Automatically extracts embedded schema
  3. Creates DataFrame with proper types
  4. AvroReader.readAvro() validates schema structure
  5. Checks for required fields (customer_id, email, etc.)
  6. Warns if new fields detected (schema evolution)
  
✅ Schema validated: 13 fields
📊 Records read: 1000
```

##### Step 2: Generate Hash Keys
```
📦 Loading Hub_Customer...
   Hash algorithm: MD5
   Input: customer_id (business key)
   Output: customer_hash_key
   
Code (simplified):
  val customerHashKey = md5(concat(col("customer_id")))
  
Example:
  customer_id = 1
  → customer_hash_key = "c4ca4238a0b923820dcc509a6f75849b"
```

##### Step 3: Deduplicate for Hub
```
Deduplication logic:
  1. Check if customer_hash_key exists in bronze.hub_customer
  2. Filter out existing keys (already loaded)
  3. Keep only new customers
  
First run: 0 existing → 1000 new
Subsequent runs: 1000 existing → only changed customers
```

##### Step 4: Load Hub
```
✅ Loaded 1000 new customers to Hub_Customer

Table contents:
┌────────────────┬─────────────┬─────────────────┐
│customer_hash_key│customer_id │load_timestamp  │
├────────────────┼─────────────┼─────────────────┤
│c4ca4238a0b... │1            │2025-12-20 10:00│
│c81e728d9d4... │2            │2025-12-20 10:00│
└────────────────┴─────────────┴─────────────────┘
```

##### Step 5: Historize Attributes in Satellite
```
🛰️  Loading Sat_Customer...
   Historization: Enabled (valid_from/valid_to)
   
Logic:
  1. For each customer, check if attributes changed
  2. If changed:
     - End-date old record (set valid_to = current_timestamp)
     - Insert new record (valid_from = current_timestamp, valid_to = NULL)
  3. If new customer:
     - Insert record (valid_from = current_timestamp, valid_to = NULL)
  
First run: All new → 1000 inserts
✅ Loaded 1000 customer records to Sat_Customer

Table contents:
┌────────────────┬──────┬────────┬──────────┬──────────┐
│customer_hash_key│email │status  │valid_from│valid_to  │
├────────────────┼──────┼────────┼──────────┼──────────┤
│c4ca4238a0b... │john@ │ACTIVE  │2025-12-20│NULL      │
│c81e728d9d4... │jane@ │ACTIVE  │2025-12-20│NULL      │
└────────────────┴──────┴────────┴──────────┴──────────┘
                                    ↑ Current record (valid_to = NULL)
```

---

#### 3.3: Load Other Entities

Run the same process for accounts and transactions:

```powershell
# Load accounts
sbt "runMain bronze.RawVaultETL --entity account"

# Load transactions
sbt "runMain bronze.RawVaultETL --entity transaction"
```

**Each entity follows the same pattern:**
1. Read Avro files
2. Generate hash keys
3. Deduplicate
4. Load Hub
5. Historize in Satellite
6. Load Links (relationships)

---

### Verification

```powershell
sbt console
```

```scala
// Check Hub counts
spark.sql("SELECT COUNT(*) FROM bronze.hub_customer").show()
// Expected: 1000

spark.sql("SELECT COUNT(*) FROM bronze.hub_account").show()
// Expected: ~2000

// Check Satellite current records
spark.sql("SELECT COUNT(*) FROM bronze.sat_customer WHERE valid_to IS NULL").show()
// Expected: 1000 (all current)

// Check history tracking
spark.sql("""
  SELECT 
    customer_id,
    email,
    customer_status,
    valid_from,
    valid_to
  FROM bronze.sat_customer
  WHERE customer_id = 1
  ORDER BY valid_from
""").show()
// Should see one record (first load, no changes yet)

// Check Links
spark.sql("SELECT COUNT(*) FROM bronze.link_customer_account").show()
// Expected: ~2000 (customer-account relationships)

// Verify join works
spark.sql("""
  SELECT 
    h.customer_id,
    s.email,
    s.customer_status
  FROM bronze.hub_customer h
  JOIN bronze.sat_customer s ON h.customer_hash_key = s.customer_hash_key
  WHERE s.valid_to IS NULL
  LIMIT 5
""").show()
```

### Validation Checkpoint
✅ **Hubs loaded:** 1000 customers, ~2000 accounts, 5000 transactions  
✅ **Satellites loaded:** Full attribute history with valid_from/valid_to  
✅ **Links loaded:** Customer-account, transaction-item relationships  
✅ **Hash keys:** MD5 generated for all entities  

### Transition to Next Stage
**You now have:** Complete Data Vault with historization  
**Next step:** Optimize queries with PIT and Bridge tables

---

## Stage 4: Silver Layer (Business Vault)

### Context from Previous Stage
✅ Bronze has 1000 customers in hub_customer  
✅ Attributes tracked in sat_customer with valid_from/valid_to  
✅ Relationships in link_customer_account  

### Purpose of This Stage
**Problem:** Querying Data Vault directly is complex:
```sql
-- Get current customer attributes (requires Hub + Satellite join)
SELECT h.customer_id, s.email, s.customer_status
FROM bronze.hub_customer h
JOIN bronze.sat_customer s ON h.customer_hash_key = s.customer_hash_key
WHERE s.valid_to IS NULL;  -- Filter for current version

-- This join pattern repeats in every query!
```

**Solution:** Silver layer creates performance-optimized tables:
- **PIT (Point-in-Time):** Snapshot of all current attributes (pre-joined)
- **Bridge:** Pre-computed relationships with aggregates

### Actions

#### 4.1: Build PIT Tables

```powershell
sbt "runMain silver.BusinessVaultETL --build-pit"
```

**What this does:**

```
📸 Building PIT_Customer for 2025-12-20...

Logic:
  1. Join hub_customer + sat_customer
  2. Filter: WHERE valid_to IS NULL (current records only)
  3. Add: snapshot_date = CURRENT_DATE
  4. Write to: silver.pit_customer
  
Result: Flattened table with current attributes
┌─────────────┬──────┬────────┬──────────────┐
│customer_id  │email │status  │snapshot_date │
├─────────────┼──────┼────────┼──────────────┤
│1            │john@ │ACTIVE  │2025-12-20    │
│2            │jane@ │ACTIVE  │2025-12-20    │
└─────────────┴──────┴────────┴──────────────┘

✅ Created PIT_Customer snapshot with 1000 records
```

**Query comparison:**
```sql
-- Before (Bronze - complex)
SELECT h.customer_id, s.email
FROM bronze.hub_customer h
JOIN bronze.sat_customer s ON h.customer_hash_key = s.customer_hash_key
WHERE s.valid_to IS NULL;

-- After (Silver - simple)
SELECT customer_id, email
FROM silver.pit_customer
WHERE snapshot_date = CURRENT_DATE;
```

---

#### 4.2: Build Bridge Tables

```powershell
sbt "runMain silver.BusinessVaultETL --build-bridge"
```

**What this does:**

```
🌉 Building Bridge_Customer_Account...

Logic:
  1. Join hub_customer + link_customer_account + hub_account
  2. Aggregate: COUNT(accounts), SUM(balance)
  3. Identify primary account (highest balance)
  4. Write to: silver.bridge_customer_account
  
Result: Pre-joined relationships with metrics
┌─────────────┬────────────┬────────┬────────────┬──────────────┐
│customer_id  │account_id  │balance │account_count│is_primary   │
├─────────────┼────────────┼────────┼────────────┼──────────────┤
│1            │101         │5000    │2            │false         │
│1            │102         │10000   │2            │true          │
└─────────────┴────────────┴────────┴────────────┴──────────────┘

✅ Created Bridge_Customer_Account with 2000 relationships
```

---

### Verification

```scala
// Check PIT table
spark.sql("SELECT COUNT(*) FROM silver.pit_customer WHERE snapshot_date = CURRENT_DATE").show()
// Expected: 1000

spark.sql("SELECT * FROM silver.pit_customer WHERE snapshot_date = CURRENT_DATE LIMIT 3").show()

// Check Bridge table
spark.sql("SELECT COUNT(*) FROM silver.bridge_customer_account").show()
// Expected: ~2000

spark.sql("""
  SELECT 
    customer_id,
    COUNT(*) as account_count,
    SUM(balance) as total_balance
  FROM silver.bridge_customer_account
  GROUP BY customer_id
  ORDER BY total_balance DESC
  LIMIT 10
""").show()
```

### Validation Checkpoint
✅ **PIT tables:** Current snapshots for fast queries  
✅ **Bridge tables:** Pre-joined relationships with aggregates  

### Transition to Next Stage
**You now have:** Optimized query layer on top of Data Vault  
**Next step:** Create BI-friendly dimensional model (star schema)

---

## Stage 5: Gold Layer (Dimensional Model)

### Context from Previous Stage
✅ Silver has pit_customer with current attributes  
✅ Silver has bridge_customer_account with relationships  

### Purpose of This Stage
**Problem:** BI tools (Tableau, Power BI) expect star schemas, not Data Vault or PIT tables.

**Solution:** Transform Silver → Gold with:
- **Dimensions:** SCD Type 2 for slowly changing dimensions
- **Facts:** Aggregated metrics with dimensional keys

### Actions

#### 5.1: Load Dimensions

```powershell
sbt "runMain gold.DimensionalModelETL --load-dimensions"
```

**What this creates:**

##### Dim_Date (Generated)
```
📅 Loading Dim_Date...

Generation logic:
  - Start: 2020-01-01
  - End: 2030-12-31 (10 years)
  - Attributes: year, quarter, month, day_of_week, is_weekend, etc.
  
✅ Loaded 3653 date records (10 years)
```

##### Dim_Customer (SCD Type 2)
```
👤 Loading Dim_Customer (SCD Type 2)...

SCD Type 2 logic:
  1. Compare incoming with existing (on customer_id)
  2. If attributes changed:
     - End-date old record (set is_current = false, valid_to = today)
     - Insert new record (set is_current = true, valid_to = 9999-12-31)
  3. If new customer:
     - Insert record (is_current = true, valid_to = 9999-12-31)
  
📊 Change Analysis:
   New Records: 1000
   Changed Records: 0
   
✅ Loaded 1000 customer records to Dim_Customer

Table structure:
┌────────────┬─────────────┬──────┬────────┬──────────┬──────────┬──────────┐
│customer_key│customer_id  │email │status  │valid_from│valid_to  │is_current│
├────────────┼─────────────┼──────┼────────┼──────────┼──────────┼──────────┤
│1           │1            │john@ │ACTIVE  │2025-12-20│9999-12-31│true      │
└────────────┴─────────────┴──────┴────────┴──────────┴──────────┴──────────┘
            ↑ Surrogate key (auto-increment)
```

##### Other Dimensions
```
💳 Loading Dim_Account...
✅ Loaded 2000 account records to Dim_Account

🏢 Loading Dim_Product...
✅ Loaded 12 product records to Dim_Product

🏦 Loading Dim_Branch...
✅ Loaded 10 branch records to Dim_Branch
```

---

#### 5.2: Load Facts

```powershell
sbt "runMain gold.DimensionalModelETL --load-facts"
```

**What this creates:**

##### Fact_Transaction
```
💰 Loading Fact_Transaction...

Logic:
  1. Read bronze.sat_transaction
  2. Lookup dimension keys:
     - customer_key from dim_customer (on customer_id)
     - account_key from dim_account (on account_id)
     - date_key from dim_date (on transaction_date)
  3. Calculate metrics:
     - net_amount = total_amount
     - transaction_count = 1
  4. Write to: gold.fact_transaction
  
✅ Loaded 5000 transaction records to Fact_Transaction

Table structure:
┌──────────────┬────────────┬────────────┬──────────┬────────────┬──────────┐
│transaction_key│customer_key│account_key │date_key  │net_amount  │item_count│
├──────────────┼────────────┼────────────┼──────────┼────────────┼──────────┤
│1             │1           │101         │20251220  │250.00      │3         │
└──────────────┴────────────┴────────────┴──────────┴────────────┴──────────┘
             ↑ All foreign keys to dimensions
```

##### Fact_Account_Balance
```
📊 Loading Fact_Account_Balance...

Logic:
  1. Aggregate daily balances per account
  2. Lookup dimension keys
  3. Write to: gold.fact_account_balance
  
✅ Loaded daily balance snapshots to Fact_Account_Balance
```

---

### Verification

```scala
// Check dimensions
spark.sql("SELECT COUNT(*) FROM gold.dim_customer WHERE is_current = true").show()
// Expected: 1000

spark.sql("SELECT COUNT(*) FROM gold.dim_date").show()
// Expected: 3653

// Check facts
spark.sql("SELECT COUNT(*) FROM gold.fact_transaction").show()
// Expected: 5000

// Run analytics query
spark.sql("""
  SELECT 
    c.customer_id,
    c.full_name,
    COUNT(DISTINCT f.transaction_key) as transaction_count,
    SUM(f.net_amount) as total_spent
  FROM gold.dim_customer c
  JOIN gold.fact_transaction f ON c.customer_key = f.customer_key
  WHERE c.is_current = true
  GROUP BY c.customer_id, c.full_name
  ORDER BY total_spent DESC
  LIMIT 10
""").show()
```

### Validation Checkpoint
✅ **Dimensions:** SCD Type 2 for customer, account, product, branch  
✅ **Facts:** Transaction and account balance metrics  
✅ **Star schema:** Ready for BI tools  

### Transition to Next Stage
**You now have:** Complete analytical data warehouse  
**Next step:** Test schema evolution (the Data Vault superpower)

---

## Stage 6: Schema Evolution Scenario

### Context from Previous Stage
✅ Complete pipeline running (PostgreSQL → Gold)  
✅ 1000 customers with 13 attributes each  

### Purpose of This Stage
Demonstrate Data Vault's killer feature: **automatic schema absorption without breaking queries**.

### Scenario: Marketing Launches Loyalty Program

**Business requirement:** Add `loyalty_tier` to customer (STANDARD, SILVER, GOLD, PLATINUM based on balance).

**Traditional ETL impact:**
- ETL breaks (hardcoded column positions)
- Dashboards fail (missing column)
- Emergency weekend work
- Data loss (old records don't have loyalty_tier value)

**Data Vault approach:**
- New column automatically added to Satellite
- Existing queries still work
- Historical records have NULL for new field
- Zero downtime

### Actions

#### 6.1: Add Column to PostgreSQL

```powershell
psql -U postgres -d banking_source -f source-system\sql\03_add_loyalty_tier.sql
```

**What this does:**
```sql
-- Add new column
ALTER TABLE banking.customer 
ADD COLUMN loyalty_tier VARCHAR(20) DEFAULT 'STANDARD';

-- Calculate loyalty tier based on total account balance
UPDATE banking.customer c
SET loyalty_tier = CASE
  WHEN (SELECT SUM(balance) FROM banking.account WHERE customer_id = c.customer_id) > 100000 THEN 'PLATINUM'
  WHEN (SELECT SUM(balance) FROM banking.account WHERE customer_id = c.customer_id) > 50000 THEN 'GOLD'
  WHEN (SELECT SUM(balance) FROM banking.account WHERE customer_id = c.customer_id) > 10000 THEN 'SILVER'
  ELSE 'STANDARD'
END;

-- Trigger CDC (update timestamp so NiFi detects change)
UPDATE banking.customer SET updated_at = CURRENT_TIMESTAMP;
```

**Verify:**
```sql
psql -U postgres -d banking_source -c "SELECT customer_id, email, loyalty_tier FROM banking.customer LIMIT 5;"
```

---

#### 6.2: Update Avro Schema

Edit `nifi\schemas\customer.avsc`, add new field:

```json
{
  "type": "record",
  "name": "Customer",
  "namespace": "com.banking.source",
  "fields": [
    {"name": "customer_id", "type": "int"},
    {"name": "customer_type", "type": "string"},
    {"name": "first_name", "type": ["null", "string"], "default": null},
    {"name": "last_name", "type": ["null", "string"], "default": null},
    {"name": "email", "type": "string"},
    {"name": "phone", "type": ["null", "string"], "default": null},
    {"name": "address", "type": ["null", "string"], "default": null},
    {"name": "city", "type": ["null", "string"], "default": null},
    {"name": "state", "type": ["null", "string"], "default": null},
    {"name": "zip_code", "type": ["null", "string"], "default": null},
    {"name": "customer_status", "type": "string"},
    {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "updated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {
      "name": "loyalty_tier",
      "type": ["null", "string"],
      "default": null,
      "doc": "Customer loyalty tier: STANDARD, SILVER, GOLD, PLATINUM"
    }
  ]
}
```

**Why `["null", "string"]`?** Makes field optional (handles old records gracefully).

---

#### 6.3: Re-run NiFi Flow

1. **Open NiFi UI:** https://localhost:8443/nifi
2. **Start customer flow** (if stopped)
3. **Wait for execution** (30 seconds)

**What NiFi does:**
- QueryDatabaseTableRecord detects updated_at changes
- Extracts customers with new `loyalty_tier` column
- Validates against updated customer.avsc (14 fields now)
- Writes new Avro files with 14 fields

**Verify:**
```powershell
# Check new Avro files created
dir warehouse\staging\customer\

# Inspect schema (should show 14 fields now)
java -jar avro-tools.jar getschema warehouse\staging\customer\customer_20251220_140000.avro | grep loyalty_tier
```

---

#### 6.4: Re-run Bronze ETL

```powershell
sbt "runMain bronze.RawVaultETL --entity customer"
```

**Watch the output carefully:**

```
📖 READING AVRO FILES
   Path: warehouse/staging/customer/*.avro
   Validation: Enabled
✅ Schema validated: 14 fields (was 13)

⚠️  NEW FIELDS DETECTED (Schema Evolution):
loyalty_tier

IMPACT:
- Fields will be automatically added to Satellite tables
- Existing queries unaffected
- Historical records will have NULL for new fields

🛰️  Loading Sat_Customer...
   Historization: Enabled
   Schema evolution: Detected new column, adding to table
   
📊 Change Analysis:
   Changed Records: 1000 (loyalty_tier updated)
   End-dating old versions (set valid_to = current_timestamp)
   Inserting new versions (with loyalty_tier populated)
   
✅ Loaded 1000 customer records to Sat_Customer
```

**What happened under the hood:**
1. AvroReader detected new field (`loyalty_tier`)
2. Iceberg automatically added column to `sat_customer` table
3. Old records: valid_to set to current_timestamp
4. New records: inserted with valid_from = current_timestamp, loyalty_tier populated

---

#### 6.5: Verify History Preserved

```scala
sbt console

// Check table schema (should have loyalty_tier now)
spark.sql("DESCRIBE bronze.sat_customer").show()

// Query historical data for one customer
spark.sql("""
  SELECT 
    customer_id,
    email,
    customer_status,
    loyalty_tier,
    valid_from,
    valid_to
  FROM bronze.sat_customer
  WHERE customer_id = 1
  ORDER BY valid_from
""").show(truncate = false)
```

**Expected output:**
```
┌────────────┬────────────────┬────────────┬────────────┬──────────────┬──────────────┐
│customer_id │email           │status      │loyalty_tier│valid_from    │valid_to      │
├────────────┼────────────────┼────────────┼────────────┼──────────────┼──────────────┤
│1           │john@example.com│ACTIVE      │NULL        │2025-12-20 10:│2025-12-20 14:│
│            │                │            │            │00:00         │00:00         │
├────────────┼────────────────┼────────────┼────────────┼──────────────┼──────────────┤
│1           │john@example.com│ACTIVE      │GOLD        │2025-12-20 14:│NULL          │
│            │                │            │            │00:00         │              │
└────────────┴────────────────┴────────────┴────────────┴──────────────┴──────────────┘
                                             ↑ NULL (old version)    ↑ GOLD (new version)
```

**Key insight:** We can query "What was customer 1's loyalty tier before noon?" → NULL (didn't exist yet)

---

#### 6.6: Verify Old Queries Still Work

```scala
// This query NEVER referenced loyalty_tier
// It should still work unchanged
spark.sql("""
  SELECT 
    customer_type,
    COUNT(*) as customer_count
  FROM bronze.sat_customer
  WHERE valid_to IS NULL
    AND customer_status = 'ACTIVE'
  GROUP BY customer_type
""").show()
```

**Output:** Exact same as before schema evolution. No breaking changes!

---

#### 6.7: Run New Analytics with Loyalty Tier

```scala
// Now we can use the new column
spark.sql("""
  SELECT 
    loyalty_tier,
    customer_type,
    COUNT(*) as customer_count,
    AVG(account_balance) as avg_balance
  FROM bronze.sat_customer s
  JOIN (
    SELECT 
      customer_hash_key,
      SUM(balance) as account_balance
    FROM bronze.sat_account
    WHERE valid_to IS NULL
    GROUP BY customer_hash_key
  ) a ON s.customer_hash_key = a.customer_hash_key
  WHERE s.valid_to IS NULL
  GROUP BY loyalty_tier, customer_type
  ORDER BY avg_balance DESC
""").show()
```

**Expected output:**
```
┌────────────┬─────────────┬──────────────┬───────────┐
│loyalty_tier│customer_type│customer_count│avg_balance│
├────────────┼─────────────┼──────────────┼───────────┤
│PLATINUM    │INDIVIDUAL   │45            │125000     │
│GOLD        │INDIVIDUAL   │120           │65000      │
│SILVER      │INDIVIDUAL   │300           │22000      │
│STANDARD    │INDIVIDUAL   │535           │5000       │
└────────────┴─────────────┴──────────────┴───────────┘
```

---

### Key Learnings from Schema Evolution

**What Data Vault gave us:**
✅ **Zero downtime:** Pipeline kept running during schema change  
✅ **Backward compatibility:** Old queries still work  
✅ **Forward compatibility:** New queries can use new column  
✅ **Historical accuracy:** Can query "What was the value before change?"  
✅ **Automatic absorption:** No code changes in ETL  

**What would have broken in traditional ETL:**
❌ Hardcoded column positions  
❌ Fixed schema in target tables  
❌ Dashboards expecting 13 columns  
❌ Historical data lost or NULL-backfilled  

---

## PART III: REFERENCE

---

## Quick Commands

### One-Time Setup
```powershell
# Database
psql -U postgres -c "CREATE DATABASE banking_source;"
psql -U postgres -d banking_source -c "CREATE SCHEMA banking;"
psql -U postgres -d banking_source -f source-system\sql\02_create_tables.sql

# Seed data
sbt "runMain seeder.ReferenceDataSeeder"
sbt "runMain seeder.TransactionalDataSeeder"

# Create Data Vault tables
sbt "runMain bronze.RawVaultSchema"
```

### Daily Operations
```powershell
# 1. Run NiFi flows (extract to Avro)
# → Open NiFi UI, start flows manually

# 2. Load Bronze (incremental)
sbt "runMain bronze.RawVaultETL --mode incremental"

# 3. Refresh Silver
sbt "runMain silver.BusinessVaultETL --build-pit"
sbt "runMain silver.BusinessVaultETL --build-bridge"

# 4. Update Gold
sbt "runMain gold.DimensionalModelETL --load-dimensions"
sbt "runMain gold.DimensionalModelETL --load-facts"
```

### Validation
```powershell
# Check Avro files
dir warehouse\staging\customer\

# Check Spark tables
sbt console
spark.sql("SHOW TABLES IN bronze").show()
spark.sql("SELECT COUNT(*) FROM bronze.hub_customer").show()
spark.sql("SELECT COUNT(*) FROM bronze.sat_customer WHERE valid_to IS NULL").show()

# Query Gold layer
spark.sql("SELECT * FROM gold.dim_customer WHERE is_current = true LIMIT 5").show()
spark.sql("SELECT COUNT(*) FROM gold.fact_transaction").show()
```

---

## Troubleshooting

### NiFi Flow Not Creating Avro Files

**Symptom:** No files in `warehouse\staging\customer\`

**Check:**
1. **Database connection enabled?**
   - NiFi UI → Controller Services → DBCPConnectionPool
   - Should have green "ENABLED" status
   
2. **Schema file path correct?**
   - Right-click ConvertRecord → Configure → Properties
   - "Schema File" must be absolute path: `C:\Users\...\nifi\schemas\customer.avsc`
   - Test: `Test-Path "C:\Users\...\nifi\schemas\customer.avsc"` should return True
   
3. **Output directory exists?**
   ```powershell
   # Create if missing
   New-Item -ItemType Directory -Force -Path "warehouse\staging\customer"
   ```

4. **Check NiFi logs:**
   ```powershell
   # View last 50 lines
   Get-Content "C:\nifi\nifi-2.7.2\logs\nifi-app.log" -Tail 50
   ```

---

### Spark Can't Read Avro Files

**Symptom:** `Path does not exist: warehouse/staging/customer/*.avro`

**Check:**
1. **Files actually exist?**
   ```powershell
   dir warehouse\staging\customer\
   # Should show .avro files
   ```

2. **Absolute vs relative path?**
   ```scala
   // Use absolute path
   val path = "C:/Users/sofiane/work/learn-intellij/data-vault-modeling-etl/warehouse/staging/customer/*.avro"
   
   // Or set working directory
   System.setProperty("user.dir", "C:/Users/sofiane/work/learn-intellij/data-vault-modeling-etl")
   ```

3. **Avro dependency in build.sbt?**
   ```scala
   // Should have:
   "org.apache.spark" %% "spark-avro" % "3.5.0"
   ```

---

### Schema Validation Fails

**Symptom:** `Missing required fields: email, customer_status`

**Cause:** Avro file doesn't match expected schema

**Fix:**
1. **Check Avro file schema:**
   ```powershell
   java -jar avro-tools.jar getschema warehouse\staging\customer\customer_*.avro
   ```

2. **Compare with AvroReader expectations:**
   - Look at `src/main/scala/bronze/utils/AvroReader.scala`
   - Function: `getRequiredFieldsForEntity("customer")`
   
3. **Update NiFi schema:**
   - Edit `nifi\schemas\customer.avsc`
   - Add missing fields
   - Re-run NiFi flow

---

### Schema Evolution Not Detected

**Symptom:** New column doesn't appear in Satellite table

**Check:**
1. **Updated Avro schema?**
   - Edit `nifi\schemas\customer.avsc`
   - Add new field with `"default": null`

2. **Re-ran NiFi flow?**
   - NiFi UI, start flow

3. **AvroReader validation enabled?**
   - In `RawVaultETL.scala`, should call:
   ```scala
   AvroReader.readAvro(path, validateSchema = true)
   ```

---

### Incremental Load Not Working

**Symptom:** `sbt "runMain bronze.RawVaultETL --mode incremental"` loads zero records

**Cause:** NiFi's `updated_at` tracking hasn't advanced

**Fix:**
1. **Update source data:**
   ```sql
   psql -U postgres -d banking_source -c "UPDATE banking.customer SET updated_at = CURRENT_TIMESTAMP WHERE customer_id = 1;"
   ```

2. **Re-run NiFi flow** (detects updated_at change)

3. **Then run incremental ETL:**
   ```powershell
   sbt "runMain bronze.RawVaultETL --mode incremental"
   ```
