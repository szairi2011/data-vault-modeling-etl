# Complete Setup Guide - Data Vault 2.0 POC

## 📋 Table of Contents
1. [Quick Start (5 minutes)](#quick-start-5-minutes)
2. [Detailed Setup (Windows Native)](#detailed-setup-windows-native)
3. [Running the ETL Pipeline](#running-the-etl-pipeline)
4. [Schema Evolution Scenario](#schema-evolution-scenario)
5. [Query Examples](#query-examples)
6. [Troubleshooting](#troubleshooting)

---

## Quick Start (5 minutes)

### Prerequisites Check
```powershell
# Verify installations (Windows Native - No Docker Required)
java -version        # Java 11+ (required for Schema Registry)
scala -version       # Scala 2.12.x
sbt version          # SBT 1.9+
psql --version       # PostgreSQL 12+

# Optional: Check if Java is in PATH
$env:JAVA_HOME       # Should point to Java installation
```

**Note**: This setup is **100% Windows native** - no Docker required!

### One-Command Setup
```powershell
# Clone repository and run setup
cd C:\dev\projects\data-vault-modeling-etl
.\scripts\windows\01-setup.ps1
```

**This script will:**
- ✅ Create PostgreSQL database and tables
- ✅ Download and configure Confluent Schema Registry (Windows native)
- ✅ Start Schema Registry process
- ✅ Register all Avro schemas
- ✅ Seed 1,000 customers, 2,000 accounts, 5,000 transactions
- ✅ Create warehouse directory structure

**Expected Duration:** 5-10 minutes (includes Schema Registry download)

**Note**: First-time setup downloads ~200MB Confluent Platform.

---

## Detailed Setup (Windows Native)

### Step 1: PostgreSQL Setup

#### 1.1 Create Database
```powershell
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE banking_source;
\c banking_source
CREATE SCHEMA banking;
\q
```

#### 1.2 Create Tables
```powershell
psql -U postgres -d banking_source -f source-system\sql\02_create_tables.sql
```

#### 1.3 Verify Tables
```sql
psql -U postgres -d banking_source

\dt banking.*

-- Expected output:
-- banking.customer
-- banking.account
-- banking.transaction_header
-- banking.transaction_item
-- banking.product
-- banking.branch
-- banking.category
```

---

### Step 2: Schema Registry Setup (Windows Native)

#### 2.1 Download Confluent Platform (Community Edition)

```powershell
# Download Confluent Platform Community (no Kafka needed, just Schema Registry)
# Visit: https://www.confluent.io/download/

# Or direct download:
$confluentVersion = "7.5.0"
$downloadUrl = "https://packages.confluent.io/archive/7.5/confluent-community-$confluentVersion.zip"
$downloadPath = "$env:USERPROFILE\Downloads\confluent-community-$confluentVersion.zip"

# Download
Invoke-WebRequest -Uri $downloadUrl -OutFile $downloadPath

# Extract to C:\confluent
Expand-Archive -Path $downloadPath -DestinationPath "C:\confluent" -Force

# Set environment variable
[System.Environment]::SetEnvironmentVariable("CONFLUENT_HOME", "C:\confluent\confluent-$confluentVersion", "User")
$env:CONFLUENT_HOME = "C:\confluent\confluent-$confluentVersion"
```

**Alternative: Manual Download**
1. Go to https://www.confluent.io/download/
2. Select "Community" version
3. Choose "Windows" (zip archive)
4. Extract to `C:\confluent\confluent-7.5.0`

---

#### 2.2 Configure Schema Registry (Standalone Mode - No Kafka)

**Problem**: The default `schema-registry.properties` requires Kafka, but we want standalone mode.

**Solution**: Modify the existing config file to use in-memory storage.

**Edit**: `C:\confluent\confluent-7.5.0\etc\schema-registry\schema-registry.properties`

Replace the `kafkastore.bootstrap.servers` line with in-memory storage:

```properties
# BEFORE (requires Kafka - doesn't work for standalone):
# kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092

# AFTER (in-memory storage - no Kafka required):
kafkastore.connection.url=

# This tells Schema Registry to use in-memory storage instead of Kafka
```

**Complete working configuration**:
```properties
# The address the socket server listens on
listeners=http://0.0.0.0:8081

# Use in-memory storage (no Kafka required)
# Leave this blank or comment out the kafkastore.bootstrap.servers line
kafkastore.connection.url=

# The name of the topic (not used with in-memory, but keep for compatibility)
kafkastore.topic=_schemas

# Schema compatibility level
schema.compatibility.level=BACKWARD

# Debugging (set to true if troubleshooting)
debug=false
```

**Alternative: Create a new config file for standalone mode**

If you prefer not to modify the default config, create a new file:  
`C:\confluent\confluent-7.5.0\etc\schema-registry\schema-registry-standalone.properties`

```properties
# Schema Registry - Standalone Mode (No Kafka)
listeners=http://0.0.0.0:8081

# In-memory storage (no Kafka)
kafkastore.connection.url=

# Schema compatibility
schema.compatibility.level=BACKWARD

# Enable CORS (optional, for web clients)
access.control.allow.methods=GET,POST,PUT,DELETE,OPTIONS
access.control.allow.origin=*

# Disable features that require Kafka
master.eligibility=true
```

Then use this config when starting:
```powershell
cd C:\confluent\confluent-7.5.0\bin\windows
.\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry-standalone.properties
```

---

#### 2.3 Install Java (if not already installed)

Schema Registry requires Java 11+.

```powershell
# Check Java version
java -version

# If not installed, download from:
# https://adoptium.net/temurin/releases/
# Choose: Windows x64, JDK 11 or 17
```

---

#### 2.4 Start Schema Registry (Windows Native)

**Important**: Make sure you've configured the properties file (see step 2.2 above).

**Option A: Using Helper Script (Recommended)**

We provide a PowerShell script that manages Schema Registry:

```powershell
# Start Schema Registry
.\scripts\windows\schema-registry.ps1 start

# Check status
.\scripts\windows\schema-registry.ps1 status

# Stop (when needed)
.\scripts\windows\schema-registry.ps1 stop
```

**Option B: Manual Start**

```powershell
cd C:\confluent\confluent-7.5.0\bin\windows

# Use the default config (if you modified it per step 2.2)
.\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry.properties

# OR use the standalone config (if you created a new file)
.\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry-standalone.properties
```

**Note**: The window will stay open showing logs. Keep it running or minimize it.

**Option C: Background Process (Recommended)**

Create a PowerShell script to run Schema Registry in the background:

`C:\confluent\start-schema-registry-background.ps1`:

```powershell
# Start Schema Registry in Background (Windows Native)
$env:CONFLUENT_HOME = "C:\confluent\confluent-7.5.0"

# Adjust JAVA_HOME to your Java installation
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.0.20.8-hotspot"

$schemaRegistryBat = "$env:CONFLUENT_HOME\bin\windows\schema-registry-start.bat"
$schemaRegistryConfig = "$env:CONFLUENT_HOME\etc\schema-registry\schema-registry.properties"

# Start in new minimized window
Start-Process -FilePath $schemaRegistryBat `
              -ArgumentList $schemaRegistryConfig `
              -WindowStyle Minimized

Write-Host "Schema Registry starting in background..."
Write-Host "Check status: .\scripts\windows\schema-registry.ps1 status"
```

**Run:**
```powershell
cd C:\confluent
.\start-schema-registry-background.ps1
```

**Wait 30 seconds for startup, then verify:**
```powershell
# Test with curl
curl http://localhost:8081/

# Or PowerShell
Invoke-RestMethod -Uri "http://localhost:8081/" -Method Get

# Expected response:
# {} or {"error_code":40401,"message":"HTTP 404 Not Found"}
# Both mean Schema Registry is running!
```

**Troubleshooting Startup**:
```powershell
# If Schema Registry doesn't start, check logs
Get-Content "$env:CONFLUENT_HOME\logs\schema-registry.log" -Tail 50

# Common issue: Port already in use
netstat -ano | findstr :8081

# Kill process using port 8081 if needed
Stop-Process -Id <PID> -Force
```

---

#### 2.5 Register Avro Schemas (Windows Native)
```powershell
.\nifi\scripts\register-schemas.ps1
```

**Expected Output:**
```
✅ Registered banking.customer-value
✅ Registered banking.account-value
✅ Registered banking.transaction_header-value
✅ Registered banking.transaction_item-value
```

#### 2.3 Verify Registration
```powershell
curl http://localhost:8081/subjects
# ["banking.customer-value", "banking.account-value", ...]
```

---

### Step 3: Seed Data

#### 3.1 Seed Reference Data
```powershell
sbt "runMain seeder.ReferenceDataSeeder"
```

**What This Creates:**
- 12 Products (checking, savings, credit cards, loans)
- 10 Branches (NYC, SF, Chicago, Boston, etc.)
- 19 Categories (hierarchical structure)

**Verify:**
```sql
psql -U postgres -d banking_source

SELECT * FROM banking.product;
SELECT * FROM banking.branch;
SELECT * FROM banking.category ORDER BY path;
```

#### 3.2 Seed Transactional Data
```powershell
sbt "runMain seeder.TransactionalDataSeeder"
```

**What This Generates:**
- 1,000 customers (900 individuals, 100 businesses)
- ~2,000 accounts (1-3 per customer)
- 5,000 transaction headers (last 90 days)
- ~10,000 transaction items (2-3 items per transaction)

**Verify:**
```sql
SELECT customer_type, COUNT(*) FROM banking.customer GROUP BY customer_type;
-- INDIVIDUAL: 900, BUSINESS: 100

SELECT account_type, COUNT(*) FROM banking.account GROUP BY account_type;
-- CHECKING, SAVINGS, CREDIT_CARD, LOAN distributions

SELECT transaction_type, COUNT(*) FROM banking.transaction_header GROUP BY transaction_type;
-- DEPOSIT, WITHDRAWAL, PAYMENT, TRANSFER distributions
```

---

### Step 4: Initialize Raw Vault

#### 4.1 Create Iceberg Tables
```powershell
sbt "runMain bronze.RawVaultSchema"
```

**This Creates:**
- 5 Hubs: Customer, Account, Transaction, Product, Branch
- 2 Links: Customer-Account, Transaction-Item
- 4 Satellites: Customer, Account, Transaction, Transaction Item
- 1 Metadata table: load_metadata

**Verify:**
```powershell
sbt console

scala> spark.sql("SHOW TABLES IN bronze").show()
```

---

## Running the ETL Pipeline

### Bronze Layer (Raw Vault)

#### Full Load (First Run)
```powershell
sbt "runMain bronze.RawVaultETL --mode full"
```

**Expected Output:**
```
╔════════════════════════════════════════════════════════════════╗
║         DATA VAULT 2.0 - RAW VAULT ETL (BRONZE LAYER)         ║
╚════════════════════════════════════════════════════════════════╝

Configuration:
  Mode: full (full|incremental)
  Entity: all

🚀 Initializing Spark Session with Iceberg & Hive Metastore...
✅ Spark 3.5.0 initialized

┌────────────────────────────────────────────────────────────────┐
│ PROCESSING CUSTOMER ENTITY                                     │
└────────────────────────────────────────────────────────────────┘

📖 READING AVRO FILES
   Path: warehouse/staging/customer/*.avro
   Validation: Enabled

✅ Schema validated: 13 fields
📊 Records read: 1000

📦 Loading Hub_Customer...
✅ Loaded 1000 new customers to Hub_Customer

🛰️  Loading Sat_Customer...
✅ Loaded 1000 customer records to Sat_Customer

✅ Load completed successfully
```

#### Incremental Load (Subsequent Runs)
```powershell
sbt "runMain bronze.RawVaultETL --mode incremental"
```

**Only new/changed records since last run are loaded.**

#### Load Specific Entity
```powershell
sbt "runMain bronze.RawVaultETL --entity customer"
sbt "runMain bronze.RawVaultETL --entity account"
sbt "runMain bronze.RawVaultETL --entity transaction"
```

---

## Schema Evolution Scenario

### 🎯 **Scenario: Adding Loyalty Program**

**Business Context:**
Marketing launches a loyalty program. The source system adds a `loyalty_tier` column to the customer table. Existing dashboards must continue working, while new dashboards need the loyalty data.

---

### **Act 1: Current State (Before Evolution)**

#### Source Schema V1
```sql
-- PostgreSQL: banking.customer (Before)
customer_id SERIAL PRIMARY KEY
customer_type VARCHAR(20) NOT NULL
first_name VARCHAR(50)
last_name VARCHAR(50)
email VARCHAR(100) NOT NULL
customer_status VARCHAR(20) NOT NULL
updated_at TIMESTAMP NOT NULL
-- NO loyalty_tier yet
```

#### Run Initial ETL
```powershell
# Load initial data
sbt "runMain bronze.RawVaultETL --entity customer"
```

#### Query Current Data
```sql
-- Spark SQL
SELECT 
  c.customer_id,
  s.email,
  s.customer_status
FROM bronze.hub_customer c
JOIN bronze.sat_customer s ON c.customer_hash_key = s.customer_hash_key
WHERE s.valid_to IS NULL  -- Current records only
LIMIT 10;

-- Output: NO loyalty_tier column
```

---

### **Act 2: Schema Change (Add Loyalty Tier)**

#### 2.1 Update Source System
```sql
-- Run this in PostgreSQL
psql -U postgres -d banking_source -f source-system\sql\03_add_loyalty_tier.sql
```

**This SQL file contains:**
```sql
-- Add new column
ALTER TABLE banking.customer 
ADD COLUMN loyalty_tier VARCHAR(20) DEFAULT 'STANDARD';

-- Populate based on account balance
UPDATE banking.customer c
SET loyalty_tier = CASE
    WHEN (SELECT SUM(balance) FROM banking.account WHERE customer_id = c.customer_id) > 100000 THEN 'PLATINUM'
    WHEN (SELECT SUM(balance) FROM banking.account WHERE customer_id = c.customer_id) > 50000 THEN 'GOLD'
    WHEN (SELECT SUM(balance) FROM banking.account WHERE customer_id = c.customer_id) > 10000 THEN 'SILVER'
    ELSE 'STANDARD'
END;

-- Update updated_at to trigger CDC
UPDATE banking.customer SET updated_at = CURRENT_TIMESTAMP;
```

#### 2.2 Verify Source Change
```sql
psql -U postgres -d banking_source

SELECT customer_id, email, loyalty_tier 
FROM banking.customer 
LIMIT 10;

-- Output now includes loyalty_tier column
```

---

### **Act 3: Automatic Schema Absorption**

#### 3.1 Register Updated Avro Schema (V2)

**Create:** `nifi/schemas/customer_v2.avsc`
```json
{
  "type": "record",
  "name": "Customer",
  "namespace": "com.banking.source",
  "fields": [
    ...existing fields...,
    {
      "name": "loyalty_tier",
      "type": ["null", "string"],
      "default": null,
      "doc": "Added in V2 - Customer loyalty tier (STANDARD, SILVER, GOLD, PLATINUM)"
    }
  ]
}
```

**Register:**
```powershell
# Schema Registry auto-validates backward compatibility
.\nifi\scripts\register-schemas.ps1
```

**Expected Output:**
```
✅ Schema banking.customer-value V2 registered
   Compatibility: BACKWARD (safe to deploy)
```

#### 3.2 NiFi CDC Extracts New Schema

**NiFi automatically:**
1. Detects new column in QueryDatabaseTable
2. Converts to Avro with new field
3. Writes to staging with V2 schema

**Verify Staged Files:**
```powershell
# Check Avro schema version
java -jar avro-tools.jar getschema warehouse\staging\customer\customer_20250119_100000.avro

# Should show loyalty_tier field
```

#### 3.3 Run Bronze ETL (Automatic Absorption)
```powershell
sbt "runMain bronze.RawVaultETL --entity customer"
```

**Expected Output:**
```
📖 READING AVRO FILES
✅ Schema validated: 14 fields (was 13)

⚠️  NEW FIELDS DETECTED (Schema Evolution):
loyalty_tier

IMPACT:
- Fields will be automatically added to Satellite tables
- Existing queries unaffected
- Historical records will have NULL for new fields

🛰️  Loading Sat_Customer...
✅ Loaded 1000 changed customer records to Sat_Customer
```

---

### **Act 4: Data Vault Resilience Demonstrated**

#### 4.1 Query Historical Data (Before Evolution)
```sql
-- Spark SQL: Time travel to before schema change
SELECT 
  customer_id,
  email,
  customer_status,
  loyalty_tier  -- This will be NULL for old records
FROM bronze.sat_customer
WHERE customer_hash_key = 'abc123...'
  AND valid_from < TIMESTAMP '2025-01-19 10:00:00'
ORDER BY valid_from;

-- Result: Historical accuracy preserved
-- Row 1 (2025-01-15): loyalty_tier = NULL (didn't exist yet)
-- Row 2 (2025-01-19): loyalty_tier = 'GOLD' (new version)
```

#### 4.2 Query Current Data (After Evolution)
```sql
-- Current state includes new field
SELECT 
  c.customer_id,
  s.email,
  s.customer_status,
  s.loyalty_tier  -- Now populated
FROM bronze.hub_customer c
JOIN bronze.sat_customer s ON c.customer_hash_key = s.customer_hash_key
WHERE s.valid_to IS NULL
LIMIT 10;

-- Output:
-- customer_id | email              | status | loyalty_tier
-- 1           | john@example.com   | ACTIVE | GOLD
-- 2           | jane@example.com   | ACTIVE | PLATINUM
```

#### 4.3 Existing Queries Still Work
```sql
-- This query NEVER referenced loyalty_tier
-- It continues working unchanged
SELECT 
  customer_type,
  COUNT(*) as customer_count
FROM bronze.sat_customer
WHERE valid_to IS NULL
  AND customer_status = 'ACTIVE'
GROUP BY customer_type;

-- Output: EXACT SAME AS BEFORE
-- No breaking changes!
```

#### 4.4 New Analytics Enabled
```sql
-- New loyalty analysis (only possible after evolution)
SELECT 
  loyalty_tier,
  customer_type,
  COUNT(*) as customer_count,
  AVG(total_balance) as avg_balance
FROM bronze.sat_customer s
JOIN (
  SELECT 
    customer_hash_key,
    SUM(balance) as total_balance
  FROM bronze.sat_account
  WHERE valid_to IS NULL
  GROUP BY customer_hash_key
) a ON s.customer_hash_key = a.customer_hash_key
WHERE s.valid_to IS NULL
GROUP BY loyalty_tier, customer_type;

-- Result:
-- loyalty_tier | customer_type | count | avg_balance
-- PLATINUM     | INDIVIDUAL    | 45    | $125,000
-- GOLD         | INDIVIDUAL    | 120   | $65,000
-- SILVER       | INDIVIDUAL    | 300   | $22,000
-- STANDARD     | INDIVIDUAL    | 535   | $5,000
```

---

### **Summary: Data Vault 2.0 Benefits**

| Traditional Star Schema | Data Vault 2.0 |
|------------------------|----------------|
| ❌ Schema change breaks ETL | ✅ Automatic absorption |
| ❌ Dashboards go down | ✅ Zero downtime |
| ❌ Historical data lost | ✅ Full audit trail |
| ❌ Emergency weekend work | ✅ Controlled release |
| ❌ "Big bang" deployment | ✅ Agile iterations |

**Key Takeaway:** Data Vault acts as a shock absorber between source systems and analytics, allowing both to evolve independently.

---

## Query Examples

### Customer 360 View
```sql
SELECT 
  c.customer_id,
  sc.first_name,
  sc.last_name,
  sc.email,
  sc.customer_status,
  sc.loyalty_tier,
  COUNT(DISTINCT a.account_id) as account_count,
  SUM(sa.balance) as total_balance,
  COUNT(DISTINCT t.transaction_id) as transaction_count
FROM bronze.hub_customer c
JOIN bronze.sat_customer sc ON c.customer_hash_key = sc.customer_hash_key AND sc.valid_to IS NULL
LEFT JOIN bronze.link_customer_account lca ON c.customer_hash_key = lca.customer_hash_key
LEFT JOIN bronze.hub_account a ON lca.account_hash_key = a.account_hash_key
LEFT JOIN bronze.sat_account sa ON a.account_hash_key = sa.account_hash_key AND sa.valid_to IS NULL
LEFT JOIN bronze.hub_transaction t ON a.account_hash_key = t.account_hash_key  -- Simplified join
WHERE c.customer_id = 123
GROUP BY c.customer_id, sc.first_name, sc.last_name, sc.email, sc.customer_status, sc.loyalty_tier;
```

### Time Travel Query
```sql
-- Query customer data as it was on specific date
SELECT 
  customer_id,
  email,
  customer_status,
  loyalty_tier
FROM bronze.sat_customer
WHERE customer_hash_key = 'abc123...'
  AND valid_from <= TIMESTAMP '2025-01-15 12:00:00'
  AND (valid_to > TIMESTAMP '2025-01-15 12:00:00' OR valid_to IS NULL);
```

### Multi-Item Transaction Analysis
```sql
SELECT 
  th.transaction_number,
  th.transaction_date,
  th.total_amount,
  COUNT(ti.item_id) as item_count,
  STRING_AGG(ti.merchant_name, ', ') as merchants
FROM bronze.hub_transaction th
JOIN bronze.sat_transaction st ON th.transaction_hash_key = st.transaction_hash_key AND st.valid_to IS NULL
JOIN bronze.link_transaction_item lti ON th.transaction_hash_key = lti.transaction_hash_key
JOIN bronze.sat_transaction_item ti ON lti.transaction_item_hash_key = ti.transaction_item_hash_key AND ti.valid_to IS NULL
WHERE th.transaction_id BETWEEN 1 AND 100
GROUP BY th.transaction_number, th.transaction_date, th.total_amount
HAVING COUNT(ti.item_id) > 1
ORDER BY th.transaction_date DESC;
```

---

## Troubleshooting

### Issue 1: PostgreSQL Connection Failed
```
Error: could not connect to server
```

**Solution:**
```powershell
# Check if PostgreSQL service is running
Get-Service -Name "postgresql-x64-16"

# Start if stopped
Start-Service "postgresql-x64-16"

# Verify connection
psql -U postgres -c "SELECT version();"
```

---

### Issue 2: Schema Registry Not Starting
```
Error: Connection refused to localhost:8081
```

**Solution (Windows Native):**
```powershell
# Check if Schema Registry process is running
Get-Process | Where-Object {$_.ProcessName -like "*schema-registry*"}

# Check if port 8081 is in use
netstat -ano | findstr :8081

# If running but not responding, kill and restart
Stop-Process -Name "java" -Force  # (if Schema Registry is the only Java process)

# Restart Schema Registry
cd C:\confluent\confluent-7.5.0\bin\windows
.\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry.properties

# Check logs
Get-Content "$env:CONFLUENT_HOME\logs\schema-registry.log" -Tail 50

# Verify connectivity
curl http://localhost:8081/
# or
Invoke-RestMethod -Uri "http://localhost:8081/" -Method Get
```

**Common Issues:**

1. **Port 8081 Already in Use**
   ```powershell
   # Find process using port 8081
   netstat -ano | findstr :8081
   # Kill the process (replace PID with actual process ID)
   Stop-Process -Id <PID> -Force
   ```

2. **Java Not Found**
   ```powershell
   # Set JAVA_HOME
   [System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-11.0.20.8-hotspot", "User")
   $env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.0.20.8-hotspot"
   ```

3. **Confluent Home Not Set**
   ```powershell
   $env:CONFLUENT_HOME = "C:\confluent\confluent-7.5.0"
   ```

---

### Issue 3: Derby Lock Error
```
Error: Another instance is already booted in the directory
```

**Solution:**
```powershell
# Only one Spark session can use embedded Derby at a time
# Close any existing Spark sessions

# If persists, remove lock files:
Remove-Item metastore_db\*.lck -Force
```

---

### Issue 4: Avro Files Not Found
```
Error: Path does not exist: warehouse/staging/customer/*.avro
```

**Solution:**
```powershell
# Check if NiFi created files
dir warehouse\staging\customer\

# If empty, configure NiFi:
# 1. Open NiFi UI: http://localhost:8080/nifi
# 2. Import template: nifi\templates\01_source_to_staging.xml
# 3. Configure PutFile processor with correct path
# 4. Start processors
```

---

### Issue 5: SBT Compilation Errors
```
Error: not found: object bronze
```

**Solution:**
```powershell
# Clean and recompile
sbt clean
sbt compile

# If still fails, check dependencies
sbt update
```

---

## Next Steps

1. ✅ **You've completed Bronze Layer!**
2. 🔄 **Test Schema Evolution** - Follow the scenario above
3. 📊 **Build Silver Layer** - PIT tables for performance
4. ⭐ **Build Gold Layer** - Dimensional model for BI
5. 🔍 **Create Semantic Layer** - Business-friendly views

**For Silver/Gold implementation, see:**
- `docs/02_silver_layer_guide.md` (coming soon)
- `docs/03_gold_layer_guide.md` (coming soon)

---

**Questions?** Check:
- `PROJECT_STATUS.md` for remaining tasks
- `IMPLEMENTATION_SUMMARY.md` for what's ready
- Inline code comments for deep explanations

