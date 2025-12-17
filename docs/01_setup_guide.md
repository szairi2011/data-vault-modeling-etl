# Data Vault 2.0 Banking POC - System Setup Guide

## Overview
This guide walks you through setting up the complete Data Vault 2.0 banking proof-of-concept on your local Windows machine. The project demonstrates end-to-end implementation from source system to dimensional model using Scala/Spark and Apache Iceberg.

---

## Prerequisites

### 1. Java Development Kit (JDK) 8 or 11
```bash
# Check if Java is installed
java -version

# Required: Java 8 or 11 (Spark 3.5 compatibility)
# Download from: https://adoptium.net/
```

### 2. PostgreSQL 12+ (Source System Database)
```bash
# Download PostgreSQL for Windows
# URL: https://www.postgresql.org/download/windows/
# Install with default port 5432
# Set password for postgres user during installation

# Verify installation
psql --version
```

### 3. Scala Build Tool (SBT)
```bash
# Download SBT for Windows
# URL: https://www.scala-sbt.org/download.html

# Verify installation
sbt --version
```

### 4. Apache NiFi (Optional - for ingestion pipeline)
```bash
# Download Apache NiFi
# URL: https://nifi.apache.org/download.html
# Extract to: C:\nifi

# Start NiFi
cd C:\nifi
bin\run-nifi.bat

# Access UI at: http://localhost:8080/nifi
# Default credentials: Check conf\login-identity-providers.xml
```

---

## Project Setup

### Step 1: Clone/Navigate to Project Directory
```powershell
cd C:\Users\sofiane\work\learn-intellij\data-vault-modeling-etl
```

### Step 2: Download Dependencies
```powershell
# This downloads all required libraries (Spark, Iceberg, PostgreSQL driver, etc.)
sbt update
```

### Step 3: Create PostgreSQL Source Database
```powershell
# Connect to PostgreSQL as postgres user
psql -U postgres

# Run database creation script
\i source-system/sql/01_create_database.sql

# Exit psql
\q
```

### Step 4: Create Source System Tables
```powershell
# Connect to banking_source database
psql -U postgres -d banking_source

# Run table creation script
\i source-system/sql/02_create_tables.sql

# Exit psql
\q
```

### Step 5: Seed Reference Data
```powershell
# Run reference data seeder (categories, branches, products)
sbt "runMain seeder.ReferenceDataSeeder"

# Expected output:
# [1/3] Seeding transaction categories...
#   ✓ Inserted 8 parent categories
#   ✓ Inserted 11 child categories
# [2/3] Seeding bank branches...
#   ✓ Inserted 10 branches
# [3/3] Seeding banking products...
#   ✓ Inserted 12 products
```

### Step 6: Seed Transactional Data
```powershell
# Run transactional data seeder (customers, accounts, transactions)
sbt "runMain seeder.TransactionalDataSeeder"

# Expected output:
# [Step 1/4] Loading reference data...
# [Step 2/4] Seeding customers...
#   ✓ Total customers created: 1000
# [Step 3/4] Seeding accounts...
#   ✓ Total accounts created: ~2000
# [Step 4/4] Seeding transactions with items...
#   ✓ Total transactions created: 5000
#   ✓ Average items per transaction: 1.85
#   ✓ Transactions with multiple items: 2100 (42%)
```

---

## Verify Data

### Check PostgreSQL Data
```sql
-- Connect to database
psql -U postgres -d banking_source

-- Set schema
SET search_path TO banking;

-- Verify counts
SELECT 'customers' as table_name, COUNT(*) as row_count FROM customer
UNION ALL
SELECT 'accounts', COUNT(*) FROM account
UNION ALL
SELECT 'transactions', COUNT(*) FROM transaction_header
UNION ALL
SELECT 'transaction_items', COUNT(*) FROM transaction_item;

-- View sample multi-item transaction
SELECT 
    th.transaction_number,
    th.transaction_type,
    th.total_amount,
    ti.item_sequence,
    ti.item_amount,
    ti.item_description,
    ti.merchant_name
FROM transaction_header th
JOIN transaction_item ti ON th.transaction_id = ti.transaction_id
WHERE th.transaction_id = (
    SELECT transaction_id 
    FROM transaction_item 
    GROUP BY transaction_id 
    HAVING COUNT(*) > 1 
    LIMIT 1
)
ORDER BY ti.item_sequence;
```

Expected output:
```
 transaction_number | transaction_type | total_amount | item_sequence | item_amount | item_description | merchant_name 
--------------------+------------------+--------------+---------------+-------------+------------------+---------------
 TXN-2025-000045   | PAYMENT          |       285.50 |             1 |      120.00 | Electric bill    | Con Edison
 TXN-2025-000045   | PAYMENT          |       285.50 |             2 |       65.50 | Water bill       | Water Dept
 TXN-2025-000045   | PAYMENT          |       285.50 |             3 |      100.00 | Internet bill    | Comcast
```

---

## Project Structure

```
data-vault-modeling-etl/
├── build.sbt                           # SBT project configuration
├── project/
│   └── build.properties                # SBT version
├── src/
│   └── main/
│       ├── scala/
│       │   ├── seeder/                 # Data generation
│       │   │   ├── ReferenceDataSeeder.scala
│       │   │   └── TransactionalDataSeeder.scala
│       │   ├── bronze/                 # Raw Vault ETL
│       │   ├── silver/                 # Business Vault ETL
│       │   ├── gold/                   # Dimensional Model ETL
│       │   └── semantic/               # Semantic Layer
│       └── resources/
│           └── hive-site.xml           # Hive metastore config
├── source-system/
│   └── sql/
│       ├── 01_create_database.sql      # Database setup
│       └── 02_create_tables.sql        # Schema definition
├── nifi-flows/                         # NiFi templates (to be added)
├── warehouse/                          # Iceberg table storage
│   ├── bronze/                         # Raw Vault layer
│   ├── silver/                         # Business Vault layer
│   └── gold/                           # Dimensional Model layer
└── docs/
    ├── 01_setup_guide.md               # This file
    ├── 02_erm_models.md                # ERD diagrams
    ├── 03_architecture.md              # Architecture overview
    └── 04_semantic_layer.md            # Semantic layer guide
```

---

## Common Issues and Solutions

### Issue 1: PostgreSQL Connection Refused
```
Error: FATAL: password authentication failed for user "postgres"
```

**Solution:**
1. Check PostgreSQL service is running: `services.msc` → find PostgreSQL service
2. Verify password in connection string matches installation password
3. Check pg_hba.conf allows local connections

### Issue 2: SBT Cannot Resolve Dependencies
```
Error: unresolved dependency: org.apache.spark#spark-core_2.12
```

**Solution:**
1. Check internet connection
2. Clear SBT cache: `sbt clean cleanFiles`
3. Try again: `sbt update`

### Issue 3: Out of Memory Error
```
Error: Java heap space
```

**Solution:**
Add to `build.sbt`:
```scala
javaOptions += "-Xmx4G"
```

Or run with: `sbt -mem 4096 "runMain ..."`

### Issue 4: Hive Metastore Lock Error
```
Error: derby.log (The process cannot access the file because it is being used by another process)
```

**Solution:**
1. Close any running Spark sessions
2. Delete `metastore_db` directory
3. Restart application

---

## Next Steps

After completing the setup:

1. **Explore the source data**: Query PostgreSQL to understand the normalized schema
2. **Review ERD models**: See `docs/02_erm_models.md` for visual representations
3. **Run Raw Vault ETL**: Create bronze layer tables from source
4. **Run Business Vault ETL**: Create silver layer with PITs and bridges
5. **Run Dimensional Model ETL**: Create gold layer star schema
6. **Query Semantic Layer**: Use business-friendly views and metrics

---

## Useful Commands

```powershell
# Start PostgreSQL
net start postgresql-x64-14

# Stop PostgreSQL
net stop postgresql-x64-14

# Connect to database
psql -U postgres -d banking_source

# Compile Scala code
sbt compile

# Run specific Scala object
sbt "runMain package.ClassName"

# Open SBT console
sbt console

# Clean build artifacts
sbt clean

# Package as JAR
sbt package
```

---

## Support and Resources

- **Spark Documentation**: https://spark.apache.org/docs/3.5.0/
- **Iceberg Documentation**: https://iceberg.apache.org/docs/latest/
- **Data Vault 2.0 Book**: https://danlinstedt.com/
- **PostgreSQL Documentation**: https://www.postgresql.org/docs/

---

**Next Document**: [ERD Models](02_erm_models.md) - View the entity relationship diagrams for all four layers

