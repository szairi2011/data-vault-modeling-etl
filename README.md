# Banking Data Vault 2.0 POC

A comprehensive end-to-end implementation of Data Vault 2.0 methodology for a fictitious banking system, demonstrating modern data warehousing best practices with schema evolution resilience.

---

## 📊 What You'll Build

```
PostgreSQL (Source) → NiFi (Extract + Validate) → Avro Files (Staging)
    ↓
Spark ETL → Bronze Layer (Raw Vault: Hubs, Links, Satellites)
    ↓
Spark ETL → Silver Layer (PIT Tables, Bridge Tables)
    ↓
Spark ETL → Gold Layer (Star Schema: Dimensions, Facts)
    ↓
BI Tools (Tableau, Power BI, SQL Clients)
```

### Key Features
- ✅ Data Vault 2.0 modeling (Hubs, Links, Satellites)
- ✅ Multi-item transactions (e-commerce pattern)
- ✅ Schema evolution handling (add columns without breaking queries)
- ✅ Incremental CDC (extract only changed data via NiFi)
- ✅ SCD Type 2 (track dimension history)
- ✅ Full audit trail with Apache Iceberg

---

## 🎯 Prerequisites

Before starting, verify you have:

```powershell
java -version        # Java 11+ (for Spark)
scala -version       # Scala 2.12.x
sbt version          # SBT 1.9+
psql --version       # PostgreSQL 12+
```

**NiFi:** Apache NiFi 2.7.2 running at `https://localhost:8443/nifi`  
**Platform:** Windows (native setup, no Docker required)

---

## 🚀 Quick Start

```powershell
# 1. Create database and seed data
psql -U postgres -c "CREATE DATABASE banking_source;"
psql -U postgres -d banking_source -f source-system\sql\02_create_tables.sql
sbt "runMain seeder.TransactionalDataSeeder"

# 2. Extract with NiFi (import template from nifi-flows/, start flow in UI)

# 3. Load Data Vault
sbt "runMain bronze.RawVaultSchema"
sbt "runMain bronze.RawVaultETL --mode full"

# 4. Build analytics layers
sbt "runMain silver.BusinessVaultETL --build-pit"
sbt "runMain gold.DimensionalModelETL --load-dimensions"
```

**Full walkthrough:** See documentation below.

---

## 📚 Documentation

### For Setup & Execution
**→ [Complete Setup & Execution Guide](docs/setup_guide.md)**
- Step-by-step instructions for all pipeline stages
- NiFi flow configuration (JSON ↔ Avro transformations)
- Data Vault ETL execution
- Schema evolution scenarios
- Troubleshooting

**Start here if:** You want to run the pipeline end-to-end.

---

### For Architecture & Design
**→ [Architecture & Design Guide](docs/architecture.md)**
- Data flow architecture diagrams
- Source system ERM models
- Data Vault model (Hubs, Links, Satellites)
- Dimensional model (Star schema)
- Semantic layer views
- Design decisions and rationale

**Start here if:** You want to understand the design before executing.

---

## 📁 Project Structure

```
data-vault-modeling-etl/
├── docs/
│   ├── setup_guide.md           ← Execution guide
│   └── architecture.md          ← Design guide
├── nifi/
│   ├── schemas/                 ← Avro schema files (.avsc)
│   └── scripts/                 ← Validation scripts
├── nifi-flows/                  ← NiFi templates (import these)
├── src/main/scala/
│   ├── bronze/                  ← Raw Vault ETL
│   ├── silver/                  ← Business Vault ETL
│   ├── gold/                    ← Dimensional Model ETL
│   └── seeder/                  ← Data generation
└── warehouse/
    ├── staging/                 ← Avro files (NiFi output)
    ├── bronze/                  ← Iceberg tables (Data Vault)
    ├── silver/                  ← Iceberg tables (PIT, Bridge)
    └── gold/                    ← Iceberg tables (Star Schema)
```

---

## 🎓 Learning Objectives

This project teaches:
1. **Data Vault 2.0** - Industry standard for enterprise data warehouses
2. **NiFi Data Ingestion** - Schema validation and CDC patterns
3. **Avro Schema Management** - Type-safe data exchange
4. **Spark ETL** - Scalable data processing with Iceberg
5. **Multi-Layer Architecture** - Bronze → Silver → Gold pattern
6. **Schema Evolution** - Handle source changes gracefully

---

## 💡 Key Concepts

### Data Vault Pattern
```
Hub_Customer (unique customers) + Sat_Customer (attributes with history)
Link_Customer_Account (relationships) + metadata (load tracking)
= Resilient to source changes, full audit trail
```

### Schema Evolution
```
Source adds loyalty_tier column → NiFi validates → Spark absorbs
Old queries: still work | New queries: can use loyalty_tier
Historical records: loyalty_tier = NULL
```

### Multi-Item Transactions
```
Transaction TXN-001 ($250)
  ├─ Item 1: Electricity Bill ($100)
  ├─ Item 2: Water Bill ($50)
  └─ Item 3: Internet Bill ($100)
```

---

## 🤝 Use This Project To

- **Learn** data engineering end-to-end
- **Refresh** your knowledge before interviews
- **Practice** with executable, real-world scenarios
- **Reference** when building similar pipelines

---

## 📄 License

MIT License - Free for learning and reference.

---

**Ready to start?** Choose your path:
- **Execution:** [Setup Guide](docs/setup_guide.md)
- **Understanding:** [Architecture Guide](docs/architecture.md)

This proof-of-concept demonstrates:

1. **Data Vault 2.0 Modeling** - Hub, Link, and Satellite patterns
2. **Multi-Item Transactions** - E-commerce style transactions with line items
3. **Schema Evolution Handling** - How Data Vault absorbs source system changes
4. **Layered Architecture** - Bronze (Raw Vault) → Silver (Business Vault) → Gold (Dimensional)
5. **Apache Iceberg Integration** - Modern table format with ACID guarantees
6. **Semantic Layer** - Business-friendly query interface

## 🏗️ Architecture

```
Source System (PostgreSQL)          Bronze Layer (Raw Vault)
  3NF Normalized                      Apache Iceberg Tables
        ↓                                     ↓
    ┌───────┐                         ┌──────────────┐
    │Customer│                        │ Hub_Customer │
    │Account │    →  NiFi CDC  →     │ Sat_Customer │
    │Transact│                        │Link_Cust_Acct│
    └───────┘                         └──────────────┘
                                              ↓
                                    Silver Layer (Business Vault)
                                      PIT Tables & Bridges
                                              ↓
                                    Gold Layer (Dimensional)
                                      Star Schema for BI
                                              ↓
                                    Semantic Layer
                                      Business Metrics & Views
```

## 📊 Key Features

### Multi-Item Transactions (E-Commerce Pattern)

Unlike traditional banking systems where one transaction = one entry, this POC models transactions like e-commerce orders:

**Example**: Bill Payment Transaction
```
Transaction Header: TXN-2025-000123
  Total: $250.00
  Items:
    1. Electricity Bill - $100.00 (Con Edison)
    2. Water Bill - $50.00 (Water Dept)
    3. Internet Bill - $100.00 (Comcast)
```

This pattern demonstrates how Data Vault handles one-to-many relationships effectively.

### Schema Evolution Resilience

When the source system adds a new field (e.g., `loyalty_tier`):
- ✅ **Raw Vault**: Automatically captured in new satellite records
- ✅ **Business Vault**: Updated in PIT tables on rebuild
- ✅ **Dimensional Model**: Added when business is ready
- ✅ **Existing Reports**: Continue working without breaking

## ✨ **PROJECT STATUS: 64% COMPLETE - Bronze Layer Ready!**

### 🎉 What's Implemented
- ✅ **Complete Bronze Layer (Raw Vault)** with 5 Hubs, 2 Links, 4 Satellites
- ✅ **Apache Iceberg** integration with ACID transactions & time travel
- ✅ **Hive Metastore** catalog support (embedded Derby)
- ✅ **Avro + Schema Registry** for CDC with evolution support
- ✅ **Comprehensive utilities** (Hash keys, Load metadata, Iceberg writer)
- ✅ **Complete automation** (setup, ETL runner, cleanup scripts)
- ✅ **Schema evolution scenario** (loyalty tier demo)
- ✅ **Build system** with all dependencies configured

### 📚 **Essential Documentation**
- **[CURRENT_STATUS.md](CURRENT_STATUS.md)** ⭐ **START HERE** - What's ready NOW
- **[docs/01_setup_guide.md](docs/01_setup_guide.md)** - Complete setup with schema evolution
- **[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)** - Technical deep dive
- **[PROJECT_STATUS.md](PROJECT_STATUS.md)** - File-by-file progress (64%)
- **[QUICKSTART.md](QUICKSTART.md)** - 5-minute quick start

---

## 🚀 Quick Start

### Prerequisites

- PostgreSQL 12+
- Java JDK 8 or 11
- SBT (Scala Build Tool)
- 4GB RAM minimum

### Setup (5 minutes)

```bash
# 1. Create source database
psql -U postgres -f source-system/sql/01_create_database.sql

# 2. Create tables
psql -U postgres -d banking_source -f source-system/sql/02_create_tables.sql

# 3. Seed reference data
sbt "runMain seeder.ReferenceDataSeeder"

# 4. Seed transactional data
sbt "runMain seeder.TransactionalDataSeeder"

# 5. Verify data
psql -U postgres -d banking_source -c "SELECT COUNT(*) FROM banking.customer;"
```

Expected result: 1000 customers, ~2000 accounts, 5000 transactions with ~10,000 items

### Run ETL Pipeline

```bash
# Bronze Layer - Raw Vault
sbt "runMain bronze.RawVaultETL"

# Silver Layer - Business Vault
sbt "runMain silver.BusinessVaultETL"

# Gold Layer - Dimensional Model
sbt "runMain gold.DimensionalModelETL"

# Query Semantic Layer
sbt "runMain semantic.QueryInterface"
```

## 📁 Project Structure

```
data-vault-modeling-etl/
├── README.md                          # This file
├── build.sbt                          # SBT configuration
├── docs/                              # Documentation
│   ├── 01_setup_guide.md             # Detailed setup instructions
│   ├── 02_erm_models.md              # All 4 ERD models
│   ├── 03_architecture.md            # Architecture deep dive
│   └── 04_semantic_layer.md          # Semantic layer guide
├── source-system/                     # PostgreSQL source
│   └── sql/
│       ├── 01_create_database.sql
│       └── 02_create_tables.sql
├── src/main/scala/
│   ├── seeder/                       # Data generation
│   │   ├── ReferenceDataSeeder.scala
│   │   └── TransactionalDataSeeder.scala
│   ├── bronze/                       # Raw Vault ETL
│   │   ├── RawVaultSchema.scala
│   │   └── RawVaultETL.scala
│   ├── silver/                       # Business Vault ETL
│   │   └── BusinessVaultETL.scala
│   ├── gold/                         # Dimensional Model ETL
│   │   └── DimensionalModelETL.scala
│   └── semantic/                     # Semantic Layer
│       ├── SemanticModel.scala
│       └── QueryInterface.scala
├── src/main/resources/
│   └── hive-site.xml                 # Hive metastore config
└── warehouse/                        # Iceberg tables
    ├── bronze/                       # Raw Vault
    ├── silver/                       # Business Vault
    └── gold/                         # Dimensional Model
```

## 🎓 Learning Objectives

### 1. Source System Modeling (3NF)
- Normalized relational design
- Parent-child relationships (transaction header/items)
- CDC tracking via timestamps
- Business keys for integration

### 2. Raw Vault (Data Vault 2.0)
- **Hubs**: Business entities (Customer, Account, Transaction)
- **Links**: Relationships (Customer-Account, Transaction-Item)
- **Satellites**: Descriptive attributes with full history
- **Hash keys**: MD5 hashing for performance
- **Immutability**: Insert-only, never update/delete

### 3. Business Vault
- **PIT Tables**: Point-in-Time snapshots for efficient querying
- **Bridges**: Pre-joined many-to-many relationships
- **Reference Tables**: Business hierarchies and classifications

### 4. Dimensional Model (Star Schema)
- **Fact Tables**: Measurable events (transactions, balances)
- **Dimension Tables**: Descriptive context (customer, product, date)
- **Type 2 SCD**: Slowly Changing Dimensions with history
- **Conformed Dimensions**: Reusable across facts

### 5. Semantic Layer
- **Business Views**: Pre-defined joins for common queries
- **Metrics Catalog**: Calculated measures with business logic
- **Query Abstraction**: Hide complexity from business users

## 🔍 Example Queries

### Multi-Item Transaction Query
```sql
-- Find transactions with multiple bill payments
SELECT 
    th.transaction_number,
    th.total_amount,
    COUNT(ti.item_id) as item_count,
    STRING_AGG(ti.merchant_name, ', ') as merchants
FROM banking.transaction_header th
JOIN banking.transaction_item ti ON th.transaction_id = ti.transaction_id
WHERE th.transaction_type = 'PAYMENT'
GROUP BY th.transaction_number, th.total_amount
HAVING COUNT(ti.item_id) > 1
ORDER BY item_count DESC
LIMIT 10;
```

### Schema Evolution Demo
```sql
-- Before: Customer has no loyalty_tier
SELECT * FROM banking.customer LIMIT 1;

-- Add new column (simulating schema drift)
ALTER TABLE banking.customer ADD COLUMN loyalty_tier VARCHAR(20) DEFAULT 'STANDARD';

-- Raw Vault automatically captures this in new satellite records
-- Dimensional model continues working until explicitly updated
```

## 📊 Data Statistics

After running seeders:

| Entity | Count | Notes |
|--------|-------|-------|
| Customers | 1,000 | 90% individual, 10% business |
| Accounts | ~2,000 | 1-3 accounts per customer |
| Transactions | 5,000 | Last 90 days of activity |
| Transaction Items | ~10,000 | Avg 2 items per transaction |
| Products | 12 | Checking, savings, loans, cards |
| Branches | 10 | Across major US cities |
| Categories | 19 | Hierarchical (8 parent, 11 child) |

## 🛠️ Technologies

- **Scala 2.12**: Main programming language
- **Apache Spark 3.5**: Distributed data processing
- **Apache Iceberg 1.4**: Modern table format with ACID
- **Apache Hive**: Metastore for table management
- **PostgreSQL**: Source system database
- **Apache NiFi**: CDC ingestion pipeline (optional)
- **SBT**: Build tool and dependency management

## 📚 Documentation

1. **[Setup Guide](docs/01_setup_guide.md)** - Step-by-step installation and configuration
2. **[ERD Models](docs/02_erm_models.md)** - Visual representation of all 4 data models
3. **[Architecture](docs/03_architecture.md)** - Detailed architecture and data flow
4. **[Semantic Layer](docs/04_semantic_layer.md)** - Query interface and business metrics

## 🎯 Use Cases Demonstrated

### 1. Customer 360 View
- Combine customer data from multiple sources
- Track customer changes over time
- Analyze customer behavior patterns

### 2. Transaction Analysis
- Multi-level transaction details (header + items)
- Categorize expenses by merchant and category
- Identify recurring payments

### 3. Balance History
- Track account balances over time
- Calculate daily/monthly aggregates
- Detect unusual balance changes

### 4. Product Performance
- Analyze product adoption rates
- Calculate revenue by product type
- Identify cross-sell opportunities

### 5. Schema Evolution
- Add new attributes without breaking existing queries
- Audit historical changes
- Support agile development

## 🔄 Data Flow

```
1. Source System (PostgreSQL)
   ↓ CDC via updated_at timestamps
   ↓ NiFi extracts changes
   
2. Raw Vault (Bronze)
   ↓ Load business keys to Hubs
   ↓ Load relationships to Links
   ↓ Load attributes to Satellites
   ↓ Hash keys for performance
   
3. Business Vault (Silver)
   ↓ Build PIT tables (temporal snapshots)
   ↓ Build Bridges (pre-joined relationships)
   ↓ Apply business rules
   
4. Dimensional Model (Gold)
   ↓ Create dimension tables (SCD Type 2)
   ↓ Create fact tables (transactions, balances)
   ↓ Calculate metrics
   
5. Semantic Layer
   ↓ Define business views
   ↓ Create metric catalog
   ↓ Provide query interface
```

## 🚧 Roadmap

- [ ] Implement NiFi CDC pipelines
- [ ] Add unit tests for ETL jobs
- [ ] Create dashboard examples (Tableau/Power BI)
- [ ] Add data quality checks
- [ ] Implement incremental load logic
- [ ] Add performance benchmarks
- [ ] Create video walkthrough

## 🤝 Contributing

This is a learning project. Feel free to:
- Fork and experiment
- Submit issues for questions
- Propose improvements
- Share your variations

## 📄 License

This project is for educational purposes. Use freely for learning Data Vault 2.0 concepts.

## 🙏 Acknowledgments

- Dan Linstedt - Creator of Data Vault methodology
- Apache Iceberg team - Modern table format
- Apache Spark team - Distributed processing framework

---

**Ready to start?** → [Setup Guide](docs/01_setup_guide.md)

**Questions about the models?** → [ERD Documentation](docs/02_erm_models.md)

**Want to understand the architecture?** → [Architecture Guide](docs/03_architecture.md)

