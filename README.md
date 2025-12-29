# Banking Data Vault 2.0 POC

A comprehensive end-to-end implementation of Data Vault 2.0 methodology for a fictitious banking system, demonstrating modern data warehousing best practices with schema evolution resilience.

---

## 📖 Navigation Hub

**Start with the right guide for your needs:**

- 🏗️ **[Architecture & Design Guide](docs/architecture.md)** - Understand the data models and design decisions
- 🚀 **[Setup & Execution Guide](docs/setup_guide.md)** - Run the complete pipeline step-by-step

**Performance benchmarking framework (planned):**

- ⚡ **[Query Engine Benchmarking Plan](docs/setup_guide.md#stage-7-query-engine-benchmarking)** - Compare Spark SQL, Hive Tez, and Impala

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

- ✅ **Data Vault 2.0 modeling** - Hubs, Links, Satellites pattern
  - [See Architecture Details](docs/architecture.md#data-vault-model-bronze)
- ✅ **Multi-item transactions** - E-commerce style transaction modeling
  - [Design Details](docs/architecture.md#source-system-erm)
- ✅ **Schema evolution handling** - Add columns without breaking queries
  - [How It Works](docs/architecture.md#why-data-vault-20)
- ✅ **Incremental CDC via NiFi** - Extract only changed data
  - [Implementation Guide](docs/architecture.md#why-apache-nifi)
- ✅ **SCD Type 2 history tracking** - Full temporal history
  - [Gold Layer Design](docs/architecture.md#dimensional-model-gold)
- ✅ **Full audit trail** - Time travel with Apache Iceberg
  - [Technology Choice](docs/architecture.md#why-apache-iceberg)

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

**→ Full details:** [Prerequisites & Tools](docs/setup_guide.md#prerequisites--tools)

---

## 🚀 Quick Start

```powershell
# 1. Create database and seed data
psql -U postgres -c "CREATE DATABASE banking_source;"
psql -U postgres -d banking_source -f source-system\sql\02_create_tables.sql
sbt "runMain seeder.TransactionalDataSeeder"

# 2. Extract with NiFi (import template from nifi-flows/, start flow in UI)

# 3. Load Data Vault Bronze Layer (Raw Vault)
sbt "runMain bronze.RawVaultETL"                    # All entities, incremental mode
sbt "runMain bronze.RawVaultETL --mode full"        # Full refresh all entities
sbt "runMain bronze.RawVaultETL --entity customer"  # Single entity only

# 4. Build analytics layers
sbt "runMain silver.BusinessVaultETL --build-pit"
sbt "runMain gold.DimensionalModelETL --load-dimensions"
```

**What the Bronze ETL does:**
- ✅ Creates all Data Vault 2.0 tables (Hubs, Links, Satellites) using Iceberg
- ✅ Reads Avro files from `warehouse/staging/` with schema validation
- ✅ Generates MD5 hash keys for deterministic joins
- ✅ Loads business keys into Hubs (deduplicated)
- ✅ Loads relationships into Links
- ✅ Loads descriptive attributes into Satellites with SCD Type 2 history
- ✅ Tracks all ETL operations in `bronze.load_metadata` for audit trail

**→ Complete walkthrough:** [Pipeline Execution Guide](docs/setup_guide.md#part-ii-pipeline-execution)

**→ If issues arise:** [Troubleshooting](docs/setup_guide.md#troubleshooting)

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

This project teaches essential data engineering concepts with hands-on implementation:

| Concept | What You'll Learn | Where to Learn More |
|---------|-------------------|---------------------|
| **Data Vault 2.0** | Hubs, Links, Satellites pattern for enterprise DW | [Data Vault Model](docs/architecture.md#data-vault-model-bronze) |
| **NiFi Ingestion** | CDC extraction with schema validation | [Why NiFi](docs/architecture.md#why-apache-nifi) + [Stage 2](docs/setup_guide.md#stage-2-nifi-data-extraction--avro-staging) |
| **Avro Schemas** | Type-safe data exchange and evolution | [Why Avro](docs/architecture.md#why-apache-avro) |
| **Spark ETL** | Scalable data processing with Iceberg tables | [Stage 3-5](docs/setup_guide.md#part-ii-pipeline-execution) |
| **Multi-Layer Architecture** | Bronze → Silver → Gold progression | [Layered Architecture](docs/architecture.md#why-multi-layer-architecture) |
| **Schema Evolution** | Handle source changes without breaking queries | [Schema Evolution Scenario](docs/setup_guide.md#stage-6-schema-evolution-scenario) |
| **Query Engine Optimization** | Compare Spark SQL, Hive Tez, and Impala performance | [Benchmarking Plan](docs/setup_guide.md#stage-7-query-engine-benchmarking) |

---

## 💡 Key Concepts Demonstrated

### Data Vault Pattern
```
Hub_Customer (unique customers) + Sat_Customer (attributes with history)
Link_Customer_Account (relationships) + metadata (load tracking)
= Resilient to source changes, full audit trail
```
**→ Deep dive:** [Why Data Vault 2.0](docs/architecture.md#why-data-vault-20)

### Schema Evolution
```
Source adds loyalty_tier column → NiFi validates → Spark absorbs
Old queries: still work | New queries: can use loyalty_tier
Historical records: loyalty_tier = NULL
```
**→ Try it yourself:** [Schema Evolution Demo](docs/setup_guide.md#stage-6-schema-evolution-scenario)

### Multi-Item Transactions
```
Transaction TXN-001 ($250)
  ├─ Item 1: Electricity Bill ($100)
  ├─ Item 2: Water Bill ($50)
  └─ Item 3: Internet Bill ($100)
```
**→ Data model:** [Source System ERM](docs/architecture.md#source-system-erm)

---

## ⚡ Query Engine Performance Comparison (Planned)

This project includes a **comprehensive benchmarking framework** comparing three query engines on the same semantic layer as part of a CI/CD-integrated performance testing strategy:

| Engine | Execution Model | Best For | Benchmark Focus |
|--------|----------------|----------|-----------------|
| **Spark SQL** | In-memory distributed processing | Complex transformations, ML pipelines, batch ETL | Baseline performance & versatility |
| **Hive on Tez** | DAG-based execution engine | Batch processing, large-scale ETL workloads | Optimized batch query performance |
| **Impala** | MPP (massively parallel processing) | Interactive analytics, low-latency queries | Ad-hoc query speed & concurrency |

### Benchmark Suite

Five standardized queries are ready for execution across complexity levels:

1. **Simple Aggregations** - Basic COUNT, SUM, AVG on single tables
2. **Complex Joins** - Customer 360 view with multi-table joins
3. **Temporal Queries** - PIT table queries with time-based filters
4. **Multi-Item Analysis** - Transaction header + items aggregations
5. **Schema Evolution** - Queries spanning old and new schema versions

### Performance Metrics (Planned)

The benchmarking framework will measure:
- **Execution Time** - Query completion duration (cold and warm runs)
- **Resource Utilization** - CPU, memory, I/O consumption
- **Concurrency** - Multi-user query handling
- **Scalability** - Performance across different data volumes

### CI/CD Integration Vision

The benchmarking framework is designed to integrate into automated release pipelines for:
- **Performance regression testing** - Detect query slowdowns across releases
- **Vulnerability checks** - Identify security issues in query execution
- **Release reporting** - Include performance metrics in release notes
- **Baseline tracking** - Monitor query performance trends over time

### Documentation & Queries Ready

**→ Implementation plan:** [Stage 7: Benchmarking Guide](docs/setup_guide.md#stage-7-query-engine-benchmarking)  
**→ Architecture analysis:** [Query Engine Comparison](docs/architecture.md#query-engine-comparison)  
**→ Benchmark queries:** Available in `sample_queries/benchmarks/` (5 SQL files ready to run)

---

## 🔍 Use Cases You Can Explore

Once the pipeline is running, try these analytical scenarios:

| Use Case | What It Demonstrates | How to Run |
|----------|---------------------|------------|
| **Customer 360 View** | Combining customer data across time | [Stage 5: Gold Layer](docs/setup_guide.md#stage-5-gold-layer-dimensional-model) |
| **Transaction Analysis** | Multi-level transaction details (header + items) | Query examples in [Stage 5](docs/setup_guide.md#stage-5-gold-layer-dimensional-model) |
| **Balance History** | Tracking account balances over time with SCD Type 2 | [Business Vault queries](docs/setup_guide.md#stage-4-silver-layer-business-vault) |
| **Schema Evolution** | Add new attributes without breaking existing queries | [Schema Evolution Scenario](docs/setup_guide.md#stage-6-schema-evolution-scenario) |
| **Query Engine Benchmarking** | Compare execution times across Spark SQL, Hive Tez, and Impala (planned) | [Benchmarking Plan](docs/setup_guide.md#stage-7-query-engine-benchmarking) |

---

## 🛠️ Technologies

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Source** | PostgreSQL 12+ | Operational database (3NF normalized) |
| **Extraction** | Apache NiFi 2.7.2 | CDC ingestion with schema validation |
| **Format** | Apache Avro | Type-safe serialization with schema evolution |
| **Processing** | Apache Spark 3.5 + Scala 2.12 | Distributed ETL processing |
| **Storage** | Apache Iceberg 1.4 | Modern table format with ACID, time travel |
| **Catalog** | Hive Metastore | Table metadata management |
| **Query Engines** | Spark SQL / Hive on Tez / Impala | Performance benchmarking & semantic layer queries |

**→ Technology rationale:** [Design Decisions](docs/architecture.md#part-iii-design-decisions)

---

## 🤝 Use This Project To

- **Learn** data engineering concepts with hands-on practice
- **Refresh** your knowledge before interviews or projects
- **Understand** Data Vault 2.0 with a real working example
- **Reference** when building similar data pipelines
- **Experiment** with schema evolution and multi-layer architectures

---

## 📄 License

MIT License - Free for learning and reference.

---

## 🚦 Ready to Start?

**Choose your path:**

1. **Want to understand the design first?** → [Architecture Guide](docs/architecture.md)
2. **Ready to build and run?** → [Setup Guide](docs/setup_guide.md)
3. **Just need quick commands?** → [Quick Commands](docs/setup_guide.md#quick-commands)
