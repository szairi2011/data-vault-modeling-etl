# How to Run ETL Jobs in IntelliJ IDEA

## ✅ Multiple Execution Modes Supported

The project now supports **three execution modes** with unified configuration management:

1. **IDE (IntelliJ)** - Direct run with environment variables or config file
2. **SBT Command Line** - Using `sbt runMain`
3. **Spark Submit** - For production deployment
4. **Airflow/Orchestration** - Programmatic invocation via `ETLRunner`

---

## 📋 Configuration Precedence

All ETL jobs use a **cascading configuration system** with this priority order:

```
1. JVM System Properties  (-Dkey=value)          [HIGHEST PRIORITY]
2. Environment Variables  (export KEY_NAME=value)
3. application.properties (in src/main/resources)
4. Hardcoded Defaults     (in code)              [LOWEST PRIORITY]
```

### Configuration File Location

Default config: `src/main/resources/application.properties`

```properties
# Warehouse & Catalog
spark.sql.catalog.spark_catalog.warehouse=warehouse
# spark.sql.catalog.spark_catalog.uri=thrift://localhost:9083

# Networking
spark.driver.host=127.0.0.1
spark.driver.bindAddress=0.0.0.0

# Data Paths
staging.path=warehouse/staging
bronze.path=warehouse/bronze
silver.path=warehouse/silver
gold.path=warehouse/gold
```

---

## 🚀 Execution Mode 1: IntelliJ IDE

### Option A: Run with Environment Variables

1. **Open Run Configuration** (Run → Edit Configurations)

2. **Create/Edit Application Configuration**:
   - **Main class**: `bronze.RawVaultETL`
   - **Program arguments**: `--mode full --entity customer`
   - **Environment variables**:
     ```
     SPARK_WAREHOUSE=warehouse
     SPARK_DRIVER_HOST=127.0.0.1
     HADOOP_HOME=C:/hadoop
     ```
   - **Working directory**: `C:\Users\sofiane\work\learn-intellij\data-vault-modeling-etl`

3. **Run** the configuration

### Option B: Run with JVM Properties

1. **VM options** (in Run Configuration):
   ```
   -Dspark.sql.catalog.spark_catalog.warehouse=warehouse
   -Dspark.driver.host=127.0.0.1
   -Dhadoop.home.dir=C:/hadoop
   ```

2. **Program arguments**:
   ```
   --mode full --entity customer
   ```

### Option C: Run with application.properties

1. Ensure `src/main/resources/application.properties` exists with your settings

2. **Program arguments**:
   ```
   --mode incremental
   ```

3. Run directly - config will be loaded automatically

---

## 🚀 Execution Mode 2: SBT Command Line

### Basic Run

```powershell
# In IntelliJ Terminal (Alt+F12) or external PowerShell
cd C:\Users\sofiane\work\learn-intellij\data-vault-modeling-etl

# Bronze Layer - Full load
sbt "runMain bronze.RawVaultETL --mode full"

# Bronze Layer - Incremental customer only
sbt "runMain bronze.RawVaultETL --mode incremental --entity customer"

# Silver Layer - Build PIT tables
sbt "runMain silver.BusinessVaultETL --build-pit --date 2024-01-15"

# Gold Layer - Rebuild all
sbt "runMain gold.DimensionalModelETL --rebuild-all"
```

### With JVM Properties Override

```powershell
# Override warehouse location
sbt -Dspark.sql.catalog.spark_catalog.warehouse=custom_warehouse "runMain bronze.RawVaultETL --mode full"

# Use Hive Metastore
sbt -Dspark.sql.catalog.spark_catalog.uri=thrift://localhost:9083 "runMain bronze.RawVaultETL --mode full"
```

### With Environment Variables

```powershell
# PowerShell
$env:SPARK_WAREHOUSE = "custom_warehouse"
$env:HIVE_METASTORE_URI = "thrift://localhost:9083"
sbt "runMain bronze.RawVaultETL --mode full"
```

---

## 🚀 Execution Mode 3: Spark Submit (Production)

### Step 1: Package the Application

```powershell
# Create fat JAR with all dependencies
sbt assembly
```

This creates: `target/scala-2.12/data-vault-modeling-etl-assembly-1.0.0.jar`

### Step 2: Submit to Spark

```powershell
# Local mode
spark-submit `
  --class bronze.RawVaultETL `
  --master local[*] `
  --conf spark.sql.catalog.spark_catalog.warehouse=warehouse `
  --conf spark.driver.host=127.0.0.1 `
  target/scala-2.12/data-vault-modeling-etl-assembly-1.0.0.jar `
  --mode full

# Cluster mode (YARN/Kubernetes)
spark-submit `
  --class bronze.RawVaultETL `
  --master yarn `
  --deploy-mode cluster `
  --conf spark.sql.catalog.spark_catalog.uri=thrift://hms-server:9083 `
  --conf spark.sql.catalog.spark_catalog.warehouse=hdfs://namenode/warehouse `
  target/scala-2.12/data-vault-modeling-etl-assembly-1.0.0.jar `
  --mode incremental --entity customer
```

---

## 🚀 Execution Mode 4: Airflow/Programmatic

### Using ETLRunner in Airflow DAG

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

bronze_etl = SparkSubmitOperator(
    task_id='bronze_raw_vault_etl',
    application='/path/to/data-vault-modeling-etl-assembly-1.0.0.jar',
    java_class='common.ETLRunner',
    application_args=[
        'bronze.RawVaultETL',
        '--mode', 'incremental',
        '--entity', 'customer'
    ],
    conf={
        'spark.sql.catalog.spark_catalog.warehouse': 'warehouse',
        'spark.sql.catalog.spark_catalog.uri': 'thrift://hms:9083'
    },
    dag=dag
)
```

### Programmatic Invocation (Scala)

```scala
import common.{ETLRunner, ETLConfig}
import bronze.RawVaultETL

// Option 1: Using config map
ETLRunner.runJob(
  RawVaultETL,
  Map(
    "mode" -> "incremental",
    "entity" -> "customer"
  )
)

// Option 2: Using args
ETLRunner.runJobWithArgs(
  RawVaultETL,
  Array("--mode", "full", "--entity", "customer")
)
```

---

## 🎯 Quick Reference: All ETL Jobs

### Bronze Layer (Raw Vault)

```powershell
# All entities, full load
sbt "runMain bronze.RawVaultETL --mode full"

# Single entity, incremental
sbt "runMain bronze.RawVaultETL --mode incremental --entity customer"
```

**Available entities**: `customer`, `account`, `transaction`

### Silver Layer (Business Vault)

```powershell
# Build PIT tables for today
sbt "runMain silver.BusinessVaultETL --build-pit"

# Build PIT for specific date
sbt "runMain silver.BusinessVaultETL --build-pit --date 2024-01-15"

# Build Bridge tables
sbt "runMain silver.BusinessVaultETL --build-bridge"

# Build all
sbt "runMain silver.BusinessVaultETL --all"
```

### Gold Layer (Dimensional Model)

```powershell
# Rebuild dimensions only
sbt "runMain gold.DimensionalModelETL --rebuild-dims"

# Rebuild facts only
sbt "runMain gold.DimensionalModelETL --rebuild-facts"

# Rebuild all
sbt "runMain gold.DimensionalModelETL --rebuild-all"
```

---

## 🔧 Troubleshooting

### Issue: NoClassDefFoundError for Spark classes

**Solution 1**: Reload SBT Project in IntelliJ
1. Right-click on `build.sbt` → "Reload SBT Project"
2. Wait for sync to complete
3. Verify Spark libraries appear in "External Libraries"

**Solution 2**: Use SBT command line instead
```powershell
sbt "runMain bronze.RawVaultETL --mode full"
```

### Issue: Warehouse directory not found

**Solution**: Set warehouse location explicitly
```powershell
# Via environment variable
$env:SPARK_WAREHOUSE = "C:/full/path/to/warehouse"

# Via JVM property
sbt -Dspark.sql.catalog.spark_catalog.warehouse=C:/full/path/to/warehouse "runMain bronze.RawVaultETL --mode full"
```

### Issue: Hive Metastore connection error

**Solution**: Use Hadoop Catalog instead (default)
- Comment out `spark.sql.catalog.spark_catalog.uri` in `application.properties`
- Or set environment: `Remove-Item Env:\HIVE_METASTORE_URI`

---

## 📖 Configuration Examples

### Development (Local Hadoop Catalog)

```properties
# application.properties
spark.sql.catalog.spark_catalog.warehouse=warehouse
spark.driver.host=127.0.0.1
```

### Staging (External Hive Metastore)

```properties
# application.properties
spark.sql.catalog.spark_catalog.warehouse=hdfs://namenode/warehouse
spark.sql.catalog.spark_catalog.uri=thrift://hms-staging:9083
```

### Production (Override via JVM props)

```powershell
spark-submit `
  --conf spark.sql.catalog.spark_catalog.warehouse=hdfs://prod-namenode/warehouse `
  --conf spark.sql.catalog.spark_catalog.uri=thrift://prod-hms:9083 `
  ...
```

---

## ✅ Expected Output

Once properly configured, you'll see:

```
╔════════════════════════════════════════════════════════════════╗
║         DATA VAULT 2.0 - RAW VAULT ETL (BRONZE LAYER)         ║
╚════════════════════════════════════════════════════════════════╝

Configuration:
  Mode: full
  Entity: all
  Snapshot Date: 2024-01-15
  Record Source: PostgreSQL

📖 Loaded configuration from classpath: application.properties

🚀 Initializing Spark Session: DATA VAULT 2.0 - RAW VAULT ETL (BRONZE LAYER)
🔧 Using Iceberg HadoopCatalog (no Hive Metastore)
✅ Spark 3.5.0 initialized (catalog=spark_catalog, warehouse=warehouse)

🔍 Current Configuration:
   spark.driver.host = 127.0.0.1
   spark.sql.catalog.spark_catalog.warehouse = warehouse
   spark.sql.catalog.spark_catalog.uri = (not set)

...
✅ DATA VAULT 2.0 - RAW VAULT ETL (BRONZE LAYER) completed successfully
```



