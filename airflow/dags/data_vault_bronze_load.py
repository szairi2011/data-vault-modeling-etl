"""
================================================================================
BRONZE LAYER ETL DAG - DATA VAULT RAW VAULT LOAD
================================================================================

PURPOSE:
Orchestrate the loading of staging Avro files into the Data Vault 2.0
Bronze Layer (Raw Vault: Hubs, Links, Satellites) using Spark ETL.

PIPELINE FLOW:
    Avro Files (warehouse/staging/*)
        ↓
    Spark ETL (RawVaultETL)
        ↓
    Iceberg Tables (warehouse/bronze/raw-vault/*)

TABLES LOADED:
- hub_customer, sat_customer
- hub_account, sat_account, link_customer_account
- hub_transaction, sat_transaction
- hub_transaction_item, sat_transaction_item, link_transaction_item

EXECUTION MODES:
- Manual trigger: For development and testing
- Scheduled: Daily at 2 AM (production)
- Full load: --mode full
- Incremental: --mode incremental (default)

DEPENDENCIES:
- Avro files must exist in warehouse/staging/{entity}/
- Spark JAR must be compiled: sbt assembly
- Iceberg tables created: RawVaultSchema.createAllTables()

================================================================================
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

# ============================================================================
# DAG CONFIGURATION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def validate_staging_data(**context):
    """
    Validate that staging Avro files exist before running ETL.
    Prevents job failure if NiFi hasn't extracted data yet.
    """
    warehouse_path = Variable.get("WAREHOUSE_PATH", default_var="/opt/warehouse")
    staging_path = f"{warehouse_path}/staging"

    entities = ['customer', 'account', 'transaction', 'transaction_item']
    missing_entities = []

    for entity in entities:
        entity_path = f"{staging_path}/{entity}"
        if not os.path.exists(entity_path) or not os.listdir(entity_path):
            missing_entities.append(entity)

    if missing_entities:
        raise FileNotFoundError(
            f"Missing staging data for: {', '.join(missing_entities)}. "
            f"Run NiFi extraction first."
        )

    print(f"✅ Staging data validated for all entities: {', '.join(entities)}")


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id='data_vault_bronze_load',
    default_args=default_args,
    description='Load Raw Vault (Bronze Layer) from Staged Avro files',
    schedule_interval=None,  # Manual trigger for development
    # schedule_interval='0 2 * * *',  # Uncomment for production: Daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['bronze', 'data-vault', 'etl', 'spark'],
    max_active_runs=1,  # Prevent concurrent runs
) as dag:

    # ========================================================================
    # TASK 1: VALIDATE STAGING DATA
    # ========================================================================

    validate_staging = PythonOperator(
        task_id='validate_staging_data',
        python_callable=validate_staging_data,
        provide_context=True,
    )

    # ========================================================================
    # TASK 2: CREATE ICEBERG TABLES (IF NOT EXIST)
    # ========================================================================

    create_tables = BashOperator(
        task_id='create_iceberg_tables',
        bash_command="""
        spark-submit \
          --class bronze.RawVaultSchema \
          --master local[*] \
          --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
          --conf spark.sql.catalog.spark_catalog.type=hadoop \
          --conf spark.sql.catalog.spark_catalog.warehouse={{ var.value.WAREHOUSE_PATH }}/bronze \
          --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
          {{ var.value.BRONZE_JAR_PATH }}
        """,
    )

    # ========================================================================
    # TASK 3: RUN BRONZE ETL (FULL LOAD)
    # ========================================================================

    run_bronze_etl = BashOperator(
        task_id='run_bronze_etl',
        bash_command="""
        spark-submit \
          --class bronze.RawVaultETL \
          --master local[*] \
          --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
          --conf spark.sql.catalog.spark_catalog.type=hadoop \
          --conf spark.sql.catalog.spark_catalog.warehouse={{ var.value.WAREHOUSE_PATH }}/bronze \
          --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
          --conf spark.driver.memory=2g \
          --conf spark.executor.memory=2g \
          {{ var.value.BRONZE_JAR_PATH }} \
          --mode full \
          --staging-path {{ var.value.WAREHOUSE_PATH }}/staging \
          --output-path {{ var.value.WAREHOUSE_PATH }}/bronze/raw-vault
        """,
    )

    # ========================================================================
    # TASK 4: VALIDATE BRONZE LAYER DATA QUALITY
    # ========================================================================

    validate_bronze = BashOperator(
        task_id='validate_bronze_quality',
        bash_command="""
        spark-submit \
          --class bronze.BronzeValidator \
          --master local[*] \
          --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
          --conf spark.sql.catalog.spark_catalog.type=hadoop \
          --conf spark.sql.catalog.spark_catalog.warehouse={{ var.value.WAREHOUSE_PATH }}/bronze \
          --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
          {{ var.value.BRONZE_JAR_PATH }}
        """,
    )

    # ========================================================================
    # TASK DEPENDENCIES
    # ========================================================================

    validate_staging >> create_tables >> run_bronze_etl >> validate_bronze

