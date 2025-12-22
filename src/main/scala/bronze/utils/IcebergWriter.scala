package bronze.utils

/**
 * ========================================================================
 * ICEBERG WRITER UTILITY
 * ========================================================================
 *
 * PURPOSE:
 * Encapsulates Iceberg write operations with proper transaction handling,
 * partitioning strategies, and schema evolution support.
 *
 * LEARNING OBJECTIVES:
 * - Iceberg table format architecture (metadata/manifest/data layers)
 * - ACID transaction model in Iceberg
 * - Write modes (append/overwrite/merge)
 * - Partitioning strategies for Data Vault
 * - Schema evolution handling
 * - Snapshot management and time travel
 *
 * ICEBERG FUNDAMENTALS:
 *
 * 1. TABLE FORMAT LAYERS:
 *    ┌──────────────────────────────────────────────────────────┐
 *    │ Metadata Layer (JSON files)                              │
 *    │  - Table schema, partition spec, snapshot history        │
 *    ├──────────────────────────────────────────────────────────┤
 *    │ Manifest Layer (Avro files)                              │
 *    │  - List of data files, column stats, partition info      │
 *    ├──────────────────────────────────────────────────────────┤
 *    │ Data Layer (Parquet/ORC files)                           │
 *    │  - Actual table data, column-oriented                    │
 *    └──────────────────────────────────────────────────────────┘
 *
 * 2. ACID TRANSACTIONS:
 *    - Every write creates a new snapshot (immutable)
 *    - Atomic pointer swap in metadata.json
 *    - Readers always see consistent view
 *    - No lock contention (optimistic concurrency)
 *
 * 3. WRITE MODES:
 *    - APPEND: Add new data files (Data Vault default)
 *    - OVERWRITE: Replace partition data (dimensional loads)
 *    - MERGE: Upsert operations (Iceberg 0.14+)
 *
 * 4. HIVE METASTORE INTEGRATION:
 *    - Iceberg registers table location in HMS
 *    - HMS stores: database, table name, storage path
 *    - Iceberg metadata in {table_location}/metadata/
 *    - Both Spark and Impala read from same HMS
 *
 * ========================================================================
 */

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

object IcebergWriter {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ WRITE TO ICEBERG TABLE (APPEND MODE)                           │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * DATA VAULT USE CASE:
   * - Raw Vault tables are insert-only (immutable)
   * - Append mode adds new data without modifying existing files
   * - Each load creates a new snapshot for time travel
   *
   * @param df DataFrame to write
   * @param database Target database (e.g., "bronze")
   * @param table Target table (e.g., "hub_customer")
   * @param partitionCols Optional partition columns
   *
   * PARTITIONING STRATEGY:
   * - Hubs: Partition by load_date (daily loads)
   * - Satellites: Partition by load_date AND valid_from (temporal queries)
   * - Links: Partition by load_date (relationship tracking)
   *
   * EXAMPLE WRITE PLAN:
   * 1. Spark analyzer validates schema against existing table
   * 2. Planner determines which partitions to write
   * 3. Each executor writes Parquet files to partition paths
   * 4. Driver commits transaction by updating metadata
   * 5. HMS notified of new snapshot (optional)
   */
  def appendToTable(
      df: DataFrame,
      database: String,
      table: String,
      partitionCols: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit = {

    // Build table identifier (namespace.table_name)
    val tableIdentifier = s"$database.$table"

    // Log write operation details
    println(s"""
      |┌────────────────────────────────────────────────────────────────┐
      |│ WRITING TO ICEBERG TABLE (APPEND MODE)                        │
      |├────────────────────────────────────────────────────────────────┤
      |│ Table: $tableIdentifier
      |│ Mode: APPEND
      |│ Partitions: ${if (partitionCols.isEmpty) "None" else partitionCols.mkString(", ")}
      |│ Rows: ${df.count()}
      |└────────────────────────────────────────────────────────────────┘
    """.stripMargin)

    // Write operation with Iceberg format
    val writer = df.write.format("iceberg")
    if (partitionCols.nonEmpty) {
      // Partitioned write: data will be physically organized by these columns
      writer
        .partitionBy(partitionCols: _*)
        .mode("append")
        .save(tableIdentifier)
    } else {
      // Non-partitioned write
      writer
        .mode("append")
        .save(tableIdentifier)
    }

    /**
     * WHAT JUST HAPPENED (Internals):
     *
     * 1. SCHEMA VALIDATION:
     *    - Iceberg checked if df.schema matches table schema
     *    - Schema evolution allowed if compatible (add columns)
     *    - Throws exception if incompatible (drop column, change type)
     *
     * 2. DATA FILES WRITTEN:
     *    - Each partition written as separate Parquet file(s)
     *    - File naming: {partition_path}/data-{uuid}.parquet
     *    - Column statistics collected (min/max/null_count)
     *
     * 3. MANIFEST FILES CREATED:
     *    - List of new data files with metadata
     *    - Stored in: {table_location}/metadata/manifest-{uuid}.avro
     *
     * 4. SNAPSHOT COMMITTED:
     *    - New snapshot entry added to metadata.json
     *    - Snapshot ID, timestamp, manifest list stored
     *    - Previous snapshots retained (time travel enabled)
     *
     * 5. HMS UPDATED (if configured):
     *    - Table location remains unchanged
     *    - HMS still points to same Iceberg metadata directory
     *    - Impala can refresh metadata to see new snapshot
     */

    println(s"✅ Successfully appended ${df.count()} rows to $tableIdentifier")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ CREATE ICEBERG TABLE IF NOT EXISTS                             │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * @param database Database name
   * @param table Table name
   * @param schema Spark StructType defining columns
   * @param partitionCols Columns to partition by
   *
   * HIVE METASTORE REGISTRATION:
   * - Table registered in HMS with location
   * - Location: {warehouse}/database/table/
   * - Metadata: {warehouse}/database/table/metadata/
   */
  def createTableIfNotExists(
      database: String,
      table: String,
      schema: StructType,
      partitionCols: Seq[String] = Seq.empty,
      tableProperties: Map[String, String] = Map.empty
  )(implicit spark: SparkSession): Unit = {

    val tableIdentifier = s"$database.$table"

    // Check if table exists in HMS
    if (!spark.catalog.tableExists(tableIdentifier)) {
      println(s"""
         |┌────────────────────────────────────────────────────────────────┐
         |│ CREATING ICEBERG TABLE                                        │
         |├────────────────────────────────────────────────────────────────┤
         |│ Table: $tableIdentifier
         |│ Partitions: ${if (partitionCols.isEmpty) "None" else partitionCols.mkString(", ")}
         |│ Properties: ${tableProperties.size} custom properties
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

      // Create empty DataFrame with schema
      val emptyDF = spark.createDataFrame(
        spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        schema
      )

      // Create table with Iceberg format
      val writer = emptyDF.write.format("iceberg")
      if (partitionCols.nonEmpty) {
        // Partitioned table creation
        writer.partitionBy(partitionCols: _*)
      }
      writer.options(tableProperties)
        .option("format-version", "2")
        .option("write.parquet.compression-codec", "snappy")
        .mode("overwrite")
        .save(tableIdentifier)

      println(s"✅ Created table: $tableIdentifier")

      // Show table details
      spark.sql(s"DESCRIBE EXTENDED $tableIdentifier").show(false)

    } else {
      println(s"ℹ️  Table already exists: $tableIdentifier")
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ OVERWRITE PARTITION                                             │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Replace data in specific partition(s) while leaving others intact.
   * Useful for reprocessing specific dates in Data Vault.
   *
   * @param df DataFrame to write
   * @param database Target database
   * @param table Target table
   *
   * EXAMPLE:
   * ```scala
   * // Reprocess 2025-01-15 data
   * val reprocessedDF = loadFromSource("2025-01-15")
   * IcebergWriter.overwritePartition(reprocessedDF, "bronze", "sat_customer")
   * // Only 2025-01-15 partition overwritten, other dates unchanged
   * ```
   */
  def overwritePartition(
      df: DataFrame,
      database: String,
      table: String
  )(implicit spark: SparkSession): Unit = {

    val tableIdentifier = s"$database.$table"

    println(s"""
      |┌────────────────────────────────────────────────────────────────┐
      |│ OVERWRITING PARTITION (DYNAMIC MODE)                          │
      |├────────────────────────────────────────────────────────────────┤
      |│ Table: $tableIdentifier
      |│ Mode: DYNAMIC OVERWRITE
      |│ Rows: ${df.count()}
      |└────────────────────────────────────────────────────────────────┘
    """.stripMargin)

    // Dynamic overwrite mode: only overwrites partitions present in df
    df.write.format("iceberg").mode("overwrite").save(tableIdentifier)

    println(s"✅ Successfully overwrote partitions in $tableIdentifier")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ QUERY ICEBERG SNAPSHOT HISTORY                                 │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE: Show all historical snapshots for time travel queries
   *
   * EXAMPLE OUTPUT:
   * +------------+-------------------+----------+
   * | snapshot_id| committed_at      | operation|
   * +------------+-------------------+----------+
   * | 6574839201 | 2025-01-15 10:30  | append   |
   * | 6574839202 | 2025-01-15 11:00  | append   |
   * +------------+-------------------+----------+
   *
   * TIME TRAVEL QUERY:
   * SELECT * FROM bronze.hub_customer
   * VERSION AS OF 6574839201
   */
  def showSnapshots(database: String, table: String)
                   (implicit spark: SparkSession): Unit = {
    val tableIdentifier = s"$database.$table"

    println(s"""
         |┌────────────────────────────────────────────────────────────────┐
         |│ SNAPSHOT HISTORY: $tableIdentifier
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    spark.sql(s"SELECT * FROM $tableIdentifier.snapshots").show(false)
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ EXPIRE OLD SNAPSHOTS                                            │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Remove old snapshots to reclaim storage space.
   * Keeps recent snapshots for time travel queries.
   *
   * @param database Database name
   * @param table Table name
   * @param olderThanTimestamp Expire snapshots older than this timestamp
   * @param retainLast Number of snapshots to always keep
   *
   * EXAMPLE:
   * ```scala
   * // Keep last 10 snapshots, expire older than 7 days
   * IcebergWriter.expireSnapshots(
   *   "bronze",
   *   "hub_customer",
   *   System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000),
   *   retainLast = 10
   * )
   * ```
   *
   * WHEN TO RUN:
   * - Scheduled maintenance (weekly/monthly)
   * - After large reprocessing jobs
   * - Storage optimization
   */
  def expireSnapshots(
      database: String,
      table: String,
      olderThanTimestamp: Long,
      retainLast: Int = 10
  )(implicit spark: SparkSession): Unit = {

    val tableIdentifier = s"$database.$table"

    println(s"""
      |┌────────────────────────────────────────────────────────────────┐
      |│ EXPIRING OLD SNAPSHOTS                                        │
      |├────────────────────────────────────────────────────────────────┤
      |│ Table: $tableIdentifier
      |│ Older than: ${new java.util.Date(olderThanTimestamp)}
      |│ Retain last: $retainLast snapshots
      |└────────────────────────────────────────────────────────────────┘
    """.stripMargin)

    spark.sql(
      s"""
         |CALL spark_catalog.system.expire_snapshots(
         |  table => '$tableIdentifier',
         |  older_than => TIMESTAMP '$olderThanTimestamp',
         |  retain_last => $retainLast
         |)
         |""".stripMargin
    )

    println(s"✅ Expired old snapshots from $tableIdentifier")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ ROLLBACK TO SNAPSHOT                                            │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Revert table to previous snapshot (undo bad load).
   *
   * @param database Database name
   * @param table Table name
   * @param snapshotId Snapshot ID to rollback to
   *
   * EXAMPLE:
   * ```scala
   * // Undo last load
   * val snapshots = spark.sql("SELECT * FROM bronze.hub_customer.snapshots")
   * val previousSnapshotId = snapshots.orderBy(desc("committed_at")).take(2)(1).getLong(0)
   * IcebergWriter.rollbackToSnapshot("bronze", "hub_customer", previousSnapshotId)
   * ```
   */
  def rollbackToSnapshot(
      database: String,
      table: String,
      snapshotId: Long
  )(implicit spark: SparkSession): Unit = {

    val tableIdentifier = s"$database.$table"

    println(s"""
      |┌────────────────────────────────────────────────────────────────┐
      |│ ROLLING BACK TO SNAPSHOT                                      │
      |├────────────────────────────────────────────────────────────────┤
      |│ Table: $tableIdentifier
      |│ Snapshot ID: $snapshotId
      |└────────────────────────────────────────────────────────────────┘
      |
      |⚠️  WARNING: This will undo all changes after snapshot $snapshotId
    """.stripMargin)

    spark.sql(
      s"""
         |CALL spark_catalog.system.rollback_to_snapshot(
         |  table => '$tableIdentifier',
         |  snapshot_id => $snapshotId
         |)
         |""".stripMargin
    )

    println(s"✅ Rolled back $tableIdentifier to snapshot $snapshotId")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ COMPACT DATA FILES                                              │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Combine small data files into larger ones for better query performance.
   *
   * WHY NEEDED:
   * - Many small appends create many small files
   * - Small files hurt query performance (metadata overhead)
   * - Compaction merges files without changing data
   *
   * WHEN TO RUN:
   * - After many incremental appends
   * - Scheduled maintenance
   * - When query performance degrades
   */
  def compactDataFiles(
      database: String,
      table: String,
      targetFileSizeMB: Int = 512
  )(implicit spark: SparkSession): Unit = {

    val tableIdentifier = s"$database.$table"

    println(s"""
      |┌────────────────────────────────────────────────────────────────┐
      |│ COMPACTING DATA FILES                                         │
      |├────────────────────────────────────────────────────────────────┤
      |│ Table: $tableIdentifier
      |│ Target file size: ${targetFileSizeMB}MB
      |└────────────────────────────────────────────────────────────────┘
    """.stripMargin)

    // Read entire table
    val df = spark.table(tableIdentifier)

    // Repartition by partition columns (if partitioned)
    val compactedDF = df.repartition(10) // Adjust based on data size

    // Overwrite entire table with compacted files
    compactedDF.write
      .format("iceberg")
      .mode("overwrite")
      .save(tableIdentifier)

    println(s"✅ Compacted data files in $tableIdentifier")
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ GET TABLE STATISTICS                                            │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Show table size, file counts, and other statistics.
   */
  def showTableStats(database: String, table: String)
                    (implicit spark: SparkSession): Unit = {

    val tableIdentifier = s"$database.$table"

    println(s"""
         |┌────────────────────────────────────────────────────────────────┐
         |│ TABLE STATISTICS: $tableIdentifier
         |└────────────────────────────────────────────────────────────────┘
         |""".stripMargin)

    // Row count
    val rowCount = spark.table(tableIdentifier).count()
    println(s"   Rows: $rowCount")

    // File count
    val fileCount = spark.sql(s"SELECT * FROM $tableIdentifier.files").count()
    println(s"   Data files: $fileCount")

    // Snapshot count
    val snapshotCount = spark.sql(s"SELECT * FROM $tableIdentifier.snapshots").count()
    println(s"   Snapshots: $snapshotCount")

    // Show recent snapshots
    println("\n   Recent snapshots:")
    spark.sql(
      s"""
         |SELECT snapshot_id, committed_at, operation, summary
         |FROM $tableIdentifier.snapshots
         |ORDER BY committed_at DESC
         |LIMIT 5
         |""".stripMargin
    ).show(false)
  }
}
