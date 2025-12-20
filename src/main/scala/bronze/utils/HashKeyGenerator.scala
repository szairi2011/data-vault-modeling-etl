package bronze.utils

/**
 * ========================================================================
 * HASH KEY GENERATOR UTILITY
 * ========================================================================
 *
 * PURPOSE:
 * Generate MD5 hash keys for Data Vault 2.0 hubs, links, and satellites.
 *
 * LEARNING OBJECTIVES:
 * - Why Data Vault uses hash keys instead of surrogate keys
 * - MD5 hashing for deterministic key generation
 * - Handling composite keys (multiple columns)
 * - Null value handling in hash computation
 * - Performance considerations
 *
 * DATA VAULT HASH KEY BENEFITS:
 * 1. **Deterministic**: Same business key always produces same hash
 * 2. **Fixed Width**: Always 32 characters (MD5) - predictable storage
 * 3. **Fast Joins**: Hash equality faster than multi-column joins
 * 4. **Multi-Source Integration**: Hash same business key from different sources
 * 5. **No Central Key Generation**: No need for sequence generators
 *
 * HASH KEY TYPES:
 * ```
 * Hub Hash Key:
 *   MD5(customer_id)
 *   Example: "a1b2c3d4e5f6..." (32 chars)
 *
 * Link Hash Key:
 *   MD5(customer_hash_key || account_hash_key)
 *   Composite of parent hubs
 *
 * Diff Hash:
 *   MD5(all_satellite_columns)
 *   Detects if record changed
 * ```
 *
 * WHY MD5 (Not SHA-256)?
 * - Faster computation (legacy DV standard)
 * - Smaller output (32 vs 64 hex chars)
 * - No security requirement (not cryptographic use)
 * - Industry standard in Data Vault 2.0
 *
 * COLLISION RISK:
 * - MD5 collisions theoretically possible
 * - Practically negligible for data warehouse (not security use)
 * - Billions of records before collision likely
 * - If concerned, use SHA-256 (change hash function only)
 *
 * ========================================================================
 */

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import java.security.MessageDigest

object HashKeyGenerator {

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ GENERATE HASH KEY FROM BUSINESS KEY COLUMNS                     │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Create hash key column by hashing one or more business key columns.
   *
   * @param hashKeyName   Name of the new hash key column (e.g., "customer_hash_key")
   * @param businessKeys  Business key column names to hash (e.g., ["customer_id"])
   * @param df            Source DataFrame
   * @return              DataFrame with new hash key column
   *
   * EXAMPLES:
   * ```scala
   * // Single business key (Hub)
   * val df = HashKeyGenerator.generateHashKey(
   *   "customer_hash_key",
   *   Seq("customer_id"),
   *   customerDF
   * )
   *
   * // Composite business key (Link)
   * val df = HashKeyGenerator.generateHashKey(
   *   "link_customer_account_hash_key",
   *   Seq("customer_hash_key", "account_hash_key"),
   *   linkDF
   * )
   *
   * // Multiple business keys (compound natural key)
   * val df = HashKeyGenerator.generateHashKey(
   *   "transaction_item_hash_key",
   *   Seq("transaction_id", "item_sequence"),
   *   itemDF
   * )
   * ```
   *
   * HASH COMPUTATION ALGORITHM:
   * 1. Concatenate business key columns with delimiter
   * 2. Convert to uppercase (case insensitive)
   * 3. Trim whitespace
   * 4. Replace NULL with "~NULL~" (consistent null representation)
   * 5. Compute MD5 hash
   * 6. Convert to hexadecimal string (32 characters)
   *
   * WHY CONCATENATION WITH DELIMITER:
   * - Prevents ambiguity: "AB" + "CD" vs "ABC" + "D" are different
   * - Delimiter "||" ensures unique concatenation
   * - Standard Data Vault 2.0 practice
   */
  def generateHashKey(hashKeyName: String,
                      businessKeys: Seq[String],
                      df: DataFrame): DataFrame = {

    // Validate business keys exist in DataFrame
    val missingKeys = businessKeys.filterNot(df.columns.contains)
    if (missingKeys.nonEmpty) {
      throw new IllegalArgumentException(
        s"""
           |Cannot generate hash key '$hashKeyName'
           |Missing columns: ${missingKeys.mkString(", ")}
           |Available columns: ${df.columns.mkString(", ")}
           |""".stripMargin
      )
    }

    println(s"""
         |🔐 Generating hash key: $hashKeyName
         |   Business keys: ${businessKeys.mkString(", ")}
         |   Algorithm: MD5
         |   Output: 32-character hexadecimal string
         |""".stripMargin)

    // Build concatenated string column
    // Format: UPPER(TRIM(col1)) || '~' || UPPER(TRIM(col2)) || ...
    val concatenated = businessKeys.map { colName =>
      // Handle NULL values explicitly
      // COALESCE converts NULL to '~NULL~' for consistent hashing
      coalesce(
        upper(trim(col(colName).cast("string"))),
        lit("~NULL~")
      )
    }.reduce((c1, c2) => concat_ws("~", c1, c2))

    // Generate MD5 hash
    val hashKeyColumn = md5(concatenated).as(hashKeyName)

    // Add hash key column to DataFrame
    df.withColumn(hashKeyName, hashKeyColumn)
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ GENERATE DIFF HASH FOR CHANGE DETECTION                         │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Create hash of all descriptive columns to detect if record changed.
   * Used in satellites to avoid loading duplicate versions.
   *
   * @param diffHashName  Name of the diff hash column (e.g., "customer_diff_hash")
   * @param columns       Columns to include in hash (all satellite attributes)
   * @param df            Source DataFrame
   * @return              DataFrame with diff hash column
   *
   * USAGE IN DATA VAULT:
   * ```scala
   * // In satellite loading:
   * // 1. Generate diff hash for incoming records
   * val newRecords = HashKeyGenerator.generateDiffHash(
   *   "customer_diff_hash",
   *   Seq("first_name", "last_name", "email", "customer_status"),
   *   stagingDF
   * )
   *
   * // 2. Get current records from satellite
   * val currentRecords = spark.table("bronze.sat_customer")
   *   .filter($"valid_to".isNull)  // Current records only
   *
   * // 3. Left anti join to find changed records
   * val changedRecords = newRecords
   *   .join(currentRecords,
   *         Seq("customer_hash_key", "customer_diff_hash"),
   *         "left_anti")
   *
   * // 4. Load only changed records (avoids duplicate versions)
   * changedRecords.writeTo("bronze.sat_customer").append()
   * ```
   *
   * BENEFITS:
   * - Single column comparison instead of comparing all columns
   * - Fast change detection
   * - Standard Data Vault 2.0 optimization
   *
   * WHEN TO SKIP DIFF HASH:
   * - Very few columns (< 3) - compare directly
   * - Very wide tables (> 100 cols) - hash computation overhead
   * - Append-only satellites (no deduplication needed)
   */
  def generateDiffHash(diffHashName: String,
                       columns: Seq[String],
                       df: DataFrame): DataFrame = {

    println(s"""
         |🔍 Generating diff hash: $diffHashName
         |   Columns: ${columns.mkString(", ")}
         |   Purpose: Change detection in satellites
         |""".stripMargin)

    // Concatenate all descriptive columns
    val concatenated = columns.map { colName =>
      coalesce(
        upper(trim(col(colName).cast("string"))),
        lit("~NULL~")
      )
    }.reduce((c1, c2) => concat_ws("~", c1, c2))

    // Generate MD5 hash
    val diffHashColumn = md5(concatenated).as(diffHashName)

    df.withColumn(diffHashName, diffHashColumn)
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ GENERATE MULTIPLE HASH KEYS IN BATCH                            │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Efficiently generate multiple hash keys in single pass.
   * Useful for link tables that reference multiple hubs.
   *
   * @param hashKeySpecs  Map of hash key name → business key columns
   * @param df            Source DataFrame
   * @return              DataFrame with all hash keys added
   *
   * EXAMPLE:
   * ```scala
   * // Generate multiple hash keys for link table
   * val hashSpecs = Map(
   *   "customer_hash_key" → Seq("customer_id"),
   *   "account_hash_key" → Seq("account_id"),
   *   "link_customer_account_hash_key" → Seq("customer_id", "account_id")
   * )
   *
   * val linkDF = HashKeyGenerator.generateHashKeysBatch(hashSpecs, sourceDF)
   * ```
   *
   * PERFORMANCE:
   * - Single DataFrame transformation instead of multiple
   * - Avoids repeated shuffles
   * - Better for wide transformations (many hash keys)
   */
  def generateHashKeysBatch(hashKeySpecs: Map[String, Seq[String]],
                            df: DataFrame): DataFrame = {

    println(s"""
         |🔐 Generating ${hashKeySpecs.size} hash keys in batch
         |""".stripMargin)

    // Generate all hash keys in single withColumns operation
    val hashKeyColumns = hashKeySpecs.map { case (hashKeyName, businessKeys) =>
      val concatenated = businessKeys.map { colName =>
        coalesce(
          upper(trim(col(colName).cast("string"))),
          lit("~NULL~")
        )
      }.reduce((c1, c2) => concat_ws("~", c1, c2))

      hashKeyName → md5(concatenated)
    }

    // Add all hash key columns at once
    hashKeyColumns.foldLeft(df) { case (accDF, (name, column)) =>
      accDF.withColumn(name, column)
    }
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ VALIDATE HASH KEY UNIQUENESS                                    │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * PURPOSE:
   * Check if hash key is unique (no collisions).
   * Should always be unique for hub hash keys.
   *
   * @param hashKeyName  Hash key column to validate
   * @param df           DataFrame to check
   * @return             True if unique, throws exception if duplicates
   *
   * WHEN TO USE:
   * - After generating hub hash keys (should be unique)
   * - Quality checks in test environment
   * - Detecting data quality issues
   *
   * DO NOT USE IN PRODUCTION:
   * - Expensive operation (full DataFrame scan)
   * - Use sampling instead for large datasets
   */
  def validateHashKeyUniqueness(hashKeyName: String, df: DataFrame): Boolean = {
    val totalCount = df.count()
    val uniqueCount = df.select(hashKeyName).distinct().count()

    if (totalCount != uniqueCount) {
      val duplicates = totalCount - uniqueCount
      throw new IllegalStateException(
        s"""
           |❌ HASH KEY COLLISION DETECTED
           |
           |Hash Key: $hashKeyName
           |Total Records: $totalCount
           |Unique Hash Keys: $uniqueCount
           |Duplicates: $duplicates
           |
           |POSSIBLE CAUSES:
           |1. Multiple records with same business key (data quality issue)
           |2. Rare MD5 collision (extremely unlikely)
           |3. Bug in hash key generation logic
           |
           |INVESTIGATION:
           |Run this query to find duplicates:
           |SELECT $hashKeyName, COUNT(*)
           |FROM table
           |GROUP BY $hashKeyName
           |HAVING COUNT(*) > 1
           |""".stripMargin
      )
    }

    println(s"✅ Hash key '$hashKeyName' is unique ($uniqueCount records)")
    true
  }

  /**
   * ┌─────────────────────────────────────────────────────────────────┐
   * │ PERFORMANCE STATISTICS                                          │
   * └─────────────────────────────────────────────────────────────────┘
   *
   * Show hash key generation statistics for monitoring.
   */
  def showHashKeyStats(df: DataFrame, hashKeyName: String): Unit = {
    println(s"""
         |📊 Hash Key Statistics: $hashKeyName
         |   Total Records: ${df.count()}
         |   Unique Hashes: ${df.select(hashKeyName).distinct().count()}
         |   Null Hash Keys: ${df.filter(col(hashKeyName).isNull).count()}
         |   Sample Hash Keys:
         |""".stripMargin)

    df.select(hashKeyName).show(5, truncate = false)
  }
}

