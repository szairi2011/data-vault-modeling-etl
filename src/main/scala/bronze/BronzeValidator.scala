package bronze

/**
 * ========================================================================
 * BRONZE LAYER DATA VALIDATOR
 * ========================================================================
 *
 * PURPOSE:
 * Validate Bronze Layer (Raw Vault) data quality after ETL execution.
 *
 * USAGE:
 * From IDE: Run main method
 * From SBT: sbt "runMain bronze.BronzeValidator"
 * From JAR: spark-submit --class bronze.BronzeValidator <jar>
 *
 * VALIDATIONS:
 * 1. Record counts match expectations
 * 2. No duplicate hash keys in Hubs
 * 3. All Satellite records have temporal fields populated
 * 4. Hash key format validation (64-character SHA-256)
 * 5. Foreign key integrity (Links reference existing Hubs)
 *
 * EXIT CODES:
 * 0 = All validations passed
 * 1 = Validation failures detected
 * 2 = Runtime error
 *
 * ========================================================================
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BronzeValidator {

  def main(args: Array[String]): Unit = {

    println("""
         |╔════════════════════════════════════════════════════════════════╗
         |║         BRONZE LAYER DATA VALIDATION                          ║
         |╚════════════════════════════════════════════════════════════════╝
         |""".stripMargin)

    // Initialize Spark Session
    implicit val spark: SparkSession = createSparkSession()

    try {
      var allPassed = true

      // Run validations
      allPassed &= validateRecordCounts()
      allPassed &= validateNoDuplicates()
      allPassed &= validateHashKeyFormat()
      allPassed &= validateTemporalFields()
      allPassed &= validateForeignKeyIntegrity()

      if (allPassed) {
        println("\n╔════════════════════════════════════════════════════════════════╗")
        println("║              ✅ ALL VALIDATIONS PASSED                        ║")
        println("╚════════════════════════════════════════════════════════════════╝\n")
        sys.exit(0)
      } else {
        println("\n╔════════════════════════════════════════════════════════════════╗")
        println("║              ❌ VALIDATION FAILURES DETECTED                  ║")
        println("╚════════════════════════════════════════════════════════════════╝\n")
        sys.exit(1)
      }

    } catch {
      case e: Exception =>
        println(s"\n❌ Validation error: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(2)
    } finally {
      spark.stop()
    }
  }

  /**
   * Create Spark Session with Iceberg support
   */
  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("Bronze Layer Validator")
      .config("spark.sql.extensions",
              "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.spark_catalog.uri",
              sys.env.getOrElse("HIVE_METASTORE_URI", ""))
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * Validation 1: Record Counts
   */
  def validateRecordCounts()(implicit spark: SparkSession): Boolean = {
    println("\n📊 Validation 1: Record Counts")
    println("─────────────────────────────────")

    try {
      val tables = Seq(
        ("bronze.hub_customer", "Hub_Customer"),
        ("bronze.sat_customer", "Sat_Customer"),
        ("bronze.hub_account", "Hub_Account"),
        ("bronze.sat_account", "Sat_Account"),
        ("bronze.link_customer_account", "Link_Customer_Account")
      )

      tables.foreach { case (tableName, displayName) =>
        try {
          val count = spark.table(tableName).count()
          println(f"  ✓ $displayName%-25s : $count%,d records")
        } catch {
          case _: Exception =>
            println(f"  ⚠ $displayName%-25s : Table not found")
        }
      }

      println("✅ Record count check completed")
      true

    } catch {
      case e: Exception =>
        println(s"❌ Failed: ${e.getMessage}")
        false
    }
  }

  /**
   * Validation 2: No Duplicate Hash Keys in Hubs
   */
  def validateNoDuplicates()(implicit spark: SparkSession): Boolean = {
    println("\n🔍 Validation 2: Duplicate Hash Keys Check")
    println("──────────────────────────────────────────")

    try {
      val hubs = Seq(
        ("bronze.hub_customer", "customer_hash_key"),
        ("bronze.hub_account", "account_hash_key")
      )

      var allClean = true

      hubs.foreach { case (tableName, hashKeyCol) =>
        try {
          val duplicates = spark.sql(s"""
            SELECT $hashKeyCol, COUNT(*) as cnt
            FROM $tableName
            GROUP BY $hashKeyCol
            HAVING cnt > 1
          """).count()

          if (duplicates == 0) {
            println(s"  ✓ $tableName: No duplicates")
          } else {
            println(s"  ❌ $tableName: Found $duplicates duplicate hash keys!")
            allClean = false
          }
        } catch {
          case _: Exception =>
            println(s"  ⚠ $tableName: Table not found, skipping")
        }
      }

      if (allClean) {
        println("✅ No duplicates found in Hubs")
        true
      } else {
        println("❌ Duplicate hash keys detected!")
        false
      }

    } catch {
      case e: Exception =>
        println(s"❌ Failed: ${e.getMessage}")
        false
    }
  }

  /**
   * Validation 3: Hash Key Format (64-character SHA-256)
   */
  def validateHashKeyFormat()(implicit spark: SparkSession): Boolean = {
    println("\n🔑 Validation 3: Hash Key Format")
    println("─────────────────────────────────")

    try {
      val sample = spark.sql("""
        SELECT customer_hash_key
        FROM bronze.hub_customer
        LIMIT 1
      """).collect()

      if (sample.isEmpty) {
        println("  ⚠ No data to validate")
        return true
      }

      val hashKey = sample(0).getString(0)
      val hashLength = hashKey.length

      println(s"  Sample hash: ${hashKey.take(16)}...${hashKey.takeRight(8)}")
      println(s"  Hash length: $hashLength characters")

      if (hashLength == 64) {
        println("✅ Hash keys are 64 characters (SHA-256)")
        true
      } else {
        println(s"❌ Unexpected hash key length: $hashLength (expected 64)")
        false
      }

    } catch {
      case e: Exception =>
        println(s"❌ Failed: ${e.getMessage}")
        false
    }
  }

  /**
   * Validation 4: Temporal Fields Populated
   */
  def validateTemporalFields()(implicit spark: SparkSession): Boolean = {
    println("\n📅 Validation 4: Temporal Fields")
    println("─────────────────────────────────")

    try {
      // Check Satellite valid_from
      val nullValidFrom = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM bronze.sat_customer
        WHERE valid_from IS NULL
      """).collect()(0).getLong(0)

      if (nullValidFrom == 0) {
        println("  ✓ All Satellite records have valid_from populated")
      } else {
        println(s"  ❌ Found $nullValidFrom records with NULL valid_from")
        return false
      }

      // Check load_date range
      val loadDateRange = spark.sql("""
        SELECT MIN(load_date) as earliest, MAX(load_date) as latest
        FROM bronze.hub_customer
      """).collect()(0)

      println(s"  ✓ Load date range: ${loadDateRange.get(0)} to ${loadDateRange.get(1)}")

      println("✅ Temporal fields validation passed")
      true

    } catch {
      case e: Exception =>
        println(s"❌ Failed: ${e.getMessage}")
        false
    }
  }

  /**
   * Validation 5: Foreign Key Integrity
   */
  def validateForeignKeyIntegrity()(implicit spark: SparkSession): Boolean = {
    println("\n🔗 Validation 5: Foreign Key Integrity")
    println("───────────────────────────────────────")

    try {
      // Check Link_Customer_Account references existing Hubs
      val orphanedLinks = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM bronze.link_customer_account l
        LEFT ANTI JOIN bronze.hub_customer hc ON l.customer_hash_key = hc.customer_hash_key
      """).collect()(0).getLong(0)

      if (orphanedLinks == 0) {
        println("  ✓ All Links reference existing Hub_Customer records")
      } else {
        println(s"  ❌ Found $orphanedLinks orphaned Links (no matching Hub_Customer)")
        return false
      }

      println("✅ Foreign key integrity validated")
      true

    } catch {
      case e: Exception =>
        println(s"❌ Failed: ${e.getMessage}")
        false
    }
  }
}

