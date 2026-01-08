package common

/**
 * ========================================================================
 * SPARK SESSION FACTORY - CENTRALIZED SESSION MANAGEMENT
 * ========================================================================
 *
 * PURPOSE:
 * Create SparkSession with unified configuration from ConfigLoader.
 * Supports both HadoopCatalog and HiveCatalog for Apache Iceberg.
 *
 * CONFIGURATION SOURCES:
 * - JVM properties: -Dspark.driver.host=localhost
 * - Environment variables: export SPARK_DRIVER_HOST=localhost
 * - application.properties file
 * - Hardcoded defaults
 *
 * USAGE:
 * ```scala
 * implicit val spark = SparkSessionFactory.createSession("My ETL Job")
 * ```
 * ========================================================================
 */

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  /**
   * Create SparkSession with Iceberg support
   *
   * @param appName Application name for Spark UI
   * @return Configured SparkSession
   */
  def createSession(appName: String): SparkSession = {
    println(s"\n🚀 Initializing Spark Session: $appName")

    // Load Hadoop home if provided (for Windows winutils)
    ConfigLoader.getOptionalString("hadoop.home.dir", "HADOOP_HOME").foreach { dir =>
      System.setProperty("hadoop.home.dir", dir)
      println(s"🔧 hadoop.home.dir = $dir")
    }

    // Networking configuration
    val preferIPv4 = ConfigLoader.getBoolean("java.net.preferIPv4Stack", "JAVA_NET_PREFER_IPV4", true)
    System.setProperty("java.net.preferIPv4Stack", preferIPv4.toString)

    val driverHost = ConfigLoader.getString("spark.driver.host", "SPARK_DRIVER_HOST", "127.0.0.1")
    val bindAddress = ConfigLoader.getString("spark.driver.bindAddress", "SPARK_DRIVER_BIND_ADDRESS", driverHost)
    val driverPortOpt = ConfigLoader.getOptionalString("spark.driver.port", "SPARK_DRIVER_PORT")

    // Warehouse and catalog configuration
    val warehouse = ConfigLoader.getString(
      "spark.sql.catalog.spark_catalog.warehouse",
      "SPARK_WAREHOUSE",
      "warehouse"
    )

    val hmsUriOpt = ConfigLoader.getOptionalString(
      "spark.sql.catalog.spark_catalog.uri",
      "HIVE_METASTORE_URI"
    )

    // Base Spark builder with Iceberg extensions
    var builder = SparkSession.builder()
      .appName(appName)
      .master(ConfigLoader.getString("spark.master", "SPARK_MASTER", "local[*]"))
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.driver.host", driverHost)
      .config("spark.driver.bindAddress", bindAddress)
      .config("spark.sql.catalog.spark_catalog.warehouse", warehouse)

    // Optional driver port
    driverPortOpt.foreach(p => builder = builder.config("spark.driver.port", p))

    // Choose catalog type based on HMS presence
    val finalBuilder = hmsUriOpt match {
      case Some(uri) =>
        println(s"🔧 Using Iceberg catalog with Hive Metastore -> uri = $uri")
        builder
          .config("spark.sql.catalog.spark_catalog.type", "hive")
          .config("spark.sql.catalog.spark_catalog.uri", uri)
      case None =>
        println("🔧 Using Iceberg HadoopCatalog (no Hive Metastore)")
        builder.config("spark.sql.catalog.spark_catalog.type", "hadoop")
    }

    // Enable Hive support only when using external HMS
    val spark = if (hmsUriOpt.isDefined) {
      finalBuilder.enableHiveSupport().getOrCreate()
    } else {
      finalBuilder.getOrCreate()
    }

    spark.sparkContext.setLogLevel("WARN")
    println(s"✅ Spark ${spark.version} initialized (catalog=spark_catalog, warehouse=$warehouse)")

    // Print configuration for debugging
    ConfigLoader.printConfig(Seq(
      ("spark.driver.host", "SPARK_DRIVER_HOST"),
      ("spark.sql.catalog.spark_catalog.warehouse", "SPARK_WAREHOUSE"),
      ("spark.sql.catalog.spark_catalog.uri", "HIVE_METASTORE_URI")
    ))

    spark
  }
}

