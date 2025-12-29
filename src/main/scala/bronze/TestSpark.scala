package bronze

import org.apache.spark.sql.SparkSession

object TestSpark {

  def main(args: Array[String]): Unit = {
    println("Testing Spark initialization...")

    try {
      val spark = SparkSession.builder()
        .appName("Test Spark")
        .master("local[*]")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .enableHiveSupport()
        .getOrCreate()

      println("Spark initialized successfully!")
      println(s"Spark version: ${spark.version}")

      // Test basic SQL
      spark.sql("SHOW DATABASES").show()

      spark.stop()
      println("Test completed successfully!")

    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
