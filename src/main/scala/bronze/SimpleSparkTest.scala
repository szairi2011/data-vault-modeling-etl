package bronze

import org.apache.spark.sql.SparkSession

object SimpleSparkTest {

  def main(args: Array[String]): Unit = {
    println("Testing basic Spark initialization...")

    try {
      val spark = SparkSession.builder()
        .appName("Simple Spark Test")
        .master("local[*]")
        .getOrCreate()

      println("Spark initialized successfully!")
      println(s"Spark version: ${spark.version}")

      // Test basic DataFrame
      import spark.implicits._
      val df = Seq(1, 2, 3).toDF("number")
      df.show()

      spark.stop()
      println("Test completed successfully!")

    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
