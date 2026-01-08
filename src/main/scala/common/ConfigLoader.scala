package common

/**
 * ========================================================================
 * CONFIGURATION LOADER - UNIFIED CONFIG MANAGEMENT
 * ========================================================================
 *
 * PURPOSE:
 * Centralized configuration loading with cascading precedence:
 *   1. JVM System Properties (-Dkey=value)
 *   2. Environment Variables (KEY_NAME)
 *   3. application.properties file
 *   4. Hardcoded defaults
 *
 * LEARNING OBJECTIVES:
 * - Configuration management best practices
 * - Precedence hierarchy for flexibility
 * - Type-safe property access
 * - Environment-specific overrides
 *
 * USAGE EXAMPLES:
 * ```scala
 * // Get warehouse path with cascading resolution
 * val warehouse = ConfigLoader.getString(
 *   "spark.sql.catalog.spark_catalog.warehouse",
 *   "SPARK_WAREHOUSE",
 *   "warehouse"
 * )
 *
 * // Get optional HMS URI
 * val hmsUri = ConfigLoader.getOptionalString(
 *   "spark.sql.catalog.spark_catalog.uri",
 *   "HIVE_METASTORE_URI"
 * )
 * ```
 *
 * CONFIGURATION SOURCES:
 * 1. JVM Props: spark-submit --conf spark.driver.host=192.168.1.10
 * 2. Env Vars: export SPARK_DRIVER_HOST=192.168.1.10
 * 3. File: application.properties (in classpath)
 * 4. Default: Fallback hardcoded value
 *
 * ========================================================================
 */

import java.io.{File, FileInputStream, InputStream}
import java.util.Properties
import scala.util.{Try, Success, Failure}

object ConfigLoader {

  private val properties = new Properties()
  private var loaded = false

  /**
   * Load application.properties from classpath or file system
   */
  private def loadProperties(): Unit = {
    if (loaded) return

    // Try to load from classpath first (packaged JAR)
    val classpathStream: Option[InputStream] = Option(
      getClass.getClassLoader.getResourceAsStream("application.properties")
    )

    classpathStream match {
      case Some(stream) =>
        Try {
          properties.load(stream)
          stream.close()
          println("📖 Loaded configuration from classpath: application.properties")
        } match {
          case Success(_) => // OK
          case Failure(e) =>
            println(s"⚠️  Failed to load application.properties from classpath: ${e.getMessage}")
        }

      case None =>
        // Try to load from file system (development mode)
        val configFile = new File("src/main/resources/application.properties")
        if (configFile.exists()) {
          Try {
            val fileStream = new FileInputStream(configFile)
            properties.load(fileStream)
            fileStream.close()
            println(s"📖 Loaded configuration from file: ${configFile.getAbsolutePath}")
          } match {
            case Success(_) => // OK
            case Failure(e) =>
              println(s"⚠️  Failed to load application.properties from file: ${e.getMessage}")
          }
        } else {
          println("ℹ️  No application.properties found, using JVM props/env vars/defaults only")
        }
    }

    loaded = true
  }

  /**
   * Get string configuration with cascading precedence:
   * JVM props → Env vars → Properties file → Default
   *
   * @param propertyKey Key in JVM properties and .properties file (e.g., "spark.driver.host")
   * @param envKey Key in environment variables (e.g., "SPARK_DRIVER_HOST")
   * @param default Default value if not found in any source
   * @return Resolved configuration value
   */
  def getString(propertyKey: String, envKey: String, default: String): String = {
    loadProperties()

    // 1. Check JVM System Properties first (highest priority)
    Option(System.getProperty(propertyKey))
      .filter(_.nonEmpty)
      .orElse {
        // 2. Check environment variables
        sys.env.get(envKey).filter(_.nonEmpty)
      }
      .orElse {
        // 3. Check application.properties file
        Option(properties.getProperty(propertyKey)).filter(_.nonEmpty)
      }
      .getOrElse {
        // 4. Use default
        default
      }
  }

  /**
   * Get optional string configuration (returns None if not found)
   *
   * @param propertyKey Key in JVM properties and .properties file
   * @param envKey Key in environment variables
   * @return Some(value) if found, None otherwise
   */
  def getOptionalString(propertyKey: String, envKey: String): Option[String] = {
    loadProperties()

    Option(System.getProperty(propertyKey))
      .filter(_.nonEmpty)
      .orElse(sys.env.get(envKey).filter(_.nonEmpty))
      .orElse(Option(properties.getProperty(propertyKey)).filter(_.nonEmpty))
  }

  /**
   * Get boolean configuration
   *
   * @param propertyKey Key in JVM properties and .properties file
   * @param envKey Key in environment variables
   * @param default Default boolean value
   * @return Resolved boolean value
   */
  def getBoolean(propertyKey: String, envKey: String, default: Boolean): Boolean = {
    val value = getString(propertyKey, envKey, default.toString)
    value.toLowerCase match {
      case "true" | "yes" | "1" | "on" => true
      case "false" | "no" | "0" | "off" => false
      case _ =>
        println(s"⚠️  Invalid boolean value for $propertyKey: '$value', using default: $default")
        default
    }
  }

  /**
   * Get integer configuration
   *
   * @param propertyKey Key in JVM properties and .properties file
   * @param envKey Key in environment variables
   * @param default Default integer value
   * @return Resolved integer value
   */
  def getInt(propertyKey: String, envKey: String, default: Int): Int = {
    val value = getString(propertyKey, envKey, default.toString)
    Try(value.toInt).getOrElse {
      println(s"⚠️  Invalid integer value for $propertyKey: '$value', using default: $default")
      default
    }
  }

  /**
   * Get all properties as Map (for debugging)
   */
  def getAllProperties: Map[String, String] = {
    loadProperties()
    import scala.collection.JavaConverters._
    properties.asScala.toMap
  }

  /**
   * Validate required configuration keys exist
   *
   * @param requiredKeys List of (propertyKey, envKey, description) tuples
   * @throws IllegalStateException if any required key is missing
   */
  def validateRequired(requiredKeys: Seq[(String, String, String)]): Unit = {
    loadProperties()

    val missing = requiredKeys.filter { case (propKey, envKey, _) =>
      getOptionalString(propKey, envKey).isEmpty
    }

    if (missing.nonEmpty) {
      val errorMessage = missing.map { case (propKey, envKey, desc) =>
        s"  - $desc: Set -D$propKey=... or export $envKey=..."
      }.mkString("\n")

      throw new IllegalStateException(
        s"❌ Missing required configuration:\n$errorMessage"
      )
    }
  }

  /**
   * Print current configuration (for debugging)
   */
  def printConfig(keys: Seq[(String, String)]): Unit = {
    println("\n🔍 Current Configuration:")
    keys.foreach { case (propKey, envKey) =>
      val value = getOptionalString(propKey, envKey).getOrElse("(not set)")
      println(s"   $propKey = $value")
    }
    println()
  }
}

