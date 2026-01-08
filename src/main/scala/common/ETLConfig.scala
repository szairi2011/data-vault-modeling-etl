package common

/**
 * ========================================================================
 * ETL CONFIGURATION - JOB-SPECIFIC PARAMETERS
 * ========================================================================
 *
 * PURPOSE:
 * Type-safe container for ETL job configuration parsed from:
 * - Command-line arguments (--mode full, --entity customer)
 * - Configuration files
 * - Defaults
 *
 * USAGE:
 * ```scala
 * val config = ETLConfig.fromArgs(args)
 * println(s"Running in ${config.mode} mode")
 * ```
 * ========================================================================
 */

import java.time.LocalDate

/**
 * Common ETL configuration
 *
 * @param mode Execution mode: "full" or "incremental"
 * @param entity Optional entity filter: "customer", "account", "transaction"
 * @param snapshotDate Date for PIT snapshots (default: today)
 * @param recordSource Source system identifier
 * @param buildPIT Build Point-In-Time tables (for Silver layer)
 * @param buildBridge Build Bridge tables (for Silver layer)
 * @param rebuildDims Rebuild dimensions (for Gold layer)
 * @param rebuildFacts Rebuild facts (for Gold layer)
 */
case class ETLConfig(
  mode: String = "incremental",
  entity: Option[String] = None,
  snapshotDate: LocalDate = LocalDate.now(),
  recordSource: String = "PostgreSQL",
  buildPIT: Boolean = false,
  buildBridge: Boolean = false,
  rebuildDims: Boolean = false,
  rebuildFacts: Boolean = false
)

object ETLConfig {

  /**
   * Parse configuration from command-line arguments
   *
   * Supported arguments:
   * - --mode [full|incremental]
   * - --entity [customer|account|transaction]
   * - --date YYYY-MM-DD
   * - --source [PostgreSQL|...]
   * - --build-pit
   * - --build-bridge
   * - --rebuild-dims
   * - --rebuild-facts
   * - --all (for Silver: build PIT + bridge; for Gold: rebuild dims + facts)
   *
   * @param args Command-line arguments array
   * @return Parsed ETL configuration
   */
  def fromArgs(args: Array[String]): ETLConfig = {
    val mode = getArgValue(args, "--mode").getOrElse("incremental")
    val entity = getArgValue(args, "--entity")
    val dateStr = getArgValue(args, "--date")
    val source = getArgValue(args, "--source").getOrElse("PostgreSQL")

    val snapshotDate = dateStr.map(LocalDate.parse).getOrElse(LocalDate.now())

    val buildAll = args.contains("--all")
    val buildPIT = args.contains("--build-pit") || buildAll
    val buildBridge = args.contains("--build-bridge") || buildAll
    val rebuildAll = args.contains("--rebuild-all") || buildAll
    val rebuildDims = args.contains("--rebuild-dims") || rebuildAll
    val rebuildFacts = args.contains("--rebuild-facts") || rebuildAll

    ETLConfig(
      mode = mode,
      entity = entity,
      snapshotDate = snapshotDate,
      recordSource = source,
      buildPIT = buildPIT,
      buildBridge = buildBridge,
      rebuildDims = rebuildDims,
      rebuildFacts = rebuildFacts
    )
  }

  /**
   * Parse configuration from Map (for programmatic invocation)
   */
  def fromMap(config: Map[String, String]): ETLConfig = {
    ETLConfig(
      mode = config.getOrElse("mode", "incremental"),
      entity = config.get("entity").filter(_.nonEmpty),
      snapshotDate = config.get("date").map(LocalDate.parse).getOrElse(LocalDate.now()),
      recordSource = config.getOrElse("source", "PostgreSQL"),
      buildPIT = config.getOrElse("buildPIT", "false").toBoolean,
      buildBridge = config.getOrElse("buildBridge", "false").toBoolean,
      rebuildDims = config.getOrElse("rebuildDims", "false").toBoolean,
      rebuildFacts = config.getOrElse("rebuildFacts", "false").toBoolean
    )
  }

  private def getArgValue(args: Array[String], key: String): Option[String] = {
    val index = args.indexOf(key)
    if (index >= 0 && index + 1 < args.length) {
      Some(args(index + 1))
    } else {
      None
    }
  }
}

