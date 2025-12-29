h# ============================================================================
# CHECK BRONZE LAYER STATUS
# ============================================================================
#
# PURPOSE:
# Verify the current state of the Bronze layer:
# - Check if Avro files exist in staging
# - Check if Bronze tables were created
# - Check if data was loaded
# - Show record counts
#
# USAGE:
#   .\scripts\windows\check-bronze-status.ps1
#
# ============================================================================

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "          BRONZE LAYER STATUS CHECK" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

# ===========================================================================
# 1. CHECK STAGING AVRO FILES
# ===========================================================================

Write-Host "1. CHECKING STAGING AVRO FILES" -ForegroundColor Yellow
Write-Host "----------------------------------------------------------------"

$stagingDir = "warehouse\staging"
$entities = @("customer", "account", "transaction_header", "transaction_item")

$avroFilesFound = $false

foreach ($entity in $entities) {
    $entityPath = Join-Path $stagingDir $entity

    if (Test-Path $entityPath) {
        $files = Get-ChildItem -Path $entityPath -File -Recurse | Where-Object { $_.Extension -eq ".avro" -or $_.Name -like "*.avro*" }

        if ($files) {
            $avroFilesFound = $true
            Write-Host "  OK $entity : $($files.Count) file(s)" -ForegroundColor Green
            $files | ForEach-Object {
                $sizeKB = [math]::Round($_.Length / 1KB, 2)
                Write-Host "     - $($_.Name) ($sizeKB KB, $($_.LastWriteTime))" -ForegroundColor Gray
            }
        } else {
            Write-Host "  EMPTY $entity : No Avro files found" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  MISSING $entity : Directory not found" -ForegroundColor Red
    }
}

if (-not $avroFilesFound) {
    Write-Host ""
    Write-Host "  WARNING: No Avro files found in staging!" -ForegroundColor Yellow
    Write-Host "  You need to extract data from PostgreSQL using NiFi first." -ForegroundColor Yellow
}

Write-Host ""

# ===========================================================================
# 2. CHECK WAREHOUSE BRONZE DIRECTORY
# ===========================================================================

Write-Host "2. CHECKING WAREHOUSE BRONZE DIRECTORY" -ForegroundColor Yellow
Write-Host "----------------------------------------------------------------"

$bronzeDir = "warehouse\bronze.db"

if (Test-Path $bronzeDir) {
    Write-Host "  OK Bronze database directory exists" -ForegroundColor Green

    # List subdirectories (tables)
    $tables = Get-ChildItem -Path $bronzeDir -Directory

    if ($tables) {
        Write-Host "  OK Found $($tables.Count) table(s):" -ForegroundColor Green
        foreach ($table in $tables) {
            Write-Host "     - $($table.Name)" -ForegroundColor Gray

            # Check for data files
            $dataFiles = Get-ChildItem -Path $table.FullName -Recurse -File | Where-Object { $_.Extension -eq ".parquet" }
            if ($dataFiles) {
                $totalSizeMB = [math]::Round(($dataFiles | Measure-Object -Property Length -Sum).Sum / 1MB, 2)
                Write-Host "       ($($dataFiles.Count) Parquet file(s), $totalSizeMB MB)" -ForegroundColor DarkGray
            }
        }
    } else {
        Write-Host "  EMPTY No tables found in bronze database" -ForegroundColor Yellow
    }
} else {
    Write-Host "  MISSING Bronze database directory not found" -ForegroundColor Red
    Write-Host "  Tables have not been created yet." -ForegroundColor Yellow
}

Write-Host ""

# ===========================================================================
# 3. CHECK HIVE METASTORE
# ===========================================================================

Write-Host "3. CHECKING HIVE METASTORE" -ForegroundColor Yellow
Write-Host "----------------------------------------------------------------"

$metastoreDir = "metastore_db"

if (Test-Path $metastoreDir) {
    Write-Host "  OK Hive Metastore directory exists" -ForegroundColor Green

    $dbFiles = Get-ChildItem -Path $metastoreDir -File -Recurse
    if ($dbFiles) {
        $totalSizeKB = [math]::Round(($dbFiles | Measure-Object -Property Length -Sum).Sum / 1KB, 2)
        Write-Host "     Metastore size: $totalSizeKB KB" -ForegroundColor Gray
    }
} else {
    Write-Host "  MISSING Hive Metastore not initialized" -ForegroundColor Red
}

Write-Host ""

# ===========================================================================
# 4. ESTIMATE TABLE STATUS (via file system inspection)
# ===========================================================================

Write-Host "4. ESTIMATING TABLE STATUS (via file system)" -ForegroundColor Yellow
Write-Host "----------------------------------------------------------------"

if (Test-Path $bronzeDir) {
    $expectedTables = @(
        "hub_customer", "sat_customer",
        "hub_account", "sat_account", "link_customer_account",
        "hub_transaction", "sat_transaction",
        "hub_transaction_item", "sat_transaction_item", "link_transaction_item",
        "load_metadata"
    )

    Write-Host "  Expected Bronze Tables Status:" -ForegroundColor White
    Write-Host ""

    foreach ($table in $expectedTables) {
        $tablePath = Join-Path $bronzeDir $table

        if (Test-Path $tablePath) {
            # Check for data files
            $dataFiles = Get-ChildItem -Path $tablePath -Recurse -File -ErrorAction SilentlyContinue |
                         Where-Object { $_.Extension -eq ".parquet" }

            if ($dataFiles) {
                $totalSizeKB = [math]::Round(($dataFiles | Measure-Object -Property Length -Sum).Sum / 1KB, 2)
                Write-Host "  OK $table : EXISTS ($($dataFiles.Count) file(s), $totalSizeKB KB)" -ForegroundColor Green
            } else {
                Write-Host "  EMPTY $table : EXISTS but no data files" -ForegroundColor Yellow
            }
        } else {
            Write-Host "  MISSING $table : NOT FOUND" -ForegroundColor Red
        }
    }

    Write-Host ""
    Write-Host "  NOTE: To see exact record counts, use:" -ForegroundColor Gray
    Write-Host "    sbt 'runMain bronze.utils.TableCounter'" -ForegroundColor DarkGray
    Write-Host "    (or query directly via spark-sql/beeline)" -ForegroundColor DarkGray
} else {
    Write-Host "  SKIP Bronze directory not found" -ForegroundColor Yellow
}

Write-Host ""

# ===========================================================================
# 5. SUMMARY & NEXT STEPS
# ===========================================================================

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "                        SUMMARY" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

if ($avroFilesFound -and (Test-Path $bronzeDir)) {
    Write-Host "STATUS: Bronze layer appears to be set up" -ForegroundColor Green
    Write-Host ""
    Write-Host "NEXT STEPS:" -ForegroundColor White
    Write-Host "  1. Query tables using Spark SQL:" -ForegroundColor Gray
    Write-Host "     sbt console" -ForegroundColor DarkGray
    Write-Host "     scala> spark.sql(\"\"\"SELECT * FROM bronze.hub_customer LIMIT 10\"\"\").show()" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "  2. Check load metadata:" -ForegroundColor Gray
    Write-Host "     spark.sql(\"\"\"SELECT * FROM bronze.load_metadata ORDER BY load_start_timestamp DESC\"\"\").show()" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "  3. Run incremental load:" -ForegroundColor Gray
    Write-Host "     sbt \"\"runMain bronze.RawVaultETL\"\"" -ForegroundColor DarkGray

} elseif (-not $avroFilesFound) {
    Write-Host "STATUS: No Avro files in staging" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "NEXT STEPS:" -ForegroundColor White
    Write-Host "  1. Start NiFi and extract data:" -ForegroundColor Gray
    Write-Host "     .\nifi\scripts\nifi_launcher.cmd" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "  2. Import NiFi flow template and start extraction" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  3. Run Bronze ETL after extraction:" -ForegroundColor Gray
    Write-Host "     sbt \"\"runMain bronze.RawVaultETL --mode full\"\"" -ForegroundColor DarkGray

} elseif (-not (Test-Path $bronzeDir)) {
    Write-Host "STATUS: Avro files exist but Bronze tables not created" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "NEXT STEPS:" -ForegroundColor White
    Write-Host "  1. Run Bronze ETL to create tables and load data:" -ForegroundColor Gray
    Write-Host "     sbt \"\"runMain bronze.RawVaultETL --mode full\"\"" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "  2. Verify tables were created:" -ForegroundColor Gray
    Write-Host "     .\scripts\windows\check-bronze-status.ps1" -ForegroundColor DarkGray
}

Write-Host ""

