# ============================================================================
# TEST BRONZE ETL - Quick validation script
# ============================================================================
#
# PURPOSE:
# Quickly test the RawVaultETL implementation by creating sample Avro files
# and running a small-scale load.
#
# USAGE:
#   .\scripts\windows\test-bronze-etl.ps1
#
# ============================================================================

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         TESTING RAW VAULT ETL (BRONZE LAYER)                  ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Step 1: Check prerequisites
Write-Host "📋 Checking prerequisites..." -ForegroundColor Yellow
Write-Host ""

$errors = @()

# Check Java
try {
    $javaVersion = java -version 2>&1 | Select-String "version"
    Write-Host "  OK Java found: $javaVersion" -ForegroundColor Green
}
catch {
    $errors += "Java not found. Install Java 11+"
}

# Check SBT
try {
    $sbtVersion = sbt --version 2>&1 | Select-String "sbt"
    Write-Host "  OK SBT found" -ForegroundColor Green
}
catch {
    $errors += "SBT not found. Install SBT 1.9+"
}

# Check Scala (optional - SBT includes Scala)
try {
    $scalaVersion = scala -version 2>&1 | Select-String "version"
    Write-Host "  OK Scala found: $scalaVersion" -ForegroundColor Green
}
catch {
    Write-Host "  INFO Scala standalone not found (SBT includes it)" -ForegroundColor Yellow
}

if ($errors.Count -gt 0) {
    Write-Host ""
    Write-Host "❌ Missing prerequisites:" -ForegroundColor Red
    $errors | ForEach-Object { Write-Host "   - $_" -ForegroundColor Red }
    exit 1
}

Write-Host ""

# Step 2: Compile the project
Write-Host "🔨 Compiling project..." -ForegroundColor Yellow
Write-Host ""

sbt compile

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "❌ Compilation failed. Fix errors and try again." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✅ Compilation successful" -ForegroundColor Green
Write-Host ""

# Step 3: Check if staging directory exists
Write-Host "📂 Checking staging directory..." -ForegroundColor Yellow

$stagingDir = "warehouse\staging"
if (-Not (Test-Path $stagingDir)) {
    Write-Host "  ⚠️  Staging directory not found: $stagingDir" -ForegroundColor Yellow
    Write-Host "  Creating directory..." -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $stagingDir -Force | Out-Null
    New-Item -ItemType Directory -Path "$stagingDir\customer" -Force | Out-Null
    New-Item -ItemType Directory -Path "$stagingDir\account" -Force | Out-Null
    New-Item -ItemType Directory -Path "$stagingDir\transaction_header" -Force | Out-Null
    New-Item -ItemType Directory -Path "$stagingDir\transaction_item" -Force | Out-Null
    Write-Host "  ✅ Staging directories created" -ForegroundColor Green
} else {
    Write-Host "  ✅ Staging directory exists" -ForegroundColor Green
}

Write-Host ""

# Step 4: Check for Avro files
Write-Host "🔍 Checking for Avro files..." -ForegroundColor Yellow

$avroFiles = Get-ChildItem -Path "$stagingDir" -Recurse -Filter "*.avro" -ErrorAction SilentlyContinue

if ($avroFiles.Count -eq 0) {
    Write-Host ""
    Write-Host "⚠️  No Avro files found in staging directory" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "NEXT STEPS:" -ForegroundColor Cyan
    Write-Host "  1. Seed the PostgreSQL database:" -ForegroundColor White
    Write-Host "     sbt 'runMain seeder.TransactionalDataSeeder'" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  2. Run NiFi to extract data to Avro files:" -ForegroundColor White
    Write-Host "     - Start NiFi: .\nifi\scripts\nifi_launcher.cmd" -ForegroundColor Gray
    Write-Host "     - Import flow template from nifi-flows/" -ForegroundColor Gray
    Write-Host "     - Start the flow in NiFi UI" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  3. Re-run this test script" -ForegroundColor White
    Write-Host ""

    # Ask if user wants to continue anyway (will create empty tables)
    $continue = Read-Host "Continue anyway to create empty tables? (y/n)"
    if ($continue -ne "y") {
        Write-Host ""
        Write-Host "Test aborted. Run NiFi extraction first." -ForegroundColor Yellow
        exit 0
    }
} else {
    Write-Host "  ✅ Found $($avroFiles.Count) Avro file(s)" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Files by entity:" -ForegroundColor White
    $avroFiles | Group-Object { $_.Directory.Name } | ForEach-Object {
        Write-Host "    - $($_.Name): $($_.Count) file(s)" -ForegroundColor Gray
    }
}

Write-Host ""

# Step 5: Run RawVaultETL (schema creation + load)
Write-Host "🚀 Running RawVaultETL..." -ForegroundColor Yellow
Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor DarkGray

sbt "runMain bronze.RawVaultETL --mode full"

Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor DarkGray
Write-Host ""

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ RawVaultETL failed. Check logs above for errors." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║              ✅ TEST COMPLETED SUCCESSFULLY                    ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "NEXT STEPS:" -ForegroundColor Cyan
Write-Host ""
Write-Host "  1. Query the Bronze tables:" -ForegroundColor White
Write-Host "     spark-sql --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \" -ForegroundColor Gray
Write-Host "               --conf spark.sql.catalog.spark_catalog.type=hive" -ForegroundColor Gray
Write-Host ""
Write-Host "     Then run:" -ForegroundColor White
Write-Host "     SELECT * FROM bronze.hub_customer LIMIT 10;" -ForegroundColor Gray
Write-Host "     SELECT * FROM bronze.sat_customer LIMIT 10;" -ForegroundColor Gray
Write-Host "     SELECT * FROM bronze.load_metadata ORDER BY load_start_timestamp DESC;" -ForegroundColor Gray
Write-Host ""
Write-Host "  2. Run Silver layer ETL:" -ForegroundColor White
Write-Host "     sbt 'runMain silver.BusinessVaultETL'" -ForegroundColor Gray
Write-Host ""
Write-Host "  3. Run Gold layer ETL:" -ForegroundColor White
Write-Host "     sbt 'runMain gold.DimensionalModelETL'" -ForegroundColor Gray
Write-Host ""

