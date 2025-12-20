###################################################################################
# Complete Windows Native Setup Script (100% Docker-Free)
###################################################################################
#
# PURPOSE:
# Full environment setup for Data Vault POC on Windows using only native
# installations - NO Docker required!
#
# WHAT THIS SCRIPT DOES:
# 1. Verify prerequisites (PostgreSQL, Java, SBT)
# 2. Create PostgreSQL database and tables
# 3. Download & install Confluent Schema Registry (Windows native)
# 4. Start Schema Registry (native Windows process)
# 5. Register Avro schemas
# 6. Seed reference and transactional data
# 7. Create staging directories
# 8. Verify all components
#
# PREREQUISITES (All Windows Native):
# - PostgreSQL 12+ installed
# - Java 11+ installed (required for Schema Registry)
# - SBT installed
# - Internet connection (for first-time Confluent Platform download ~200MB)
#
# NO DOCKER REQUIRED! 🎉
#
# USAGE:
#   .\scripts\windows\01-setup.ps1
#
# FIRST RUN:
# Downloads Confluent Platform Community Edition (~200MB one-time download)
#
###################################################################################

param(
    [string]$ProjectRoot = "C:\dev\projects\data-vault-modeling-etl",
    [string]$PostgresBin = "C:\Program Files\PostgreSQL\16\bin",
    [string]$NiFiHome = "C:\nifi",
    [string]$PostgresUser = "postgres",
    [string]$PostgresPassword = "postgres"
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         DATA VAULT 2.0 POC - WINDOWS NATIVE SETUP             ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Change to project root
Set-Location $ProjectRoot

# ============================================================================
# STEP 1: Verify Prerequisites
# ============================================================================

Write-Host "┌────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ STEP 1: Verifying Prerequisites                               │" -ForegroundColor Yellow
Write-Host "└────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
Write-Host ""

# Check PostgreSQL
Write-Host "📦 Checking PostgreSQL..." -ForegroundColor Cyan
try {
    $psqlPath = Join-Path $PostgresBin "psql.exe"
    & $psqlPath -U $PostgresUser -c "SELECT version();" 2>&1 | Out-Null
    Write-Host "   ✅ PostgreSQL found" -ForegroundColor Green
} catch {
    Write-Host "   ❌ PostgreSQL not accessible" -ForegroundColor Red
    Write-Host "   Please install PostgreSQL or check path: $PostgresBin" -ForegroundColor Yellow
    exit 1
}

# Check Java
Write-Host "📦 Checking Java..." -ForegroundColor Cyan
try {
    $javaVersion = java -version 2>&1 | Select-String "version" | Select-Object -First 1
    Write-Host "   ✅ Java found: $javaVersion" -ForegroundColor Green
} catch {
    Write-Host "   ❌ Java not found" -ForegroundColor Red
    Write-Host "   Please install Java 11+ and add to PATH" -ForegroundColor Yellow
    exit 1
}

# Check SBT
Write-Host "📦 Checking SBT..." -ForegroundColor Cyan
try {
    $sbtVersion = sbt about 2>&1 | Select-String "sbt version" | Select-Object -First 1
    Write-Host "   ✅ SBT found" -ForegroundColor Green
} catch {
    Write-Host "   ❌ SBT not found" -ForegroundColor Red
    Write-Host "   Please install SBT from https://www.scala-sbt.org/download.html" -ForegroundColor Yellow
    exit 1
}

# Note: Docker is NOT required - this setup is 100% Windows native!

# Check NiFi
Write-Host "📦 Checking NiFi..." -ForegroundColor Cyan
if (Test-Path $NiFiHome) {
    Write-Host "   ✅ NiFi found at $NiFiHome" -ForegroundColor Green
} else {
    Write-Host "   ⚠️  NiFi not found at $NiFiHome" -ForegroundColor Yellow
    Write-Host "   Update `$NiFiHome parameter if installed elsewhere" -ForegroundColor Gray
}

Write-Host ""

# ============================================================================
# STEP 2: Create PostgreSQL Database and Tables
# ============================================================================

Write-Host "┌────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ STEP 2: Setting Up PostgreSQL Source Database                 │" -ForegroundColor Yellow
Write-Host "└────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
Write-Host ""

$psqlExe = Join-Path $PostgresBin "psql.exe"
$env:PGPASSWORD = $PostgresPassword

Write-Host "📦 Creating banking_source database..." -ForegroundColor Cyan
& $psqlExe -U $PostgresUser -c "CREATE DATABASE banking_source;" 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0 -or $LASTEXITCODE -eq 1) {
    Write-Host "   ✅ Database created (or already exists)" -ForegroundColor Green
}

Write-Host "📦 Creating tables..." -ForegroundColor Cyan
& $psqlExe -U $PostgresUser -d banking_source -f "$ProjectRoot\source-system\sql\02_create_tables.sql"
if ($LASTEXITCODE -eq 0) {
    Write-Host "   ✅ Tables created" -ForegroundColor Green
}

Write-Host ""

# ============================================================================
# STEP 3: Setup Confluent Schema Registry (Windows Native - No Docker)
# ============================================================================

Write-Host "┌────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ STEP 3: Setting up Schema Registry (Windows Native)           │" -ForegroundColor Yellow
Write-Host "└────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
Write-Host ""

$confluentVersion = "7.5.0"
$confluentHome = "C:\confluent\confluent-$confluentVersion"
$downloadUrl = "https://packages.confluent.io/archive/7.5/confluent-community-$confluentVersion.zip"
$downloadPath = "$env:USERPROFILE\Downloads\confluent-community-$confluentVersion.zip"

# Check if already installed
if (Test-Path $confluentHome) {
    Write-Host "   ✅ Confluent Platform already installed at $confluentHome" -ForegroundColor Green
} else {
    Write-Host "📦 Downloading Confluent Platform Community ($confluentVersion)..." -ForegroundColor Cyan
    Write-Host "   This is a one-time download (~200MB)..." -ForegroundColor Gray

    try {
        # Create directory
        New-Item -Path "C:\confluent" -ItemType Directory -Force | Out-Null

        # Download
        if (-not (Test-Path $downloadPath)) {
            Invoke-WebRequest -Uri $downloadUrl -OutFile $downloadPath
            Write-Host "   ✅ Downloaded Confluent Platform" -ForegroundColor Green
        } else {
            Write-Host "   ℹ️  Using cached download" -ForegroundColor Gray
        }

        # Extract
        Write-Host "   📦 Extracting..." -ForegroundColor Cyan
        Expand-Archive -Path $downloadPath -DestinationPath "C:\confluent" -Force

        # Set environment variable
        [System.Environment]::SetEnvironmentVariable("CONFLUENT_HOME", $confluentHome, "User")
        $env:CONFLUENT_HOME = $confluentHome

        Write-Host "   ✅ Confluent Platform installed successfully" -ForegroundColor Green
    } catch {
        Write-Host "   ❌ Failed to download/install Confluent Platform: $_" -ForegroundColor Red
        Write-Host ""
        Write-Host "Manual installation instructions:" -ForegroundColor Yellow
        Write-Host "1. Download from: https://www.confluent.io/download/" -ForegroundColor White
        Write-Host "2. Select Community edition, Windows zip" -ForegroundColor White
        Write-Host "3. Extract to: C:\confluent\confluent-7.5.0" -ForegroundColor White
        Write-Host "4. Set CONFLUENT_HOME environment variable" -ForegroundColor White
        exit 1
    }
}

# Verify Java is installed
Write-Host "☕ Verifying Java installation..." -ForegroundColor Cyan
try {
    $javaVersion = java -version 2>&1 | Select-String "version" | Select-Object -First 1
    Write-Host "   ✅ Java found: $javaVersion" -ForegroundColor Green
} catch {
    Write-Host "   ❌ Java is not installed. Schema Registry requires Java 11+" -ForegroundColor Red
    Write-Host "   Download from: https://adoptium.net/temurin/releases/" -ForegroundColor Yellow
    exit 1
}

# Start Schema Registry
Write-Host "🚀 Starting Schema Registry..." -ForegroundColor Cyan

# Kill any existing Schema Registry process
Get-Process | Where-Object {$_.CommandLine -like "*schema-registry*"} | Stop-Process -Force 2>$null | Out-Null

# Start Schema Registry in background
$schemaRegistryBat = "$confluentHome\bin\windows\schema-registry-start.bat"
$schemaRegistryConfig = "$confluentHome\etc\schema-registry\schema-registry.properties"

if (-not (Test-Path $schemaRegistryBat)) {
    Write-Host "   ❌ Schema Registry start script not found at: $schemaRegistryBat" -ForegroundColor Red
    exit 1
}

try {
    # Start in new window (background process)
    $process = Start-Process -FilePath $schemaRegistryBat -ArgumentList $schemaRegistryConfig -WindowStyle Minimized -PassThru

    Write-Host "   ⏳ Waiting for Schema Registry to start (30 seconds)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 30

    # Verify Schema Registry is running
    $schemaRegistryUrl = "http://localhost:8081"
    $maxAttempts = 6
    $attempt = 0
    $ready = $false

    while ($attempt -lt $maxAttempts -and -not $ready) {
        $attempt++
        try {
            $response = Invoke-WebRequest -Uri $schemaRegistryUrl -UseBasicParsing -TimeoutSec 5 -ErrorAction SilentlyContinue
            $ready = $true
            Write-Host "   ✅ Schema Registry started successfully on port 8081" -ForegroundColor Green
        } catch {
            if ($attempt -lt $maxAttempts) {
                Write-Host "   ⏳ Retry $attempt/$maxAttempts..." -ForegroundColor Gray
                Start-Sleep -Seconds 10
            }
        }
    }

    if (-not $ready) {
        Write-Host "   ⚠️  Schema Registry may not be ready yet" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "Manual verification:" -ForegroundColor Yellow
        Write-Host "   curl http://localhost:8081/" -ForegroundColor White
        Write-Host "   or: Invoke-RestMethod -Uri http://localhost:8081/ -Method Get" -ForegroundColor White
        Write-Host ""
        Write-Host "If Schema Registry didn't start, manually run:" -ForegroundColor Yellow
        Write-Host "   cd $confluentHome\bin\windows" -ForegroundColor White
        Write-Host "   .\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry.properties" -ForegroundColor White
    }

} catch {
    Write-Host "   ❌ Failed to start Schema Registry: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "Manual start command:" -ForegroundColor Yellow
    Write-Host "   cd $confluentHome\bin\windows" -ForegroundColor White
    Write-Host "   .\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry.properties" -ForegroundColor White
    exit 1
}

Set-Location $ProjectRoot
Write-Host ""

# ============================================================================
# STEP 4: Register Avro Schemas
# ============================================================================

Write-Host "┌────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ STEP 4: Registering Avro Schemas                              │" -ForegroundColor Yellow
Write-Host "└────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
Write-Host ""

Write-Host "📦 Registering schemas to Schema Registry..." -ForegroundColor Cyan
try {
    & "$ProjectRoot\nifi\scripts\register-schemas.ps1"
} catch {
    Write-Host "   ⚠️  Schema registration failed (may need to retry): $_" -ForegroundColor Yellow
}

Write-Host ""

# ============================================================================
# STEP 5: Seed Data
# ============================================================================

Write-Host "┌────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ STEP 5: Seeding Reference and Transactional Data              │" -ForegroundColor Yellow
Write-Host "└────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
Write-Host ""

Write-Host "📦 Seeding reference data (products, branches, categories)..." -ForegroundColor Cyan
sbt "runMain seeder.ReferenceDataSeeder"
if ($LASTEXITCODE -eq 0) {
    Write-Host "   ✅ Reference data seeded" -ForegroundColor Green
}

Write-Host ""
Write-Host "📦 Seeding transactional data (customers, accounts, transactions)..." -ForegroundColor Cyan
Write-Host "   This may take 2-3 minutes..." -ForegroundColor Gray
sbt "runMain seeder.TransactionalDataSeeder"
if ($LASTEXITCODE -eq 0) {
    Write-Host "   ✅ Transactional data seeded" -ForegroundColor Green
}

Write-Host ""

# ============================================================================
# STEP 6: Create Directory Structure
# ============================================================================

Write-Host "┌────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ STEP 6: Creating Directory Structure                          │" -ForegroundColor Yellow
Write-Host "└────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
Write-Host ""

$directories = @(
    "warehouse\staging\customer",
    "warehouse\staging\account",
    "warehouse\staging\transaction_header",
    "warehouse\staging\transaction_item",
    "warehouse\bronze",
    "warehouse\silver",
    "warehouse\gold",
    "logs\spark-events",
    "tmp"
)

foreach ($dir in $directories) {
    $fullPath = Join-Path $ProjectRoot $dir
    if (-not (Test-Path $fullPath)) {
        New-Item -ItemType Directory -Path $fullPath -Force | Out-Null
        Write-Host "   ✅ Created: $dir" -ForegroundColor Green
    } else {
        Write-Host "   ℹ️  Exists: $dir" -ForegroundColor Gray
    }
}

Write-Host ""

# ============================================================================
# STEP 7: Verify Data
# ============================================================================

Write-Host "┌────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ STEP 7: Verifying Seeded Data                                 │" -ForegroundColor Yellow
Write-Host "└────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
Write-Host ""

Write-Host "📊 Data counts:" -ForegroundColor Cyan

$queries = @(
    @{Name="Customers"; Query="SELECT COUNT(*) FROM banking.customer;"},
    @{Name="Accounts"; Query="SELECT COUNT(*) FROM banking.account;"},
    @{Name="Transactions"; Query="SELECT COUNT(*) FROM banking.transaction_header;"},
    @{Name="Transaction Items"; Query="SELECT COUNT(*) FROM banking.transaction_item;"},
    @{Name="Products"; Query="SELECT COUNT(*) FROM banking.product;"},
    @{Name="Branches"; Query="SELECT COUNT(*) FROM banking.branch;"}
)

foreach ($q in $queries) {
    $result = & $psqlExe -U $PostgresUser -d banking_source -t -A -c $q.Query
    Write-Host "   $($q.Name): $result" -ForegroundColor White
}

Write-Host ""

# ============================================================================
# FINAL SUMMARY
# ============================================================================

Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║                    SETUP COMPLETE! 🎉                          ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""

Write-Host "✅ Components Ready:" -ForegroundColor Cyan
Write-Host "   • PostgreSQL: localhost:5432 (database: banking_source)" -ForegroundColor White
Write-Host "   • Schema Registry: http://localhost:8081" -ForegroundColor White
Write-Host "   • Staging Zone: $ProjectRoot\warehouse\staging" -ForegroundColor White
Write-Host ""

Write-Host "📋 NEXT STEPS:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1️⃣  Configure NiFi:" -ForegroundColor Cyan
Write-Host "   • Open NiFi UI: http://localhost:8080/nifi (or https://localhost:8443/nifi)" -ForegroundColor White
Write-Host "   • Import template: $ProjectRoot\nifi\templates\01_source_to_staging.xml" -ForegroundColor White
Write-Host "   • Add DBCPConnectionPool controller service:" -ForegroundColor White
Write-Host "     - URL: jdbc:postgresql://localhost:5432/banking_source" -ForegroundColor Gray
Write-Host "     - User: postgres" -ForegroundColor Gray
Write-Host "     - Driver: org.postgresql.Driver" -ForegroundColor Gray
Write-Host "   • Add AvroSchemaRegistry controller service:" -ForegroundColor White
Write-Host "     - URL: http://localhost:8081" -ForegroundColor Gray
Write-Host "   • Start all processors" -ForegroundColor White
Write-Host ""

Write-Host "2️⃣  Verify CDC Extraction:" -ForegroundColor Cyan
Write-Host "   • Check Avro files created: dir warehouse\staging\customer" -ForegroundColor White
Write-Host "   • Verify with avro-tools (optional)" -ForegroundColor White
Write-Host ""

Write-Host "3️⃣  Run ETL Pipelines:" -ForegroundColor Cyan
Write-Host "   • Bronze Layer: .\scripts\windows\02-run-etl.ps1 -Layer bronze" -ForegroundColor White
Write-Host "   • Silver Layer: .\scripts\windows\02-run-etl.ps1 -Layer silver" -ForegroundColor White
Write-Host "   • Gold Layer: .\scripts\windows\02-run-etl.ps1 -Layer gold" -ForegroundColor White
Write-Host "   • All Layers: .\scripts\windows\02-run-etl.ps1 -Layer all" -ForegroundColor White
Write-Host ""

Write-Host "4️⃣  Query Data:" -ForegroundColor Cyan
Write-Host "   • Spark SQL: spark-sql --conf spark.sql.catalog.spark_catalog=..." -ForegroundColor White
Write-Host "   • Query Interface: sbt 'runMain semantic.QueryInterface'" -ForegroundColor White
Write-Host ""

Write-Host "5️⃣  Test Schema Evolution:" -ForegroundColor Cyan
Write-Host "   • Run: .\scripts\windows\03-schema-evolution.ps1" -ForegroundColor White
Write-Host "   • Demonstrates Data Vault resilience to schema changes" -ForegroundColor White
Write-Host ""

Write-Host "📚 Documentation:" -ForegroundColor Yellow
Write-Host "   • Setup Guide: docs\01_setup_guide.md" -ForegroundColor White
Write-Host "   • Architecture: docs\03_architecture.md" -ForegroundColor White
Write-Host "   • NiFi CDC Guide: docs\05_nifi_cdc_guide.md" -ForegroundColor White
Write-Host ""

Write-Host "🐛 Troubleshooting:" -ForegroundColor Yellow
Write-Host "   • PostgreSQL: Check service is running (services.msc)" -ForegroundColor White
Write-Host "   • Schema Registry: docker ps | docker logs banking-schema-registry" -ForegroundColor White
Write-Host "   • NiFi: Check logs in $NiFiHome\logs" -ForegroundColor White
Write-Host ""

Write-Host "Press any key to exit..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

