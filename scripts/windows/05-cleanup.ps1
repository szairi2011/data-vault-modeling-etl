###################################################################################
# Environment Cleanup Script (Windows PowerShell)
###################################################################################
#
# PURPOSE:
# Reset the entire Data Vault environment to pristine state for testing.
#
# WARNING:
# This script will DELETE all data, metadata, and configurations.
# Use only for development/testing, never in production!
#
# USAGE:
#   .\scripts\windows\05-cleanup.ps1
#   .\scripts\windows\05-cleanup.ps1 -Confirm:$false  # Skip confirmation
#
###################################################################################

param(
    [string]$ProjectRoot = "C:\dev\projects\data-vault-modeling-etl",
    [switch]$KeepPostgres = $false,
    [switch]$KeepDocker = $false
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Red
Write-Host "║            ENVIRONMENT CLEANUP - DATA VAULT POC               ║" -ForegroundColor Red
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Red
Write-Host ""

Write-Host "⚠️  WARNING: This will DELETE:" -ForegroundColor Yellow
Write-Host "  • All warehouse data (Bronze, Silver, Gold)" -ForegroundColor White
Write-Host "  • Hive Metastore database (Derby)" -ForegroundColor White
Write-Host "  • Staging zone Avro files" -ForegroundColor White
Write-Host "  • Spark event logs" -ForegroundColor White
if (-not $KeepPostgres) {
    Write-Host "  • PostgreSQL banking_source database" -ForegroundColor White
}
if (-not $KeepDocker) {
    Write-Host "  • Docker containers (Schema Registry)" -ForegroundColor White
}
Write-Host ""

# Confirmation prompt
$confirmation = Read-Host "Type 'DELETE' to confirm"
if ($confirmation -ne "DELETE") {
    Write-Host "Cleanup cancelled." -ForegroundColor Green
    exit 0
}

Write-Host ""
Write-Host "🧹 Starting cleanup process..." -ForegroundColor Cyan

Set-Location $ProjectRoot

# ============================================================================
# STEP 1: Stop Docker Containers
# ============================================================================

if (-not $KeepDocker) {
    Write-Host ""
    Write-Host "📦 Stopping Docker containers..." -ForegroundColor Cyan

    try {
        Set-Location "$ProjectRoot\nifi"
        docker-compose down -v
        Write-Host "✅ Docker containers stopped and volumes removed" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  Failed to stop Docker containers: $_" -ForegroundColor Yellow
    }

    Set-Location $ProjectRoot
}

# ============================================================================
# STEP 2: Drop PostgreSQL Database
# ============================================================================

if (-not $KeepPostgres) {
    Write-Host ""
    Write-Host "🗄️  Dropping PostgreSQL database..." -ForegroundColor Cyan

    $env:PGPASSWORD = "postgres"

    try {
        # Terminate active connections
        $terminateQuery = @"
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'banking_source'
  AND pid <> pg_backend_pid();
"@

        psql -U postgres -c $terminateQuery 2>$null | Out-Null

        # Drop database
        psql -U postgres -c "DROP DATABASE IF EXISTS banking_source;" 2>$null

        Write-Host "✅ PostgreSQL database dropped" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  Failed to drop PostgreSQL database: $_" -ForegroundColor Yellow
    }
}

# ============================================================================
# STEP 3: Delete Warehouse Data
# ============================================================================

Write-Host ""
Write-Host "🗂️  Deleting warehouse data..." -ForegroundColor Cyan

$warehousePath = Join-Path $ProjectRoot "warehouse"

if (Test-Path $warehousePath) {
    try {
        # Count files before deletion
        $fileCount = (Get-ChildItem -Path $warehousePath -Recurse -File).Count

        Remove-Item -Path $warehousePath -Recurse -Force

        Write-Host "✅ Deleted $fileCount files from warehouse/" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  Failed to delete warehouse: $_" -ForegroundColor Yellow
    }
} else {
    Write-Host "ℹ️  Warehouse directory not found" -ForegroundColor Gray
}

# ============================================================================
# STEP 4: Delete Hive Metastore
# ============================================================================

Write-Host ""
Write-Host "🗄️  Deleting Hive Metastore..." -ForegroundColor Cyan

$metastorePath = Join-Path $ProjectRoot "metastore_db"

if (Test-Path $metastorePath) {
    try {
        # Remove Derby lock files first
        Get-ChildItem -Path $metastorePath -Filter "*.lck" | Remove-Item -Force 2>$null

        Remove-Item -Path $metastorePath -Recurse -Force

        Write-Host "✅ Hive Metastore deleted" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  Failed to delete Metastore: $_" -ForegroundColor Yellow
    }
} else {
    Write-Host "ℹ️  Metastore directory not found" -ForegroundColor Gray
}

# Delete derby.log if exists
$derbyLog = Join-Path $ProjectRoot "derby.log"
if (Test-Path $derbyLog) {
    Remove-Item -Path $derbyLog -Force
}

# ============================================================================
# STEP 5: Delete Spark Logs
# ============================================================================

Write-Host ""
Write-Host "📋 Deleting Spark event logs..." -ForegroundColor Cyan

$logsPath = Join-Path $ProjectRoot "logs"

if (Test-Path $logsPath) {
    try {
        $logCount = (Get-ChildItem -Path $logsPath -Recurse -File).Count

        Remove-Item -Path $logsPath -Recurse -Force

        Write-Host "✅ Deleted $logCount log files" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  Failed to delete logs: $_" -ForegroundColor Yellow
    }
} else {
    Write-Host "ℹ️  Logs directory not found" -ForegroundColor Gray
}

# ============================================================================
# STEP 6: Delete Temporary Files
# ============================================================================

Write-Host ""
Write-Host "🗑️  Deleting temporary files..." -ForegroundColor Cyan

$tempPath = Join-Path $ProjectRoot "tmp"

if (Test-Path $tempPath) {
    Remove-Item -Path $tempPath -Recurse -Force
    Write-Host "✅ Temporary files deleted" -ForegroundColor Green
}

# Delete SBT target directories (optional - saves space)
Write-Host ""
$cleanTarget = Read-Host "Delete SBT compiled classes? (y/n)"
if ($cleanTarget -eq "y") {
    Write-Host "🔨 Running SBT clean..." -ForegroundColor Cyan
    sbt clean
    Write-Host "✅ SBT artifacts cleaned" -ForegroundColor Green
}

# ============================================================================
# STEP 7: Verify Cleanup
# ============================================================================

Write-Host ""
Write-Host "🔍 Verifying cleanup..." -ForegroundColor Cyan

$cleanupReport = @{
    "Warehouse" = -not (Test-Path (Join-Path $ProjectRoot "warehouse"))
    "Metastore" = -not (Test-Path (Join-Path $ProjectRoot "metastore_db"))
    "Logs" = -not (Test-Path (Join-Path $ProjectRoot "logs"))
    "Temp" = -not (Test-Path (Join-Path $ProjectRoot "tmp"))
}

Write-Host ""
foreach ($item in $cleanupReport.GetEnumerator()) {
    if ($item.Value) {
        Write-Host "✅ $($item.Key): Cleaned" -ForegroundColor Green
    } else {
        Write-Host "⚠️  $($item.Key): Still exists" -ForegroundColor Yellow
    }
}

# ============================================================================
# SUMMARY
# ============================================================================

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║               CLEANUP COMPLETED                                ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""

Write-Host "✅ Environment reset to pristine state" -ForegroundColor Green
Write-Host ""

Write-Host "📋 NEXT STEPS TO REBUILD:" -ForegroundColor Yellow
Write-Host "1. Run setup script:" -ForegroundColor White
Write-Host "   .\scripts\windows\01-setup.ps1" -ForegroundColor Gray
Write-Host ""
Write-Host "2. Or manually:" -ForegroundColor White
Write-Host "   a. Start Schema Registry: docker-compose -f nifi\docker-compose.yml up -d" -ForegroundColor Gray
Write-Host "   b. Create database: psql -U postgres -f source-system\sql\02_create_tables.sql" -ForegroundColor Gray
Write-Host "   c. Seed data: sbt seedRef && sbt seedTxn" -ForegroundColor Gray
Write-Host "   d. Initialize warehouse: sbt initEnv" -ForegroundColor Gray
Write-Host "   e. Create Raw Vault: sbt `"runMain bronze.RawVaultSchema`"" -ForegroundColor Gray
Write-Host ""

# ============================================================================
# DISK SPACE RECOVERED
# ============================================================================

Write-Host "💾 Disk Space Information:" -ForegroundColor Cyan

$drive = (Get-Item $ProjectRoot).PSDrive
$driveInfo = Get-PSDrive $drive.Name

Write-Host "  Free space on $($drive.Name): $([math]::Round($driveInfo.Free / 1GB, 2)) GB" -ForegroundColor White
Write-Host ""

Write-Host "Press any key to exit..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

###################################################################################
# TROUBLESHOOTING
###################################################################################

<#
# If cleanup fails with "file in use" errors:

1. Close all applications accessing the files:
   - IntelliJ IDEA / VS Code
   - Command prompts / PowerShell windows
   - SBT shells

2. Stop services:
   Get-Service | Where-Object {$_.DisplayName -like "*postgres*"} | Stop-Service
   docker stop $(docker ps -aq)

3. Kill Java processes:
   Get-Process java | Stop-Process -Force

4. Retry cleanup:
   .\scripts\windows\05-cleanup.ps1

# If PostgreSQL database won't drop:

psql -U postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'banking_source';"
psql -U postgres -c "DROP DATABASE banking_source;"

# If Docker containers won't stop:

docker-compose -f nifi\docker-compose.yml down -v --remove-orphans
docker system prune -af

#>

