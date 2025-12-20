###################################################################################
# ETL Pipeline Runner (Windows PowerShell)
###################################################################################
#
# PURPOSE:
# Execute Data Vault ETL pipelines across all layers (Bronze, Silver, Gold).
#
# USAGE:
#   .\scripts\windows\02-run-etl.ps1 -Layer bronze
#   .\scripts\windows\02-run-etl.ps1 -Layer silver
#   .\scripts\windows\02-run-etl.ps1 -Layer gold
#   .\scripts\windows\02-run-etl.ps1 -Layer all
#   .\scripts\windows\02-run-etl.ps1 -Layer bronze -Entity customer
#   .\scripts\windows\02-run-etl.ps1 -Layer bronze -Mode full
#
# PARAMETERS:
#   -Layer: bronze|silver|gold|semantic|all
#   -Mode: full|incremental (default: incremental)
#   -Entity: customer|account|transaction (for bronze layer only)
#
###################################################################################

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("bronze", "silver", "gold", "semantic", "all")]
    [string]$Layer,

    [ValidateSet("full", "incremental")]
    [string]$Mode = "incremental",

    [ValidateSet("customer", "account", "transaction", "all")]
    [string]$Entity = "all",

    [string]$ProjectRoot = "C:\dev\projects\data-vault-modeling-etl"
)

$ErrorActionPreference = "Stop"

# Change to project root
Set-Location $ProjectRoot

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

function Write-Header {
    param([string]$Message)
    Write-Host ""
    Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
    Write-Host "║  $Message" -ForegroundColor Cyan
    Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step {
    param([string]$Message)
    Write-Host "▶ $Message" -ForegroundColor Yellow
}

function Write-Success {
    param([string]$Message)
    Write-Host "✅ $Message" -ForegroundColor Green
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "❌ $Message" -ForegroundColor Red
}

function Get-Timestamp {
    return Get-Date -Format "yyyy-MM-dd HH:mm:ss"
}

# ============================================================================
# PRE-FLIGHT CHECKS
# ============================================================================

Write-Header "DATA VAULT ETL PIPELINE RUNNER"

Write-Host "Configuration:" -ForegroundColor Cyan
Write-Host "  Layer: $Layer" -ForegroundColor White
Write-Host "  Mode: $Mode" -ForegroundColor White
Write-Host "  Entity: $Entity" -ForegroundColor White
Write-Host "  Start Time: $(Get-Timestamp)" -ForegroundColor White
Write-Host ""

# Check SBT is available
Write-Step "Checking prerequisites..."
try {
    sbt about 2>&1 | Out-Null
    Write-Success "SBT is available"
} catch {
    Write-Error-Custom "SBT not found. Please install SBT and add to PATH."
    exit 1
}

# ============================================================================
# RUN BRONZE LAYER (RAW VAULT)
# ============================================================================

function Run-BronzeLayer {
    Write-Header "BRONZE LAYER (RAW VAULT)"

    $startTime = Get-Date

    if ($Entity -eq "all") {
        Write-Step "Processing all entities..."
        $command = "sbt `"runMain bronze.RawVaultETL --mode $Mode`""
    } else {
        Write-Step "Processing entity: $Entity"
        $command = "sbt `"runMain bronze.RawVaultETL --mode $Mode --entity $Entity`""
    }

    Write-Host "Executing: $command" -ForegroundColor Gray
    Write-Host ""

    Invoke-Expression $command

    if ($LASTEXITCODE -eq 0) {
        $duration = (Get-Date) - $startTime
        Write-Success "Bronze Layer completed in $($duration.TotalSeconds) seconds"

        # Show load statistics
        Write-Step "Fetching load statistics..."
        sbt "runMain bronze.utils.LoadMetadata.showLoadStatistics"
    } else {
        Write-Error-Custom "Bronze Layer failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

# ============================================================================
# RUN SILVER LAYER (BUSINESS VAULT)
# ============================================================================

function Run-SilverLayer {
    Write-Header "SILVER LAYER (BUSINESS VAULT)"

    $startTime = Get-Date

    Write-Step "Building PIT tables..."
    $command = "sbt `"runMain silver.BusinessVaultETL --build-pit`""

    Write-Host "Executing: $command" -ForegroundColor Gray
    Write-Host ""

    Invoke-Expression $command

    if ($LASTEXITCODE -eq 0) {
        $duration = (Get-Date) - $startTime
        Write-Success "Silver Layer completed in $($duration.TotalSeconds) seconds"
    } else {
        Write-Error-Custom "Silver Layer failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

# ============================================================================
# RUN GOLD LAYER (DIMENSIONAL MODEL)
# ============================================================================

function Run-GoldLayer {
    Write-Header "GOLD LAYER (DIMENSIONAL MODEL)"

    $startTime = Get-Date

    Write-Step "Building dimensions and facts..."
    $command = "sbt `"runMain gold.DimensionalModelETL --rebuild-all`""

    Write-Host "Executing: $command" -ForegroundColor Gray
    Write-Host ""

    Invoke-Expression $command

    if ($LASTEXITCODE -eq 0) {
        $duration = (Get-Date) - $startTime
        Write-Success "Gold Layer completed in $($duration.TotalSeconds) seconds"
    } else {
        Write-Error-Custom "Gold Layer failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

# ============================================================================
# RUN SEMANTIC LAYER (BUSINESS VIEWS)
# ============================================================================

function Run-SemanticLayer {
    Write-Header "SEMANTIC LAYER (BUSINESS VIEWS)"

    $startTime = Get-Date

    Write-Step "Creating semantic views..."
    $command = "sbt `"runMain semantic.SemanticModel`""

    Write-Host "Executing: $command" -ForegroundColor Gray
    Write-Host ""

    Invoke-Expression $command

    if ($LASTEXITCODE -eq 0) {
        $duration = (Get-Date) - $startTime
        Write-Success "Semantic Layer completed in $($duration.TotalSeconds) seconds"

        # List available queries
        Write-Step "Available queries:"
        sbt "runMain semantic.QueryInterface --list"
    } else {
        Write-Error-Custom "Semantic Layer failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

$overallStart = Get-Date

try {
    switch ($Layer) {
        "bronze" {
            Run-BronzeLayer
        }
        "silver" {
            Run-SilverLayer
        }
        "gold" {
            Run-GoldLayer
        }
        "semantic" {
            Run-SemanticLayer
        }
        "all" {
            Write-Header "RUNNING COMPLETE ETL PIPELINE"

            Run-BronzeLayer
            Write-Host ""

            Run-SilverLayer
            Write-Host ""

            Run-GoldLayer
            Write-Host ""

            Run-SemanticLayer
        }
    }

    $overallDuration = (Get-Date) - $overallStart

    Write-Host ""
    Write-Header "ETL PIPELINE COMPLETED SUCCESSFULLY"
    Write-Host "Total Duration: $($overallDuration.TotalMinutes) minutes" -ForegroundColor Green
    Write-Host "End Time: $(Get-Timestamp)" -ForegroundColor White
    Write-Host ""

    # Show summary statistics
    Write-Step "Pipeline Summary:"

    # Check if tables exist and show row counts
    Write-Host ""
    Write-Host "Bronze Layer Tables:" -ForegroundColor Cyan
    sbt "runMain bronze.utils.IcebergWriter.showTableStats bronze hub_customer" 2>$null

    Write-Host ""
    Write-Host "For detailed load history, run:" -ForegroundColor Yellow
    Write-Host "  sbt `"runMain bronze.utils.LoadMetadata.showLoadHistory`"" -ForegroundColor White
    Write-Host ""

    exit 0

} catch {
    $overallDuration = (Get-Date) - $overallStart

    Write-Host ""
    Write-Error-Custom "ETL PIPELINE FAILED"
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host "Duration before failure: $($overallDuration.TotalMinutes) minutes" -ForegroundColor Red
    Write-Host ""

    Write-Host "TROUBLESHOOTING:" -ForegroundColor Yellow
    Write-Host "1. Check logs in: logs\spark-events\" -ForegroundColor White
    Write-Host "2. Verify source data: psql -U postgres -d banking_source" -ForegroundColor White
    Write-Host "3. Check Hive Metastore: verify metastore_db\ exists" -ForegroundColor White
    Write-Host "4. Review load metadata: sbt `"runMain bronze.utils.LoadMetadata.showLoadHistory`"" -ForegroundColor White
    Write-Host ""

    exit 1
}

###################################################################################
# USAGE EXAMPLES
###################################################################################

<#
# Run Bronze layer only (incremental)
.\scripts\windows\02-run-etl.ps1 -Layer bronze

# Run Bronze layer full load
.\scripts\windows\02-run-etl.ps1 -Layer bronze -Mode full

# Run Bronze layer for specific entity
.\scripts\windows\02-run-etl.ps1 -Layer bronze -Entity customer

# Run all layers (complete pipeline)
.\scripts\windows\02-run-etl.ps1 -Layer all

# Run Silver layer only
.\scripts\windows\02-run-etl.ps1 -Layer silver

# Run Gold layer only
.\scripts\windows\02-run-etl.ps1 -Layer gold

# SCHEDULING:
# You can schedule this script with Windows Task Scheduler:
# 1. Open Task Scheduler
# 2. Create Basic Task
# 3. Action: Start a Program
# 4. Program: powershell.exe
# 5. Arguments: -File "C:\dev\projects\data-vault-modeling-etl\scripts\windows\02-run-etl.ps1" -Layer all
# 6. Set schedule (e.g., daily at 2 AM)
#>

