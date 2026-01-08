# ============================================================================
# Run ETL Job with Automatic Cleanup
# ============================================================================
# Purpose: Execute spark-submit job and automatically clean up temp files
# Usage:
#   .\scripts\windows\run-etl-with-cleanup.ps1 -Job bronze.RawVaultETL -Mode full
#   .\scripts\windows\run-etl-with-cleanup.ps1 -Job silver.BusinessVaultETL -Args "--build-pit"
# ============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$Job,  # Fully qualified class name (e.g., bronze.RawVaultETL)

    [string]$JarPath = "target/scala-2.12/data-vault-modeling-etl-1.0.0.jar",

    [string]$Master = "local[*]",

    [string]$Args = "--mode full",  # Default arguments

    [switch]$SkipCleanup  # Skip cleanup if you want to debug temp files
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  ETL JOB EXECUTION WITH AUTO-CLEANUP" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Job:        $Job" -ForegroundColor White
Write-Host "JAR:        $JarPath" -ForegroundColor White
Write-Host "Master:     $Master" -ForegroundColor White
Write-Host "Arguments:  $Args" -ForegroundColor White
Write-Host "Cleanup:    $(if ($SkipCleanup) { 'Disabled' } else { 'Enabled' })" -ForegroundColor White
Write-Host ""

# Check JAR exists
if (-not (Test-Path $JarPath)) {
    Write-Host "❌ JAR file not found: $JarPath" -ForegroundColor Red
    Write-Host "   Run: sbt clean assembly" -ForegroundColor Yellow
    exit 1
}

# Check spark-submit is available
try {
    $sparkVersion = spark-submit --version 2>&1 | Select-String "version" | Select-Object -First 1
    Write-Host "✅ Spark found: $sparkVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ spark-submit not found in PATH" -ForegroundColor Red
    Write-Host "   Ensure SPARK_HOME is set and %SPARK_HOME%\bin is in PATH" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "─────────────────────────────────────────" -ForegroundColor Gray
Write-Host "🚀 Starting Spark job..." -ForegroundColor Cyan
Write-Host "─────────────────────────────────────────" -ForegroundColor Gray
Write-Host ""

# Build spark-submit command
$sparkCmd = "spark-submit --class $Job --master $Master $JarPath $Args"

# Record start time
$startTime = Get-Date

# Execute spark-submit
try {
    Invoke-Expression $sparkCmd
    $exitCode = $LASTEXITCODE

    $endTime = Get-Date
    $duration = $endTime - $startTime

    Write-Host ""
    Write-Host "─────────────────────────────────────────" -ForegroundColor Gray

    if ($exitCode -eq 0) {
        Write-Host "✅ Job completed successfully" -ForegroundColor Green
        Write-Host "   Duration: $($duration.ToString('hh\:mm\:ss'))" -ForegroundColor Gray
    } else {
        Write-Host "❌ Job failed with exit code: $exitCode" -ForegroundColor Red
        Write-Host "   Duration: $($duration.ToString('hh\:mm\:ss'))" -ForegroundColor Gray

        # Still cleanup on failure unless explicitly disabled
        if (-not $SkipCleanup) {
            Write-Host ""
            Write-Host "⚠️  Cleanup will still run to free disk space..." -ForegroundColor Yellow
        }
    }

} catch {
    Write-Host ""
    Write-Host "❌ Error executing spark-submit: $($_.Exception.Message)" -ForegroundColor Red
    $exitCode = 1
}

# Run cleanup script unless disabled
if (-not $SkipCleanup) {
    Write-Host ""
    Write-Host "─────────────────────────────────────────" -ForegroundColor Gray
    Write-Host "🧹 Running automatic cleanup..." -ForegroundColor Cyan
    Write-Host "─────────────────────────────────────────" -ForegroundColor Gray

    $cleanupScript = Join-Path $PSScriptRoot "cleanup-spark-temp.ps1"

    if (Test-Path $cleanupScript) {
        try {
            # Wait a moment for JVM to release file handles
            Start-Sleep -Seconds 2

            & $cleanupScript

            Write-Host ""
            Write-Host "✅ Cleanup completed" -ForegroundColor Green

        } catch {
            Write-Host "⚠️  Cleanup failed: $($_.Exception.Message)" -ForegroundColor Yellow
            Write-Host "   You can run cleanup manually later: .\scripts\windows\cleanup-spark-temp.ps1" -ForegroundColor Gray
        }
    } else {
        Write-Host "⚠️  Cleanup script not found: $cleanupScript" -ForegroundColor Yellow
    }
} else {
    Write-Host ""
    Write-Host "⏭️  Cleanup skipped (--SkipCleanup flag)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  EXECUTION FINISHED" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

exit $exitCode

