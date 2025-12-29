# ============================================================================
# RUN BRONZE ETL - Simple Execution Script
# ============================================================================
#
# This script runs the Bronze ETL to create tables and load data
#
# ============================================================================

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "          RUNNING BRONZE ETL (RAW VAULT)" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Executing: sbt 'runMain bronze.RawVaultETL --mode full'" -ForegroundColor Yellow
Write-Host ""
Write-Host "This will:" -ForegroundColor White
Write-Host "  1. Create all Bronze tables (Hubs, Links, Satellites)" -ForegroundColor Gray
Write-Host "  2. Read Avro files from warehouse/staging/" -ForegroundColor Gray
Write-Host "  3. Load data into Iceberg tables" -ForegroundColor Gray
Write-Host "  4. Track load metadata" -ForegroundColor Gray
Write-Host ""
Write-Host "Press any key to continue or Ctrl+C to cancel..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

Write-Host ""
Write-Host "Starting ETL..." -ForegroundColor Green
Write-Host ""

sbt "runMain bronze.RawVaultETL --mode full"

$exitCode = $LASTEXITCODE

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan

if ($exitCode -eq 0) {
    Write-Host "          ETL COMPLETED SUCCESSFULLY" -ForegroundColor Green
    Write-Host "================================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "NEXT: Run the status check to verify tables:" -ForegroundColor White
    Write-Host "  .\scripts\windows\check-bronze-status.ps1" -ForegroundColor Gray
} else {
    Write-Host "          ETL FAILED" -ForegroundColor Red
    Write-Host "================================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Check the error messages above." -ForegroundColor Yellow
    Write-Host "Exit code: $exitCode" -ForegroundColor Red
}

Write-Host ""

