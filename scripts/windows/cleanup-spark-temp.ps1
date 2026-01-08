# Cleanup Spark Temp Directory
# Purpose: Remove Spark temporary files after job completion

param(
    [int]$DaysToKeep = 1,
    [string]$SparkTempDir = "C:\spark-temp"
)

Write-Host ""
Write-Host "Cleaning Spark temporary files..." -ForegroundColor Cyan
Write-Host "Directory: $SparkTempDir" -ForegroundColor Gray
Write-Host "Retention: Keep files newer than $DaysToKeep days" -ForegroundColor Gray
Write-Host ""

if (-not (Test-Path $SparkTempDir)) {
    Write-Host "Spark temp directory not found: $SparkTempDir" -ForegroundColor Yellow
    Write-Host "Creating directory..." -ForegroundColor Gray
    New-Item -ItemType Directory -Force -Path $SparkTempDir | Out-Null
    Write-Host "Directory created" -ForegroundColor Green
    exit 0
}

try {
    $cutoffDate = (Get-Date).AddDays(-$DaysToKeep)
    $oldDirs = Get-ChildItem -Path $SparkTempDir -Directory | Where-Object { $_.CreationTime -lt $cutoffDate }

    if ($oldDirs.Count -eq 0) {
        Write-Host "No temporary directories older than $DaysToKeep days" -ForegroundColor Green

        $allDirs = Get-ChildItem -Path $SparkTempDir -Directory
        if ($allDirs.Count -gt 0) {
            Write-Host "Current: $($allDirs.Count) directories" -ForegroundColor Gray
        }
        exit 0
    }

    Write-Host "Found $($oldDirs.Count) directories to remove" -ForegroundColor Yellow
    Write-Host ""

    $deletedCount = 0
    $skippedCount = 0

    foreach ($dir in $oldDirs) {
        try {
            Remove-Item -Path $dir.FullName -Recurse -Force -ErrorAction Stop
            $deletedCount++
            Write-Host "Deleted: $($dir.Name)" -ForegroundColor Green
        } catch {
            $skippedCount++
            Write-Host "Skipped: $($dir.Name) - files in use" -ForegroundColor Yellow
        }
    }

    Write-Host ""
    Write-Host "Cleanup complete: Deleted $deletedCount directories" -ForegroundColor Green

    if ($skippedCount -gt 0) {
        Write-Host "Skipped $skippedCount directories - will clean next time" -ForegroundColor Yellow
    }

    $remaining = (Get-ChildItem -Path $SparkTempDir -Directory).Count
    Write-Host "Remaining: $remaining directories" -ForegroundColor Gray
    Write-Host ""

} catch {
    Write-Host ""
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

