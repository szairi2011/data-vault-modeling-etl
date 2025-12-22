# ============================================================================
# Validate NiFi Local Schema Configuration
# ============================================================================
#
# PURPOSE:
# Validate that Avro schemas are accessible and properly configured for NiFi.
#
# USAGE:
#   .\nifi\scripts\validate-nifi-schemas.ps1
# ============================================================================

Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  NiFi Schema Validation" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

$SchemaDir = "$PSScriptRoot\..\schemas"

# Check if schema directory exists
if (-not (Test-Path $SchemaDir)) {
    Write-Host "❌ Schema directory not found: $SchemaDir" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Schema directory found: $SchemaDir" -ForegroundColor Green
Write-Host ""

# Get all .avsc files
$schemas = Get-ChildItem -Path $SchemaDir -Filter "*.avsc"

if ($schemas.Count -eq 0) {
    Write-Host "❌ No .avsc files found in schema directory" -ForegroundColor Red
    exit 1
}

Write-Host "Found $($schemas.Count) Avro schema(s):" -ForegroundColor Yellow
Write-Host ""

$allValid = $true

foreach ($schema in $schemas) {
    Write-Host "Validating: $($schema.Name)" -ForegroundColor Cyan

    # Check file is readable
    try {
        $content = Get-Content $schema.FullName -Raw -ErrorAction Stop
        Write-Host "  File is readable" -ForegroundColor Green
    } catch {
        Write-Host "  Cannot read file: $_" -ForegroundColor Red
        $allValid = $false
        continue
    }

    # Validate JSON syntax
    try {
        $jsonObject = $content | ConvertFrom-Json -ErrorAction Stop
        Write-Host "  Valid JSON syntax" -ForegroundColor Green
    } catch {
        Write-Host "  Invalid JSON: $_" -ForegroundColor Red
        $allValid = $false
        continue
    }

    # Check Avro schema structure
    if ($jsonObject.type -eq "record") {
        Write-Host "  Type: record" -ForegroundColor Green
    } else {
        Write-Host "  Type is not 'record': $($jsonObject.type)" -ForegroundColor Yellow
    }

    if ($jsonObject.name) {
        Write-Host "  Name: $($jsonObject.name)" -ForegroundColor Green
    } else {
        Write-Host "  Missing 'name' field" -ForegroundColor Red
        $allValid = $false
    }

    if ($jsonObject.fields) {
        Write-Host "  Fields: $($jsonObject.fields.Count) field(s)" -ForegroundColor Green
    } else {
        Write-Host "  Missing 'fields' array" -ForegroundColor Red
        $allValid = $false
    }

    # Display full path for NiFi
    Write-Host "  [Full path]: $($schema.FullName)" -ForegroundColor White
    Write-Host ""
}

# Summary
Write-Host "============================================================================" -ForegroundColor Cyan
if ($allValid) {
    Write-Host "All schemas are valid!" -ForegroundColor Green
} else {
    Write-Host "Some schemas have issues - see details above" -ForegroundColor Yellow
}
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

# NiFi check (optional)
Write-Host "Checking NiFi..." -ForegroundColor Yellow
try {
    $nifiResponse = Invoke-WebRequest -Uri "https://localhost:8443/nifi" -Method Head -TimeoutSec 5 -ErrorAction Stop
    Write-Host "NiFi is running at https://localhost:8443/nifi" -ForegroundColor Green
} catch {
    Write-Host "NiFi not reachable at https://localhost:8443/nifi" -ForegroundColor Yellow
    Write-Host "   Make sure NiFi is started before configuring processors" -ForegroundColor Yellow
}
Write-Host ""

# Next steps
if ($allValid) {
    Write-Host "Next Steps:" -ForegroundColor Cyan
    Write-Host "  1. Open NiFi UI: https://localhost:8443/nifi" -ForegroundColor White
    Write-Host "  2. Add AvroSchemaRegistry Controller Service" -ForegroundColor White
    Write-Host "  3. Configure processors to use schema paths above" -ForegroundColor White
    Write-Host ""
}

exit 0
