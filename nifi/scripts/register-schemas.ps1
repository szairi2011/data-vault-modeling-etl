###################################################################################
# Register Avro Schemas to Confluent Schema Registry (PowerShell)
###################################################################################
#
# PURPOSE:
# Batch register all Avro schemas to Schema Registry for NiFi CDC pipeline.
#
# USAGE:
#   .\nifi\scripts\register-schemas.ps1
#
# PREREQUISITES:
#   - Schema Registry running at http://localhost:8081
#   - Avro schema files in nifi/schemas/*.avsc
#
# WHAT THIS SCRIPT DOES:
#   1. Verifies Schema Registry is accessible
#   2. Reads each .avsc file
#   3. Registers schema as new version
#   4. Verifies registration success
#
###################################################################################

param(
    [string]$RegistryUrl = "http://localhost:8081",
    [string]$SchemasDir = "$PSScriptRoot\..\schemas"
)

$ErrorActionPreference = "Stop"

Write-Host "🚀 Registering Avro schemas to Schema Registry..." -ForegroundColor Green
Write-Host "Registry URL: $RegistryUrl" -ForegroundColor Gray
Write-Host "Schemas Directory: $SchemasDir" -ForegroundColor Gray
Write-Host ""

# ============================================================================
# FUNCTION: Test Schema Registry Connection
# ============================================================================

function Test-SchemaRegistry {
    param([string]$Url)

    try {
        $response = Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 5
        return $true
    }
    catch {
        Write-Host "❌ Schema Registry not accessible at $Url" -ForegroundColor Red
        Write-Host "   Error: $_" -ForegroundColor Red
        Write-Host "" -ForegroundColor Red
        Write-Host "TROUBLESHOOTING:" -ForegroundColor Yellow
        Write-Host "1. Start Schema Registry: docker-compose -f nifi\docker-compose.yml up -d" -ForegroundColor White
        Write-Host "2. Wait 30 seconds for startup" -ForegroundColor White
        Write-Host "3. Verify: curl http://localhost:8081/" -ForegroundColor White
        return $false
    }
}

# ============================================================================
# FUNCTION: Register Single Schema
# ============================================================================

function Register-AvroSchema {
    param(
        [string]$SchemaFile,
        [string]$Subject,
        [string]$RegistryUrl
    )

    Write-Host "📝 Registering $Subject..." -ForegroundColor Cyan

    # Read schema file
    $schemaPath = Join-Path $SchemasDir $SchemaFile

    if (-not (Test-Path $schemaPath)) {
        Write-Host "   ❌ Schema file not found: $schemaPath" -ForegroundColor Red
        return $false
    }

    $schemaContent = Get-Content $schemaPath -Raw

    # Create JSON payload for Schema Registry
    # Schema Registry expects: {"schema": "<escaped-json-string>"}
    $schemaEscaped = $schemaContent -replace '\\', '\\' -replace '"', '\"' -replace "`r`n", "\n" -replace "`n", "\n"

    $payload = @{
        schema = $schemaContent
    } | ConvertTo-Json -Depth 10

    # Register schema
    try {
        $response = Invoke-RestMethod -Method Post `
            -Uri "$RegistryUrl/subjects/$Subject/versions" `
            -ContentType "application/vnd.schemaregistry.v1+json" `
            -Body $payload

        Write-Host "   ✅ Registered as version $($response.id)" -ForegroundColor Green
        return $true
    }
    catch {
        $errorDetail = $_.Exception.Response
        Write-Host "   ❌ Failed to register schema" -ForegroundColor Red
        Write-Host "   Error: $_" -ForegroundColor Red

        # Try to get detailed error from response
        if ($errorDetail) {
            $reader = New-Object System.IO.StreamReader($errorDetail.GetResponseStream())
            $responseBody = $reader.ReadToEnd()
            Write-Host "   Response: $responseBody" -ForegroundColor Red
        }

        return $false
    }
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

# Step 1: Verify Schema Registry
Write-Host "🔍 Checking Schema Registry connectivity..." -ForegroundColor Cyan
if (-not (Test-SchemaRegistry -Url $RegistryUrl)) {
    exit 1
}
Write-Host "   ✅ Schema Registry is accessible" -ForegroundColor Green
Write-Host ""

# Step 2: Register all schemas
$schemas = @(
    @{File = "customer.avsc"; Subject = "banking.customer-value"},
    @{File = "account.avsc"; Subject = "banking.account-value"},
    @{File = "transaction_header.avsc"; Subject = "banking.transaction_header-value"},
    @{File = "transaction_item.avsc"; Subject = "banking.transaction_item-value"}
)

$successCount = 0
$failCount = 0

foreach ($schema in $schemas) {
    $result = Register-AvroSchema -SchemaFile $schema.File -Subject $schema.Subject -RegistryUrl $RegistryUrl
    if ($result) {
        $successCount++
    } else {
        $failCount++
    }
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "REGISTRATION SUMMARY" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "✅ Successful: $successCount" -ForegroundColor Green
Write-Host "❌ Failed: $failCount" -ForegroundColor $(if ($failCount -gt 0) { "Red" } else { "Gray" })

# Step 3: Verify registered subjects
Write-Host ""
Write-Host "🔍 Verifying registered subjects..." -ForegroundColor Cyan

try {
    $subjects = Invoke-RestMethod -Uri "$RegistryUrl/subjects"
    Write-Host "   Registered subjects:" -ForegroundColor Gray
    $subjects | ForEach-Object {
        Write-Host "     - $_" -ForegroundColor White
    }
}
catch {
    Write-Host "   ⚠️  Could not retrieve subjects list" -ForegroundColor Yellow
}

# Step 4: Show schema details
Write-Host ""
Write-Host "📊 Schema Details:" -ForegroundColor Cyan

foreach ($schema in $schemas) {
    try {
        $url = "$RegistryUrl/subjects/$($schema.Subject)/versions/latest"
        $details = Invoke-RestMethod -Uri $url
        Write-Host "   $($schema.Subject):" -ForegroundColor White
        Write-Host "     Version: $($details.version)" -ForegroundColor Gray
        Write-Host "     ID: $($details.id)" -ForegroundColor Gray
    }
    catch {
        Write-Host "   $($schema.Subject): Not found" -ForegroundColor Red
    }
}

Write-Host ""
if ($failCount -eq 0) {
    Write-Host "🎉 All schemas registered successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "NEXT STEPS:" -ForegroundColor Yellow
    Write-Host "1. Open NiFi UI: http://localhost:8080/nifi" -ForegroundColor White
    Write-Host "2. Import template: nifi\templates\01_source_to_staging.xml" -ForegroundColor White
    Write-Host "3. Configure AvroSchemaRegistry controller service:" -ForegroundColor White
    Write-Host "   - URL: $RegistryUrl" -ForegroundColor Gray
    Write-Host "4. Start NiFi processors" -ForegroundColor White
    Write-Host "5. Verify Avro files in: warehouse\staging\" -ForegroundColor White
    exit 0
} else {
    Write-Host "❌ Some schemas failed to register. Check errors above." -ForegroundColor Red
    exit 1
}

