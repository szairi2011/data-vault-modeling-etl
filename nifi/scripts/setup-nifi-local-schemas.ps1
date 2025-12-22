# ============================================================================
# Setup NiFi Local Schemas (No Docker Required)
# ============================================================================
#
# PURPOSE:
# Configure local Avro schema files for NiFi processors without requiring
# Confluent Schema Registry or Docker containers.
#
# USAGE:
#   .\nifi\scripts\setup-nifi-local-schemas.ps1
#
# WHAT THIS DOES:
# 1. Creates a centralized schema directory (optional)
# 2. Copies Avro schemas to a NiFi-accessible location
# 3. Displays configuration instructions for NiFi processors
# ============================================================================

param(
    [string]$NiFiHome = "C:\Users\sofiane\.nifi-2.7.2",
    [string]$SchemaSource = "$PSScriptRoot\..\schemas",
    [switch]$CopyToNiFi = $false
)

Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  NiFi Local Schema Setup (No Docker Required)" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

# Resolve absolute path to schemas
$SchemaSourcePath = Resolve-Path $SchemaSource -ErrorAction SilentlyContinue
if (-not $SchemaSourcePath) {
    Write-Host "❌ Schema source directory not found: $SchemaSource" -ForegroundColor Red
    exit 1
}

Write-Host "📁 Schema source directory: $SchemaSourcePath" -ForegroundColor Green
Write-Host ""

# List available schemas
$schemas = Get-ChildItem -Path $SchemaSourcePath -Filter "*.avsc"
Write-Host "📋 Available Avro Schemas:" -ForegroundColor Yellow
foreach ($schema in $schemas) {
    Write-Host "   ✓ $($schema.Name)" -ForegroundColor White
}
Write-Host ""

# Option 1: Use schemas directly from project directory
Write-Host "══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  OPTION 1: Use Schemas Directly from Project (Recommended)" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "Configure NiFi processors to reference schemas directly:" -ForegroundColor Green
Write-Host ""

foreach ($schema in $schemas) {
    $schemaPath = $schema.FullName
    Write-Host "Schema: $($schema.BaseName)" -ForegroundColor Yellow
    Write-Host "  Path: $schemaPath" -ForegroundColor White
    Write-Host ""
}

Write-Host "In NiFi Processor Configuration:" -ForegroundColor Cyan
Write-Host "  1. Add AvroSchemaRegistry Controller Service" -ForegroundColor White
Write-Host "  2. In ConvertRecord/UpdateRecord processors:" -ForegroundColor White
Write-Host "     - Schema Registry: AvroSchemaRegistry" -ForegroundColor White
Write-Host "     - Schema Access Strategy: Use 'Schema File' Property" -ForegroundColor White
Write-Host "     - Schema File: [paste path from above]" -ForegroundColor White
Write-Host ""

# Option 2: Copy to NiFi directory
if ($CopyToNiFi) {
    Write-Host "══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  OPTION 2: Copy Schemas to NiFi Directory" -ForegroundColor Cyan
    Write-Host "══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""

    if (-not (Test-Path $NiFiHome)) {
        Write-Host "⚠️  NiFi home directory not found: $NiFiHome" -ForegroundColor Yellow
        Write-Host "   Specify correct path with -NiFiHome parameter" -ForegroundColor Yellow
        Write-Host ""
    } else {
        $nifiSchemaDir = Join-Path $NiFiHome "conf\schemas\banking"

        # Create directory
        if (-not (Test-Path $nifiSchemaDir)) {
            New-Item -ItemType Directory -Path $nifiSchemaDir -Force | Out-Null
            Write-Host "✅ Created NiFi schema directory: $nifiSchemaDir" -ForegroundColor Green
        }

        # Copy schemas
        foreach ($schema in $schemas) {
            Copy-Item $schema.FullName -Destination $nifiSchemaDir -Force
            Write-Host "   ✓ Copied $($schema.Name)" -ForegroundColor White
        }
        Write-Host ""
        Write-Host "📁 Schemas copied to: $nifiSchemaDir" -ForegroundColor Green
        Write-Host ""
        Write-Host "In NiFi Processor Configuration:" -ForegroundColor Cyan
        Write-Host "  Schema File: $nifiSchemaDir\<schema-name>.avsc" -ForegroundColor White
        Write-Host ""
    }
}

# Option 3: Schema Text
Write-Host "══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  OPTION 3: Use Schema Text (Inline)" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "For quick testing, paste schema JSON directly into NiFi:" -ForegroundColor Green
Write-Host "  1. In processor configuration" -ForegroundColor White
Write-Host "  2. Schema Access Strategy: Use 'Schema Text' Property" -ForegroundColor White
Write-Host "  3. Schema Text: [paste full JSON from .avsc file]" -ForegroundColor White
Write-Host ""

# Summary
Write-Host "══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Summary" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "✅ Local schemas are ready to use" -ForegroundColor Green
Write-Host "✅ No Docker or Schema Registry required" -ForegroundColor Green
Write-Host "✅ Schemas located at: $SchemaSourcePath" -ForegroundColor Green
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "  1. Start NiFi if not running" -ForegroundColor White
Write-Host "  2. Open NiFi UI: http://localhost:8080/nifi" -ForegroundColor White
Write-Host "  3. Configure processors with schema paths above" -ForegroundColor White
Write-Host "  4. Test Avro conversion with sample data" -ForegroundColor White
Write-Host ""

# Additional info
Write-Host "💡 Tips:" -ForegroundColor Cyan
Write-Host "  - Use absolute paths for schema files" -ForegroundColor White
Write-Host "  - Forward slashes work: C:/path/to/schema.avsc" -ForegroundColor White
Write-Host "  - Validate schemas at: https://json-schema-validator.herokuapp.com/avro.jsp" -ForegroundColor White
Write-Host ""

# Export paths to clipboard (optional)
if ($schemas.Count -gt 0) {
    $firstSchema = $schemas[0].FullName
    Write-Host "📋 Sample schema path copied to clipboard:" -ForegroundColor Yellow
    Write-Host "   $firstSchema" -ForegroundColor White
    Set-Clipboard -Value $firstSchema
    Write-Host ""
}

Write-Host "✅ Setup complete!" -ForegroundColor Green
Write-Host ""
