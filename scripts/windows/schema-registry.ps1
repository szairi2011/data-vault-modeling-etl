param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("start", "stop", "status", "restart", "logs")]
    [string]$Action,
    [string]$ConfluentHome = "C:\dev\dev-tools\confluent-7.5.0"
)

$ErrorActionPreference = "Stop"

function Write-Header {
    param([string]$Message)
    Write-Host ""
    Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
    Write-Host "║  $Message" -ForegroundColor Cyan
    Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Success {
    param([string]$Message)
    Write-Host "✅ $Message" -ForegroundColor Green
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "❌ $Message" -ForegroundColor Red
}

function Get-SchemaRegistryProcess {
    return Get-Process | Where-Object {
        $_.ProcessName -eq "java" -and $_.CommandLine -like "*schema-registry*"
    }
}

function Test-SchemaRegistryRunning {
    try {
        Invoke-WebRequest -Uri "http://localhost:8081/" -UseBasicParsing -TimeoutSec 2 -ErrorAction SilentlyContinue | Out-Null
        return $true
    } catch {
        return $false
    }
}

function Start-SchemaRegistry {
    Write-Header "STARTING SCHEMA REGISTRY"

    if (Test-SchemaRegistryRunning) {
        Write-Host "   Already running" -ForegroundColor Yellow
        return
    }

    if (-not (Test-Path $ConfluentHome)) {
        Write-Error-Custom "Not found: $ConfluentHome"
        exit 1
    }

    # Force Java 11 - ignore PATH
    $env:JAVA_HOME = "C:\Program Files\Java\jdk-11"
    $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

    if (-not (Test-Path "$env:JAVA_HOME\bin\java.exe")) {
        Write-Error-Custom "Java 11 not found at: $env:JAVA_HOME"
        exit 1
    }

    Write-Host "   Using Java: $env:JAVA_HOME" -ForegroundColor Gray

    # Use ONLY standalone config (no Kafka dependency)
    $cfg = "$ConfluentHome\etc\schema-registry\schema-registry-standalone.properties"

    if (-not (Test-Path $cfg)) {
        Write-Error-Custom "Standalone config not found: $cfg"
        Write-Host ""
        Write-Host "Create the file with this content:" -ForegroundColor Yellow
        Write-Host "listeners=http://0.0.0.0:8081" -ForegroundColor White
        Write-Host "kafkastore.connection.url=" -ForegroundColor White
        Write-Host "schema.compatibility.level=BACKWARD" -ForegroundColor White
        exit 1
    }

    Write-Host "   Using config: $cfg" -ForegroundColor Gray
    Write-Host "🚀 Starting..." -ForegroundColor Cyan

    try {
        # Use Confluent's batch script if it exists, otherwise use Java directly
        $startScript = "$ConfluentHome\bin\schema-registry-start.bat"

        if (Test-Path $startScript) {
            Write-Host "   Using: $startScript" -ForegroundColor Gray
            $proc = Start-Process -FilePath $startScript -ArgumentList $cfg -WindowStyle Minimized -PassThru
        } else {
            # Start with Java directly - auto-discover all JARs
            Write-Host "   Starting with Java directly" -ForegroundColor Gray
            $javaExe = "$env:JAVA_HOME\bin\java.exe"

            # Auto-discover ALL JAR directories
            $javaDir = "$ConfluentHome\share\java"
            if (Test-Path $javaDir) {
                $jarDirs = Get-ChildItem $javaDir -Directory | ForEach-Object { "$($_.FullName)\*" }
                $classpath = $jarDirs -join ";"
                Write-Host "   Found $($jarDirs.Count) JAR directories" -ForegroundColor Gray
            } else {
                Write-Error-Custom "Java directory not found: $javaDir"
                exit 1
            }

            $javaArgs = @(
                "-Xms256M",
                "-Xmx2G",
                "-cp", $classpath,
                "io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain",
                $cfg
            )

            $proc = Start-Process -FilePath $javaExe -ArgumentList $javaArgs -WindowStyle Minimized -PassThru
        }

        Write-Host "   Started process PID: $($proc.Id)" -ForegroundColor Gray
        Write-Host "   Waiting for Schema Registry to be ready..." -ForegroundColor Gray
        Start-Sleep 15

        for ($i=1; $i -le 8; $i++) {
            if (Test-SchemaRegistryRunning) {
                Write-Success "Schema Registry is ready on port 8081 (PID: $($proc.Id))"
                return
            }
            Write-Host "   Checking... attempt $i/8" -ForegroundColor Gray
            Start-Sleep 5
        }

        Write-Host "   Process started but not responding on port 8081" -ForegroundColor Yellow
        Write-Host "   The process may still be starting up. Check manually:" -ForegroundColor Gray
        Write-Host "   - Test: curl http://localhost:8081/" -ForegroundColor Gray
        Write-Host "   - Logs: Get-Content $ConfluentHome\logs\schema-registry.log -Tail 50" -ForegroundColor Gray
        Write-Host "   - Process: Get-Process -Id $($proc.Id)" -ForegroundColor Gray
    } catch {
        Write-Error-Custom "Failed: $_"
        exit 1
    }
}

function Stop-SchemaRegistry {
    Write-Header "STOPPING SCHEMA REGISTRY"

    $procs = Get-SchemaRegistryProcess

    if ($procs) {
        foreach ($p in $procs) {
            Stop-Process -Id $p.Id -Force
            Write-Success "Stopped PID: $($p.Id)"
        }
        Start-Sleep 2
    } else {
        Write-Host "   Not running" -ForegroundColor Gray
    }
}

function Show-SchemaRegistryStatus {
    Write-Header "STATUS"

    $procs = Get-SchemaRegistryProcess
    $running = Test-SchemaRegistryRunning

    if ($procs) {
        Write-Host "Process: PID $($procs[0].Id)" -ForegroundColor Cyan
    } else {
        Write-Host "Process: Not running" -ForegroundColor Gray
    }

    if ($running) {
        Write-Success "API responding on :8081"
        try {
            $subj = Invoke-RestMethod -Uri "http://localhost:8081/subjects"
            Write-Host "   Subjects: $($subj.Count)"
        } catch { }
    } else {
        Write-Host "   API not responding" -ForegroundColor Red
    }
}

function Restart-SchemaRegistry {
    Stop-SchemaRegistry
    Start-Sleep 3
    Start-SchemaRegistry
}

function Show-Logs {
    Write-Header "SCHEMA REGISTRY LOGS"

    $logFile = "$ConfluentHome\logs\schema-registry.log"

    if (Test-Path $logFile) {
        Write-Host "Showing last 50 lines of: $logFile" -ForegroundColor Cyan
        Write-Host ""
        Get-Content $logFile -Tail 50
    } else {
        Write-Host "   Log file not found: $logFile" -ForegroundColor Yellow
    }
}

switch ($Action) {
    "start" { Start-SchemaRegistry }
    "stop" { Stop-SchemaRegistry }
    "status" { Show-SchemaRegistryStatus }
    "restart" { Restart-SchemaRegistry }
    "logs" { Show-Logs }
}
