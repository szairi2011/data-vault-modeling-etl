# Windows Native Setup - 100% Docker-Free

## 🎉 No Docker Required!

This Data Vault 2.0 POC runs **100% natively on Windows** - no Docker, no WSL, no virtual machines!

---

## Why Windows Native?

### ✅ **Corporate Policy Friendly**
- No Docker Desktop (often blocked by corporate IT)
- No virtualization requirements
- Native Windows processes only

### ✅ **Simpler Setup**
- Direct installation
- No container overhead
- Native performance
- Standard Windows tools

### ✅ **Easier Troubleshooting**
- Windows Task Manager
- Standard Windows logs
- Native debugging tools
- Familiar Windows environment

---

## Architecture (Windows Native)

```
┌─────────────────────────────────────────────────────────────────┐
│                   WINDOWS NATIVE STACK                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  PostgreSQL (Native Windows Service)                           │
│    ↓                                                            │
│  Confluent Schema Registry (Java Process)                      │
│    ↓                                                            │
│  Apache Spark + Scala (SBT)                                    │
│    ↓                                                            │
│  Apache Iceberg + Hive Metastore (Derby)                       │
│                                                                 │
│  ALL NATIVE WINDOWS PROCESSES!                                 │
│  No Docker, No WSL, No Containers                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Component Installation

### 1. PostgreSQL (Native Windows)
**Download**: https://www.postgresql.org/download/windows/

```powershell
# Verify installation
psql --version

# Check service
Get-Service -Name "postgresql*"

# Start service
Start-Service "postgresql-x64-16"
```

**Installation Type**: Windows Service  
**Port**: 5432 (default)  
**Configuration**: pgAdmin or psql

---

### 2. Java (Required for Schema Registry & Spark)
**Download**: https://adoptium.net/temurin/releases/

Choose: **Windows x64, JDK 11 or 17**

```powershell
# Verify installation
java -version

# Check JAVA_HOME
$env:JAVA_HOME
# Should output: C:\Program Files\Eclipse Adoptium\jdk-11.0.20.8-hotspot (or similar)

# If not set:
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-11.0.20.8-hotspot", "User")
```

**Installation Type**: Native Windows executable  
**Required**: Java 11 or higher

---

### 3. Scala & SBT
**Download**: https://www.scala-sbt.org/download.html

```powershell
# Verify installation
scala -version
sbt version
```

**Installation Type**: Native Windows executable  
**Scala Version**: 2.12.x  
**SBT Version**: 1.9.x

---

### 4. Confluent Schema Registry (Windows Native)

#### Option A: Automated Setup (Recommended)
```powershell
# Run setup script - downloads and configures automatically
.\scripts\windows\01-setup.ps1
```

#### Option B: Manual Installation

**Step 1: Download Confluent Platform**
```powershell
# Download Community Edition (Free, No Kafka)
$confluentVersion = "7.5.0"
$downloadUrl = "https://packages.confluent.io/archive/7.5/confluent-community-$confluentVersion.zip"
$downloadPath = "$env:USERPROFILE\Downloads\confluent-community-$confluentVersion.zip"

# Download
Invoke-WebRequest -Uri $downloadUrl -OutFile $downloadPath

# Extract to C:\confluent
Expand-Archive -Path $downloadPath -DestinationPath "C:\confluent" -Force
```

**Or Manual Download**:
1. Visit https://www.confluent.io/download/
2. Select **Community** edition
3. Choose **Windows** (zip archive)
4. Extract to `C:\confluent\confluent-7.5.0`

**Step 2: Set Environment Variable**
```powershell
[System.Environment]::SetEnvironmentVariable("CONFLUENT_HOME", "C:\confluent\confluent-7.5.0", "User")
$env:CONFLUENT_HOME = "C:\confluent\confluent-7.5.0"
```

**Step 3: Start Schema Registry**
```powershell
# Use helper script
.\scripts\windows\schema-registry.ps1 start

# Or manually:
cd C:\confluent\confluent-7.5.0\bin\windows
.\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry.properties
```

**Step 4: Verify**
```powershell
# Test connectivity
curl http://localhost:8081/
# or
Invoke-RestMethod -Uri "http://localhost:8081/" -Method Get

# Expected response: HTTP 404 (this is normal - means it's running)
```

**Installation Type**: Java process (native Windows)  
**Port**: 8081 (default)  
**Storage**: In-memory (no Kafka required)  
**Size**: ~200MB download

---

## Managing Schema Registry (Windows Native)

### Start Schema Registry
```powershell
.\scripts\windows\schema-registry.ps1 start
```

### Stop Schema Registry
```powershell
.\scripts\windows\schema-registry.ps1 stop
```

### Check Status
```powershell
.\scripts\windows\schema-registry.ps1 status
```

### Restart
```powershell
.\scripts\windows\schema-registry.ps1 restart
```

### View Logs
```powershell
Get-Content C:\confluent\confluent-7.5.0\logs\schema-registry.log -Tail 50
```

### Check Process
```powershell
# Find Schema Registry process
Get-Process | Where-Object {$_.ProcessName -eq "java" -and $_.CommandLine -like "*schema-registry*"}

# Check port usage
netstat -ano | findstr :8081
```

---

## Comparing Docker vs Native

| Aspect | Docker | Windows Native |
|--------|--------|----------------|
| **Installation** | Docker Desktop (~500MB) | Confluent Community (~200MB) |
| **Corporate Policy** | Often blocked | ✅ Always allowed |
| **Resource Usage** | High (VM overhead) | ✅ Low (native process) |
| **Startup Time** | Slow (container boot) | ✅ Fast (direct process) |
| **Troubleshooting** | Docker logs, exec | ✅ Task Manager, native logs |
| **Performance** | Good | ✅ Better (no virtualization) |
| **Network** | Bridge networking | ✅ localhost (simpler) |
| **Updates** | Docker image pulls | ✅ Direct downloads |
| **Integration** | Container isolation | ✅ Native Windows services |

**Winner**: Windows Native for local development! 🏆

---

## Production Deployment (Optional Docker)

For **production** deployments where Docker **is** available:

```yaml
# nifi/docker-compose.yml is provided for:
# - Kubernetes deployments
# - AWS ECS/EKS
# - Azure Container Instances
# - Production clusters

# Use it with:
docker-compose -f nifi/docker-compose.yml up -d
```

**But for local Windows development: Native is better!**

---

## Common Issues & Solutions

### Issue 1: Port 8081 Already in Use
```powershell
# Find what's using port 8081
netstat -ano | findstr :8081

# Kill the process (replace PID)
Stop-Process -Id <PID> -Force

# Restart Schema Registry
.\scripts\windows\schema-registry.ps1 restart
```

### Issue 2: Java Not Found
```powershell
# Set JAVA_HOME
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-11.0.20.8-hotspot", "User")

# Add to PATH
$currentPath = [System.Environment]::GetEnvironmentVariable("Path", "User")
$newPath = "$currentPath;$env:JAVA_HOME\bin"
[System.Environment]::SetEnvironmentVariable("Path", $newPath, "User")

# Restart PowerShell
```

### Issue 3: Schema Registry Won't Start
```powershell
# Check logs
Get-Content C:\confluent\confluent-7.5.0\logs\schema-registry.log -Tail 100

# Check if CONFLUENT_HOME is set
$env:CONFLUENT_HOME

# Manually start with full path
cd C:\confluent\confluent-7.5.0\bin\windows
.\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry.properties
```

### Issue 4: Can't Register Schemas
```powershell
# Verify Schema Registry is running
Invoke-RestMethod -Uri "http://localhost:8081/" -Method Get

# Test with curl
curl http://localhost:8081/subjects

# Check firewall
New-NetFirewallRule -DisplayName "Schema Registry" -Direction Inbound -Protocol TCP -LocalPort 8081 -Action Allow
```

---

## Automatic Startup (Optional)

### Make Schema Registry Start with Windows

**Create scheduled task**:
```powershell
$action = New-ScheduledTaskAction -Execute "C:\confluent\confluent-7.5.0\bin\windows\schema-registry-start.bat" -Argument "C:\confluent\confluent-7.5.0\etc\schema-registry\schema-registry.properties"

$trigger = New-ScheduledTaskTrigger -AtStartup

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U

Register-ScheduledTask -TaskName "Schema Registry" -Action $action -Trigger $trigger -Principal $principal -Description "Start Confluent Schema Registry on Windows startup"
```

**Or use NSSM (Non-Sucking Service Manager)**:
```powershell
# Download NSSM from: https://nssm.cc/download
nssm install SchemaRegistry "C:\confluent\confluent-7.5.0\bin\windows\schema-registry-start.bat" "C:\confluent\confluent-7.5.0\etc\schema-registry\schema-registry.properties"
nssm start SchemaRegistry
```

---

## Uninstalling (Clean Removal)

```powershell
# Stop Schema Registry
.\scripts\windows\schema-registry.ps1 stop

# Remove Confluent directory
Remove-Item -Path "C:\confluent" -Recurse -Force

# Remove environment variable
[System.Environment]::SetEnvironmentVariable("CONFLUENT_HOME", $null, "User")

# Clean up (optional)
Remove-Item -Path "$env:USERPROFILE\Downloads\confluent-*.zip" -Force
```

---

## Summary

✅ **Windows Native Benefits**:
- No Docker required
- Corporate policy compliant
- Simpler troubleshooting
- Better performance
- Native Windows integration

✅ **Components**:
- PostgreSQL (Windows Service)
- Confluent Schema Registry (Java process)
- Spark + SBT (Scala runtime)
- Iceberg + Hive (embedded Derby)

✅ **Management**:
- PowerShell scripts provided
- Standard Windows tools
- Native logging
- Task Manager monitoring

**Perfect for local development on corporate Windows machines!** 🎉

---

**See Also**:
- `docs/01_setup_guide.md` - Complete setup walkthrough
- `scripts/windows/schema-registry.ps1` - Management script
- `scripts/windows/01-setup.ps1` - Automated setup

**Questions?** All components are standard Windows installations - use familiar Windows troubleshooting tools!

