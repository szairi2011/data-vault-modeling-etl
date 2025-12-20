# Schema Registry Configuration - Quick Reference

## 🎯 Problem

The default Confluent Schema Registry configuration requires Kafka, but we want to run it **standalone** (no Kafka) for local development.

---

## ✅ Solution: Modify Existing Config

### File Location
`C:\confluent\confluent-7.5.0\etc\schema-registry\schema-registry.properties`

### Changes Required

**BEFORE (Default - Requires Kafka):**
```properties
kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092
```

**AFTER (Standalone - No Kafka):**
```properties
# Comment out or remove the Kafka line:
# kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092

# Add this line for in-memory storage:
kafkastore.connection.url=
```

---

## 📝 Complete Working Configuration

Replace the contents of `schema-registry.properties` with:

```properties
#
# Confluent Schema Registry - Standalone Mode (No Kafka)
# Modified for local development without Kafka dependency
#

# The address the socket server listens on
listeners=http://0.0.0.0:8081

# STANDALONE MODE: Use in-memory storage (no Kafka required)
# Leave kafkastore.connection.url blank to use in-memory storage
kafkastore.connection.url=

# The name of the topic (not actually used with in-memory, but required by config)
kafkastore.topic=_schemas

# Schema compatibility level
# BACKWARD: New schema can read old data
# FORWARD: Old schema can read new data
# FULL: Both backward and forward compatible
schema.compatibility.level=BACKWARD

# Debugging (set to true if troubleshooting)
debug=false

# Master eligibility (required for standalone)
master.eligibility=true

# Optional: Enable CORS for web clients
access.control.allow.methods=GET,POST,PUT,DELETE,OPTIONS
access.control.allow.origin=*
```

---

## 🚀 Start Schema Registry

```powershell
cd C:\confluent\confluent-7.5.0\bin\windows
.\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry.properties
```

**Or use our helper script:**
```powershell
.\scripts\windows\schema-registry.ps1 start
```

---

## ✅ Verify It's Running

```powershell
# Test connectivity
curl http://localhost:8081/

# Or PowerShell
Invoke-RestMethod -Uri "http://localhost:8081/" -Method Get

# Expected responses (both are good):
# {} 
# OR
# {"error_code":40401,"message":"HTTP 404 Not Found"}

# Both mean Schema Registry is running!
```

---

## 📊 Why This Works

### Kafka-Based (Production)
```
Schema Registry → Kafka → Zookeeper
(Complex, requires all 3 components)
```

### In-Memory (Development)
```
Schema Registry → In-Memory Storage
(Simple, standalone, no dependencies)
```

**Key Setting**: `kafkastore.connection.url=` (blank value)
- When blank, Schema Registry uses in-memory storage
- No Kafka required
- Perfect for local development
- Schemas persist only while process is running

---

## 🔧 Alternative: Create New Config File

If you don't want to modify the default config:

**Create**: `C:\confluent\confluent-7.5.0\etc\schema-registry\schema-registry-standalone.properties`

```properties
# Standalone Schema Registry Configuration
listeners=http://0.0.0.0:8081
kafkastore.connection.url=
kafkastore.topic=_schemas
schema.compatibility.level=BACKWARD
debug=false
master.eligibility=true
```

**Start with new config:**
```powershell
cd C:\confluent\confluent-7.5.0\bin\windows
.\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry-standalone.properties
```

---

## 🐛 Troubleshooting

### Error: "Kafka broker not available"
**Cause**: Still trying to connect to Kafka  
**Fix**: Make sure `kafkastore.connection.url=` is blank (not commented out, just empty value)

### Error: "Address already in use: bind"
**Cause**: Port 8081 is already used  
**Fix**: 
```powershell
# Find what's using port 8081
netstat -ano | findstr :8081

# Kill the process
Stop-Process -Id <PID> -Force
```

### Error: "Java command not found"
**Cause**: JAVA_HOME not set  
**Fix**:
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.0.20.8-hotspot"
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", $env:JAVA_HOME, "User")
```

### Check logs
```powershell
Get-Content "C:\confluent\confluent-7.5.0\logs\schema-registry.log" -Tail 50
```

---

## 📚 Configuration Options Explained

| Setting | Value | Purpose |
|---------|-------|---------|
| `listeners` | `http://0.0.0.0:8081` | Port Schema Registry listens on |
| `kafkastore.connection.url` | `` (blank) | Use in-memory storage (no Kafka) |
| `kafkastore.bootstrap.servers` | (remove) | Don't connect to Kafka |
| `schema.compatibility.level` | `BACKWARD` | Allow backward-compatible changes |
| `master.eligibility` | `true` | Can act as master (required for standalone) |
| `debug` | `false` | Enable debug logging |

---

## 🎯 Quick Commands

```powershell
# Start
.\scripts\windows\schema-registry.ps1 start

# Check status
.\scripts\windows\schema-registry.ps1 status

# Stop
.\scripts\windows\schema-registry.ps1 stop

# Restart
.\scripts\windows\schema-registry.ps1 restart

# View logs
Get-Content C:\confluent\confluent-7.5.0\logs\schema-registry.log -Tail 50

# Test connectivity
curl http://localhost:8081/subjects

# Check process
Get-Process | Where-Object {$_.CommandLine -like "*schema-registry*"}

# Check port
netstat -ano | findstr :8081
```

---

## ✅ Summary

1. **Edit**: `C:\confluent\confluent-7.5.0\etc\schema-registry\schema-registry.properties`
2. **Change**: `kafkastore.connection.url=` (blank value)
3. **Remove/Comment**: `kafkastore.bootstrap.servers` line
4. **Start**: `.\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry.properties`
5. **Verify**: `curl http://localhost:8081/`

**That's it! Schema Registry running standalone without Kafka.** 🎉

---

**See Also**:
- `docs/01_setup_guide.md` - Complete setup guide
- `docs/WINDOWS_NATIVE_SETUP.md` - Windows native overview
- `scripts/windows/schema-registry.ps1` - Management script

