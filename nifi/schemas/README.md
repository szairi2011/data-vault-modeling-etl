# Avro Schemas for Banking Data Vault POC

This directory contains Avro schema definitions (`.avsc` files) for all source system entities extracted via NiFi CDC pipeline.

## 📋 Schema Files

| File | Entity | Purpose |
|------|--------|---------|
| `customer.avsc` | Customer master data | Individual/business customer information |
| `account.avsc` | Account master data | Checking, savings, loans, credit cards |
| `transaction_header.avsc` | Transaction header | Parent transaction record |
| `transaction_item.avsc` | Transaction line items | Child transaction details (multi-item) |

## 🎯 Why Avro?

Avro is chosen for CDC (Change Data Capture) because:

1. **Self-Describing**: Schema embedded in each file
2. **Compact**: Binary format 30-50% smaller than JSON
3. **Fast**: No parsing overhead compared to text formats
4. **Schema Evolution**: Add/remove fields without breaking consumers
5. **Splittable**: Parallel processing with sync markers
6. **Type Safety**: Strong typing with logical types (date, decimal, timestamp)

## 🔄 Schema Evolution Rules

All schemas follow **backward compatibility** rules:

### ✅ Allowed Changes (Non-Breaking)
- Add optional field with default value
- Add enum symbol (at end of list)
- Promote type (int → long, float → double)
- Add documentation

### ❌ Forbidden Changes (Breaking)
- Remove required field
- Change field type (string → int)
- Rename field without alias
- Remove enum symbol
- Change field order (for serialization)

## 📝 Schema Naming Convention

Subject naming in Schema Registry follows pattern:
```
{namespace}.{entity}-{type}

Examples:
- banking.customer-value
- banking.account-value
- banking.transaction_header-value
- banking.transaction_item-value
```

The `-value` suffix indicates this is the schema for record values (vs. keys).

## 🚀 Registering Schemas

### Windows PowerShell
```powershell
.\nifi\scripts\register-schemas.ps1
```

### Linux/Mac
```bash
./nifi/scripts/register-schemas.sh
```

### Manual Registration
```bash
# Register customer schema
curl -X POST http://localhost:8081/subjects/banking.customer-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @nifi/schemas/customer.avsc

# Verify registration
curl http://localhost:8081/subjects/banking.customer-value/versions
```

## 🔍 Schema Evolution Example

### Initial Schema (V1)
```json
{
  "name": "Customer",
  "fields": [
    {"name": "customer_id", "type": "int"},
    {"name": "first_name", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}
```

### Evolved Schema (V2) - Adding Loyalty Tier
```json
{
  "name": "Customer",
  "fields": [
    {"name": "customer_id", "type": "int"},
    {"name": "first_name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "loyalty_tier", "type": ["null", "string"], "default": null}
  ]
}
```

**Result:**
- ✅ Old consumers continue working (ignore new field)
- ✅ New consumers get loyalty_tier (NULL for historical records)
- ✅ Data Vault automatically captures in new satellite records

## 🧪 Testing Schemas

### Validate Schema Syntax
```bash
# Using avro-tools
java -jar avro-tools-1.11.0.jar compile schema customer.avsc .

# Expected: Generates Customer.java (validates syntax)
```

### Test Schema Compatibility
```bash
# Check if V2 is backward compatible with V1
curl -X POST http://localhost:8081/compatibility/subjects/banking.customer-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @nifi/schemas/customer_v2.avsc

# Response: {"is_compatible": true}
```

## 📊 Logical Types Used

| Logical Type | Avro Type | Description | Example |
|--------------|-----------|-------------|---------|
| `date` | `int` | Days since epoch | 19723 = 2024-01-01 |
| `timestamp-millis` | `long` | Milliseconds since epoch | 1704067200000 = 2024-01-01 00:00:00 UTC |
| `decimal` | `bytes` | Fixed-point decimal | Decimal(15,2) for currency |

## 🔗 Integration with NiFi

NiFi processors use these schemas:

1. **ConvertRecord**
   - Input: CSV from QueryDatabaseTable
   - Output: Avro with schema from Registry
   - Schema validation: On write

2. **AvroSchemaRegistry Controller Service**
   - URL: `http://localhost:8081`
   - Schema strategy: Use schema name property
   - Cache: 1000 schemas, 1 hour TTL

3. **PutFile**
   - Writes Avro files to staging zone
   - Schema embedded in file header
   - Sync markers for splittability

## 🎓 Learning Resources

- [Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [Schema Evolution Best Practices](https://docs.confluent.io/platform/current/schema-registry/avro.html)
- [Logical Types](https://avro.apache.org/docs/current/spec.html#Logical+Types)

## 🐛 Troubleshooting

### Issue: Schema registration fails
```
Error: Incompatible schema
```

**Solution:** Check compatibility rules. Use `none` mode for development:
```bash
curl -X PUT http://localhost:8081/config/banking.customer-value \
  -H "Content-Type: application/json" \
  -d '{"compatibility": "NONE"}'
```

### Issue: NiFi can't read schema
```
Error: Subject not found
```

**Solution:** Verify schema registered:
```bash
curl http://localhost:8081/subjects
```

### Issue: Decimal precision error
```
Error: Value out of range for precision
```

**Solution:** Increase precision in schema:
```json
{
  "type": "bytes",
  "logicalType": "decimal",
  "precision": 20,  // Increase from 15
  "scale": 2
}
```

## 📅 Schema Change Log

| Date | Schema | Version | Change | Breaking? |
|------|--------|---------|--------|-----------|
| 2025-01-01 | customer | 1.0 | Initial version | N/A |
| 2025-01-15 | customer | 2.0 | Add loyalty_tier (optional) | No |
| 2025-01-01 | account | 1.0 | Initial version | N/A |
| 2025-01-01 | transaction_header | 1.0 | Initial version | N/A |
| 2025-01-01 | transaction_item | 1.0 | Initial version | N/A |

## ✅ Next Steps

1. Start Schema Registry: `docker-compose -f nifi/docker-compose.yml up -d`
2. Register all schemas: `.\nifi\scripts\register-schemas.ps1`
3. Configure NiFi controller services
4. Import NiFi template: `nifi/templates/01_source_to_staging.xml`
5. Start NiFi processors
6. Verify Avro files in staging: `warehouse/staging/customer/*.avro`

