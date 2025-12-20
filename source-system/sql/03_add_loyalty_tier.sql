/*
 * ========================================================================
 * SCHEMA EVOLUTION SCENARIO: Add Loyalty Tier
 * ========================================================================
 *
 * PURPOSE:
 * Demonstrate Data Vault 2.0 resilience to source system schema changes.
 * This script adds a new column to the customer table, simulating a real
 * business requirement (loyalty program launch).
 *
 * LEARNING OBJECTIVES:
 * - How Data Vault handles schema evolution gracefully
 * - Backward compatibility in action
 * - No downtime for existing queries
 * - Controlled propagation to dimensional models
 *
 * SCENARIO:
 * Marketing department launches a loyalty program with 4 tiers based on
 * total account balance:
 * - PLATINUM: Balance > $100,000
 * - GOLD: Balance > $50,000
 * - SILVER: Balance > $10,000
 * - STANDARD: Balance <= $10,000
 *
 * EXPECTED IMPACT:
 * - Source table structure changes
 * - NiFi CDC detects new column automatically
 * - Avro schema evolves (V1 → V2)
 * - Data Vault Satellite absorbs new field
 * - Existing queries continue working
 * - New analytics become possible
 *
 * ========================================================================
 */

-- ============================================================================
-- STEP 1: Add New Column (Schema Evolution)
-- ============================================================================

ALTER TABLE banking.customer
ADD COLUMN IF NOT EXISTS loyalty_tier VARCHAR(20) DEFAULT 'STANDARD';

COMMENT ON COLUMN banking.customer.loyalty_tier IS
  'Customer loyalty tier (STANDARD, SILVER, GOLD, PLATINUM). Added 2025-01-19 for loyalty program.';

/*
 * WHY DEFAULT 'STANDARD':
 * - All existing customers start at base tier
 * - Prevents NULL values in new column
 * - Backward compatible (no schema validation errors)
 */

-- ============================================================================
-- STEP 2: Populate Loyalty Tiers Based on Account Balance
-- ============================================================================

/*
 * BUSINESS RULE:
 * Calculate total balance across all accounts for each customer
 * and assign tier based on thresholds.
 */

UPDATE banking.customer c
SET loyalty_tier = CASE
    -- PLATINUM: High net worth customers
    WHEN (
        SELECT COALESCE(SUM(balance), 0)
        FROM banking.account
        WHERE customer_id = c.customer_id
    ) > 100000 THEN 'PLATINUM'

    -- GOLD: Affluent customers
    WHEN (
        SELECT COALESCE(SUM(balance), 0)
        FROM banking.account
        WHERE customer_id = c.customer_id
    ) > 50000 THEN 'GOLD'

    -- SILVER: Growing customers
    WHEN (
        SELECT COALESCE(SUM(balance), 0)
        FROM banking.account
        WHERE customer_id = c.customer_id
    ) > 10000 THEN 'SILVER'

    -- STANDARD: Base tier
    ELSE 'STANDARD'
END;

/*
 * EXPECTED DISTRIBUTION (approximate):
 * - PLATINUM: 5-10% of customers
 * - GOLD: 15-20% of customers
 * - SILVER: 30-35% of customers
 * - STANDARD: 40-50% of customers
 */

-- ============================================================================
-- STEP 3: Update Timestamp to Trigger CDC
-- ============================================================================

/*
 * IMPORTANT:
 * NiFi's QueryDatabaseTable uses updated_at for CDC.
 * We must update this timestamp to trigger extraction.
 */

UPDATE banking.customer
SET updated_at = CURRENT_TIMESTAMP;

/*
 * WHAT HAPPENS NEXT:
 * 1. NiFi QueryDatabaseTable detects updated_at change
 * 2. Extracts all customers with new loyalty_tier column
 * 3. ConvertRecord creates Avro with V2 schema
 * 4. Schema Registry validates backward compatibility
 * 5. Files written to staging zone
 * 6. Spark ETL reads Avro and auto-detects new field
 * 7. Satellite table schema evolves (Iceberg handles this)
 * 8. Historical records have NULL for loyalty_tier
 * 9. New records have actual tier values
 */

-- ============================================================================
-- STEP 4: Verification Queries
-- ============================================================================

-- 4.1 Check loyalty tier distribution
SELECT
    loyalty_tier,
    COUNT(*) as customer_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM banking.customer
GROUP BY loyalty_tier
ORDER BY
    CASE loyalty_tier
        WHEN 'PLATINUM' THEN 1
        WHEN 'GOLD' THEN 2
        WHEN 'SILVER' THEN 3
        ELSE 4
    END;

/*
 * EXPECTED OUTPUT:
 * loyalty_tier | customer_count | percentage
 * -------------+----------------+------------
 * PLATINUM     | 65             | 6.50
 * GOLD         | 150            | 15.00
 * SILVER       | 300            | 30.00
 * STANDARD     | 485            | 48.50
 */

-- 4.2 Show top PLATINUM customers
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.loyalty_tier,
    COALESCE(SUM(a.balance), 0) as total_balance
FROM banking.customer c
LEFT JOIN banking.account a ON c.customer_id = a.customer_id
WHERE c.loyalty_tier = 'PLATINUM'
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.loyalty_tier
ORDER BY total_balance DESC
LIMIT 10;

-- 4.3 Verify all customers have loyalty_tier
SELECT
    COUNT(*) as total_customers,
    COUNT(loyalty_tier) as customers_with_tier,
    COUNT(*) - COUNT(loyalty_tier) as customers_without_tier
FROM banking.customer;

/*
 * EXPECTED:
 * total_customers | customers_with_tier | customers_without_tier
 * ----------------+---------------------+------------------------
 * 1000            | 1000                | 0
 */

-- 4.4 Check for customers without accounts (edge case)
SELECT
    c.customer_id,
    c.email,
    c.loyalty_tier,
    COUNT(a.account_id) as account_count
FROM banking.customer c
LEFT JOIN banking.account a ON c.customer_id = a.customer_id
GROUP BY c.customer_id, c.email, c.loyalty_tier
HAVING COUNT(a.account_id) = 0;

/*
 * These customers should have STANDARD tier (no accounts = $0 balance)
 */

-- ============================================================================
-- STEP 5: Create Audit Trail Entry (Optional)
-- ============================================================================

/*
 * Document the schema change for compliance and troubleshooting
 */

CREATE TABLE IF NOT EXISTS banking.schema_changes (
    change_id SERIAL PRIMARY KEY,
    change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    change_type VARCHAR(50),
    description TEXT,
    applied_by VARCHAR(100)
);

INSERT INTO banking.schema_changes (
    table_name,
    column_name,
    change_type,
    description,
    applied_by
) VALUES (
    'banking.customer',
    'loyalty_tier',
    'ADD_COLUMN',
    'Added loyalty_tier column for customer loyalty program. 4 tiers: PLATINUM, GOLD, SILVER, STANDARD based on total account balance.',
    CURRENT_USER
);

-- ============================================================================
-- ROLLBACK (If Needed)
-- ============================================================================

/*
 * To rollback this change (for testing):
 *
 * -- Remove column
 * ALTER TABLE banking.customer DROP COLUMN IF EXISTS loyalty_tier;
 *
 * -- Reset updated_at to previous values (if you saved them)
 * -- This prevents re-extraction by NiFi CDC
 *
 * CAUTION: Rollback after Data Vault loading will create inconsistency.
 * Data Vault preserves history, so old satellite records will have the field.
 * Better approach: Set loyalty_tier to NULL for new changes.
 */

-- ============================================================================
-- TESTING CHECKLIST
-- ============================================================================

/*
 * After running this script, verify:
 *
 * ✅ 1. Column exists in source table
 *      \d banking.customer (should show loyalty_tier)
 *
 * ✅ 2. All customers have tier assigned
 *      SELECT COUNT(*) FROM banking.customer WHERE loyalty_tier IS NULL;
 *      (Should return 0)
 *
 * ✅ 3. Reasonable distribution
 *      Check tier percentages make sense for your data
 *
 * ✅ 4. NiFi CDC picks up changes
 *      Check staging zone: warehouse\staging\customer\
 *      New Avro files should have loyalty_tier field
 *
 * ✅ 5. Avro schema V2 registered
 *      curl http://localhost:8081/subjects/banking.customer-value/versions
 *      Should show version 2
 *
 * ✅ 6. Bronze ETL completes successfully
 *      sbt "runMain bronze.RawVaultETL --entity customer"
 *      Should detect new field and load to Satellite
 *
 * ✅ 7. Query historical vs current data
 *      Verify old records have NULL loyalty_tier
 *      Verify new records have actual tier values
 *
 * ✅ 8. Existing queries still work
 *      Run queries that don't reference loyalty_tier
 *      They should return same results as before
 */

-- ============================================================================
-- NEXT STEPS
-- ============================================================================

/*
 * After verifying Bronze layer absorbed the change:
 *
 * 1. SILVER LAYER (Business Vault):
 *    - Rebuild PIT tables to include loyalty_tier
 *    - Update bridges if needed
 *
 * 2. GOLD LAYER (Dimensional Model):
 *    - DECISION POINT: Add to Dim_Customer?
 *    - Business decides when to expose new field
 *    - Existing dashboards unaffected until you update them
 *
 * 3. SEMANTIC LAYER:
 *    - Create new views for loyalty analysis
 *    - Example: v_loyalty_tier_distribution
 *    - Example: v_loyalty_retention_rate
 *
 * 4. BI DASHBOARDS:
 *    - Build loyalty program dashboard
 *    - Analyze tier migration patterns
 *    - Calculate tier-specific metrics
 */

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================

-- Show summary
SELECT
    'Schema Evolution Complete' as status,
    COUNT(*) as total_customers,
    COUNT(DISTINCT loyalty_tier) as tier_count,
    MAX(updated_at) as last_updated
FROM banking.customer;

