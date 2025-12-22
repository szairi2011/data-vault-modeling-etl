-- Benchmark Query 5: Schema Evolution Query
-- Description: Query spanning old and new schema (with loyalty_tier field)
-- Complexity: Low (NULL handling, schema evolution resilience)
-- Expected execution: 1-2s (Impala), 3-5s (Spark), 6-8s (Hive)

SELECT
    c.customer_id,
    c.email,
    c.loyalty_tier,  -- New field added via schema evolution (NULL for old records)
    c.customer_type,
    c.valid_from,
    c.valid_to,
    CASE
        WHEN c.loyalty_tier IS NULL THEN 'Pre-Loyalty Era'
        ELSE c.loyalty_tier
    END as tier_status,
    CASE
        WHEN c.valid_to IS NULL THEN 'Current Record'
        ELSE 'Historical Record'
    END as record_status
FROM gold.dim_customer c
WHERE c.customer_id IN (1, 10, 50, 100, 500, 1000)
ORDER BY c.customer_id, c.valid_from DESC;

