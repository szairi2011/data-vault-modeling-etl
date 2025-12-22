-- Benchmark Query 3: Temporal Query (Point-in-Time)
-- Description: Account balances at specific point in time using PIT tables
-- Complexity: Medium (temporal joins, Data Vault pattern)
-- Expected execution: 4-5s (Impala), 6-8s (Spark), 10-12s (Hive)

SELECT
    p.customer_id,
    p.account_id,
    s.balance,
    s.account_status,
    s.account_type,
    s.load_date,
    p.snapshot_date
FROM silver.pit_account p
JOIN bronze.sat_account s
    ON p.account_hkey = s.account_hkey
    AND p.sat_account_ldts = s.load_date
WHERE p.snapshot_date = DATE('2024-12-01')
    AND s.balance > 5000
ORDER BY s.balance DESC
LIMIT 50;

