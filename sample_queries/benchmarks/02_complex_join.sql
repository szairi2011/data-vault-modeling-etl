-- Benchmark Query 2: Complex Join (Customer 360)
-- Description: Customer 360 view with account and transaction details
-- Complexity: High (multi-table joins, aggregations)
-- Expected execution: 3-4s (Impala), 5-7s (Spark), 8-10s (Hive)

SELECT
    c.customer_id,
    c.full_name,
    c.loyalty_tier,
    c.customer_type,
    COUNT(DISTINCT a.account_id) as account_count,
    SUM(a.balance) as total_balance,
    COUNT(t.transaction_id) as transaction_count,
    SUM(t.net_amount) as total_transaction_amount
FROM gold.dim_customer c
LEFT JOIN gold.dim_account a
    ON c.customer_id = a.customer_id
    AND a.current_flag = TRUE
LEFT JOIN gold.fact_transaction t
    ON a.account_id = t.account_id
WHERE c.current_flag = TRUE
GROUP BY c.customer_id, c.full_name, c.loyalty_tier, c.customer_type
HAVING total_balance > 10000
ORDER BY total_balance DESC
LIMIT 100;

