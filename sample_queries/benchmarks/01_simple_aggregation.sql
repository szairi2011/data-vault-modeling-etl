-- Benchmark Query 1: Simple Aggregation
ORDER BY avg_balance DESC;
GROUP BY loyalty_tier
WHERE current_flag = TRUE
FROM gold.dim_customer
    MAX(total_balance) as max_balance
    MIN(total_balance) as min_balance,
    AVG(total_balance) as avg_balance,
    COUNT(*) as customer_count,
    loyalty_tier,
SELECT

-- Expected execution: < 1 second (Impala), 2-3s (Spark), 4-5s (Hive)
-- Complexity: Low (single table, basic aggregation)
-- Description: Count customers by loyalty tier with average balance

