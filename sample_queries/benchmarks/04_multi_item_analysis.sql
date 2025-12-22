-- Benchmark Query 4: Multi-Item Transaction Analysis
-- Description: Analyze transactions with multiple items (e-commerce pattern)
-- Complexity: Medium (one-to-many joins, string aggregation)
-- Expected execution: 2-3s (Impala), 4-6s (Spark), 7-9s (Hive)

SELECT
    th.transaction_id,
    th.transaction_number,
    th.transaction_date,
    th.total_amount,
    COUNT(ti.item_id) as item_count,
    STRING_AGG(ti.merchant_name, ', ') as merchants,
    STRING_AGG(ti.merchant_category, ', ') as categories,
    SUM(ti.item_amount) as items_total,
    th.total_amount - SUM(ti.item_amount) as amount_difference
FROM gold.fact_transaction_header th
JOIN gold.fact_transaction_item ti
    ON th.transaction_id = ti.transaction_id
WHERE th.transaction_type = 'PAYMENT'
    AND th.transaction_date >= DATE_SUB(CURRENT_DATE, 30)
GROUP BY
    th.transaction_id,
    th.transaction_number,
    th.transaction_date,
    th.total_amount
HAVING item_count > 1
ORDER BY total_amount DESC
LIMIT 50;

