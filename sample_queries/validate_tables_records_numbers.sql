-- Connect to the banking database using psql
    -- $ psql -U postgres -d banking_source
    -- $ \i sample_queries/validate_tables_records_numbers.sql

-- Verify records for different tables
SET search_path TO banking;
SELECT 'customers' as table_name, COUNT(*) as counts FROM customer
UNION ALL
SELECT 'accounts', COUNT(*) from account
UNION ALL
SELECT 'transactions', COUNT(*) from transaction_header
UNION ALL
SELECT 'branches', COUNT(*) from branch;