-- Connect to the banking database using psql
-- $ psql -U postgres -d banking_source
-- $

-- SET search_path TO banking;
-- select 'customers' as table_name, count(*) as row_count from customer
-- UNION ALL
-- select 'accounts', count(*) from account;
-- Sum of all balances and number of accounts per customer
-- Something like: Barbara Anderson has 3 bank accounts with a total balance of 90574.26 $ 
SELECT 
	cus.customer_id as cusID,
	cus.customer_number,
	cus.first_name as fname, 
	cus.last_name as lname,
	COUNT(*) num_accounts,
	SUM(acc.available_balance) as sum_all_balances
	-- br.branch_name
FROM customer cus 
JOIN account acc on acc.customer_id = cus.customer_id
GROUP BY customer_number, cusID, fname, lname
ORDER BY fname, lname;