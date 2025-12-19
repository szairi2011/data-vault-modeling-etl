-- Connect to the banking database using psql
-- $ psql -U postgres -d banking_source
-- $ \i sample_queries/num_of_accounts_per_branch_and_per_cust.sql

SET search_path TO banking;
select 
	c.customer_id as cid,
	c.customer_number as cnumber,
	c.first_name as fname,
	c.last_name as lname,
	b.branch_name as branch,
	-- t.transaction_type as trans_type,
	-- SUM(t.total_amount) as all_transacs_amount,
	COUNT(a.account_id) as num_accounts,
	a.account_number as acc_number
FROM customer c
JOIN account a on a.customer_id = c.customer_id
JOIN branch b on a.branch_id = b.branch_id
JOIN transaction_header t on t.account_id = a.account_id
-- GROUP BY cid, cnumber, fname, lname, branch HAVING b.branch_name = 'Manhattan Investment'
WHERE c.customer_number = 'CUS-000635'
GROUP BY cid, cnumber, fname, lname, branch, acc_number;
-- GROUP BY cid, cnumber, fname, lname, branch;
-- ORDER BY all_transacs_amount DESC LIMIT 10;