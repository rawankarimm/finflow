
            dt.sim_day, 
            ft.transaction_type_id, 
            COUNT(*) AS volume
        FROM fact_transactions ft
        JOIN dim_time dt ON ft.step = dt.step
        GROUP BY ft.transaction_type_id, dt.sim_day;

SELECT 
            dt.sim_day, 
            COUNT(*) AS fraud_count
        FROM fact_transactions ft
        JOIN dim_time dt ON ft.step = dt.step
        WHERE ft.is_fraud = TRUE
        GROUP BY dt.sim_day;

SELECT 
            dt.sim_day, 
            ft.transaction_type_id, 
            AVG(ft.amount) AS mean_amount
        FROM fact_transactions ft
        JOIN dim_time dt ON ft.step = dt.step
        GROUP BY dt.sim_day, ft.transaction_type_id;

SELECT
            dtt.transaction_type_name AS transaction_type,
            ft.amount
        FROM fact_transactions ft
        JOIN dim_transaction_type dtt
            ON ft.transaction_type_id = dtt.transaction_type_id
        WHERE dtt.transaction_type_name IN ('TRANSFER', 'CASH_OUT');

-- Conditional fraud probability by balance-drain tail thresholds.
-- This query estimates P(fraud | |balance_drain| > threshold) at the 75th, 90th, 95th, and 99th percentiles of absolute balance_drain.


WITH abs_balance_drain AS (
    SELECT
        ABS(balance_drain) AS abs_balance_drain,
        is_fraud
    FROM fact_transactions
    WHERE balance_drain IS NOT NULL
),
percentiles AS (
    SELECT
        percentile_cont(0.75) WITHIN GROUP (ORDER BY abs_balance_drain) AS p75,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY abs_balance_drain) AS p90,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY abs_balance_drain) AS p95,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY abs_balance_drain) AS p99
    FROM abs_balance_drain
)
SELECT 'p75' AS threshold_label,
       (SELECT p75 FROM percentiles) AS threshold_value,
       SUM(CASE WHEN abs_balance_drain > (SELECT p75 FROM percentiles) AND is_fraud THEN 1 ELSE 0 END) * 1.0
       / NULLIF(SUM(CASE WHEN abs_balance_drain > (SELECT p75 FROM percentiles) THEN 1 ELSE 0 END), 0) AS p_fraud_given_threshold
FROM abs_balance_drain
UNION ALL
SELECT 'p90',
       (SELECT p90 FROM percentiles),
       SUM(CASE WHEN abs_balance_drain > (SELECT p90 FROM percentiles) AND is_fraud THEN 1 ELSE 0 END) * 1.0
       / NULLIF(SUM(CASE WHEN abs_balance_drain > (SELECT p90 FROM percentiles) THEN 1 ELSE 0 END), 0)
FROM abs_balance_drain
UNION ALL
SELECT 'p95',
       (SELECT p95 FROM percentiles),
       SUM(CASE WHEN abs_balance_drain > (SELECT p95 FROM percentiles) AND is_fraud THEN 1 ELSE 0 END) * 1.0
       / NULLIF(SUM(CASE WHEN abs_balance_drain > (SELECT p95 FROM percentiles) THEN 1 ELSE 0 END), 0)
FROM abs_balance_drain
UNION ALL
SELECT 'p99',
       (SELECT p99 FROM percentiles),
       SUM(CASE WHEN abs_balance_drain > (SELECT p99 FROM percentiles) AND is_fraud THEN 1 ELSE 0 END) * 1.0
       / NULLIF(SUM(CASE WHEN abs_balance_drain > (SELECT p99 FROM percentiles) THEN 1 ELSE 0 END), 0)
FROM abs_balance_drain;


SELECT * FROM complaints;



SELECT 
dtt.transaction_type_name as transaction_type,
ft.amount
FROM fact_transactions ft
JOIN dim_transaction_type dtt ON dtt.transaction_type_id = ft.transaction_type_id
WHERE dtt.transaction_type_name IN ('TRANSFER', 'CASH_OUT');
