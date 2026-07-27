CREATE VIEW IF NOT EXISTS v_monthly_volume AS
SELECT 
    ft.transaction_type_id,
    dt.sim_day / 30 AS sim_month,
    SUM(ft.amount) AS total_amount, 
    COUNT(*) AS transaction_count
FROM fact_transactions ft
JOIN dim_time dt ON ft.step = dt.step
GROUP BY ft.transaction_type_id, dt.sim_day / 30;


CREATE VIEW IF NOT EXISTS v_fraud_by_type AS
SELECT 
    ft.transaction_type_id,
    COUNT(*) AS total_count,
    SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_count,
    SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS fraud_rate
    
FROM fact_transactions ft
GROUP BY transaction_type_id;

CREATE VIEW IF NOT EXISTS v_monthly_complaints AS
SELECT
    product,
    STRFTIME(date_received, '%Y-%m') AS year_month,
    COUNT(*) AS complaint_count
FROM complaints
GROUP BY product, STRFTIME(date_received, '%Y-%m');

CREATE VIEW IF NOT EXISTS v_balance_anomalies AS
SELECT 
    transaction_id,
    amount,
    balance_drain,
    is_fraud
FROM fact_transactions
WHERE is_fraud = FALSE AND ABS(balance_drain) > 0.01; --filtering out tiny floating-point errors 

