CREATE TABLE IF NOT EXISTS dim_transaction_type (
    transaction_type_id INT PRIMARY KEY,
    transaction_type_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_account_type (
    account_type_id INT PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_account (
    account_id INT PRIMARY KEY,
    account_name VARCHAR(100),
    account_type_id INT,
    FOREIGN KEY (account_type_id) REFERENCES dim_account_type(account_type_id)
);

CREATE TABLE IF NOT EXISTS dim_time (
    step INT PRIMARY KEY,
    sim_day INT,
    sim_week INT,
    hour_of_day INT
);

CREATE TABLE IF NOT EXISTS complaints (
    complaint_id INT PRIMARY KEY,
    date_received DATE,
    product VARCHAR(100) NOT NULL,
    sub_product VARCHAR(100),
    issue VARCHAR(255) NOT NULL,
    company VARCHAR(100) NOT NULL,
    state CHAR(2),
    resolution VARCHAR(255) NOT NULL
); 

-- DuckDB automatically handles auto-increment on BIGINT PRIMARY KEY
-- without needing 'CREATE SEQUENCE' or 'DEFAULT nextval()'
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id BIGINT PRIMARY KEY,
    transaction_type_id INT,
    step INT NOT NULL,
    sender_account_id INT NOT NULL,
    receiver_account_id INT NOT NULL,
    amount FLOAT NOT NULL,
    log_amount FLOAT,
    balance_drain FLOAT,
    is_fraud BOOLEAN,
    is_flagged_fraud BOOLEAN,
    old_balance_sender FLOAT,
    new_balance_sender FLOAT,
    old_balance_receiver FLOAT,
    new_balance_receiver FLOAT,
    FOREIGN KEY (transaction_type_id) REFERENCES dim_transaction_type(transaction_type_id),
    FOREIGN KEY (step) REFERENCES dim_time(step),
    FOREIGN KEY (sender_account_id) REFERENCES dim_account(account_id),
    FOREIGN KEY (receiver_account_id) REFERENCES dim_account(account_id)
);