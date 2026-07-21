CREATE TABLE IF NOT EXISTS dim_transaction_type(
    transaction_type_id INT PRIMARY KEY,
    transaction_type_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_account_type(
    account_type_id INT PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_account(
    account_id INT PRIMARY KEY,
    account_name VARCHAR(100),
    account_type_id INT REFERENCES dim_account_type(account_type_id)
);

CREATE TABLE IF NOT EXISTS dim_time(
    step INT PRIMARY KEY,
    sim_day INT ,
    sim_week INT ,
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
    resolution VARCHAR (255) NOT NULL

); 


CREATE TABLE IF NOT EXISTS fact_transactions(
    transaction_id INT PRIMARY KEY,

    --Foreign keys:
    transaction_type_id INT NOT NULL REFERENCES dim_transaction_type(transaction_type_id),
    step INT NOT NULL REFERENCES dim_time(step),
    sender_account_id INT NOT NULL REFERENCES dim_account(account_id),
    receiver_account_id INT NOT NULL REFERENCES dim_account(account_id),

    amount FLOAT NOT NULL,
    log_amount FLOAT,
    balance_drain FLOAT,
    is_fraud BOOLEAN,
    is_flagged_fraud BOOLEAN,
    old_balance_sender FLOAT,
    new_balance_sender FLOAT,
    old_balance_receiver FLOAT,
    new_balance_receiver FLOAT

);  
