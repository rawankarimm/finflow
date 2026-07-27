import os
import time
import duckdb
import pyarrow.parquet as pq  #loading the parquet file lazily in batches instead of loading the whole dataset to RAM at once 
import config

DB_PATH = getattr(config, "db_path", "data/finflow.duckdb")
SCHEMA_SQL_PATH = "models/schema.sql"
PROCESSED_TRANSACTIONS = "data/processed/transactions.parquet"

def main():
    start_time = time.perf_counter()
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    # 1. Reset Schema for Idempotency
    conn = duckdb.connect(DB_PATH)
    
    # Restrict DuckDB internal RAM usage
    conn.execute("SET memory_limit='1GB';")

    print("Resetting schema...")

    # DROP TABLE to guarantee idempotency on re-running the script
    conn.execute("""
        DROP TABLE IF EXISTS fact_transactions;
        DROP TABLE IF EXISTS complaints;
        DROP TABLE IF EXISTS dim_account;
        DROP TABLE IF EXISTS dim_account_type;
        DROP TABLE IF EXISTS dim_transaction_type;
        DROP TABLE IF EXISTS dim_time;
        DROP SEQUENCE IF EXISTS transaction_seq;
    """)

    # Executes DDL commands to recreate empty fact and dimention tables cleanly
    with open(SCHEMA_SQL_PATH, "r") as f:
        conn.execute(f.read())

    # 2. Populate dimension tables first in order to join their primary keys when inserting rows in the fact table 
    print("Populating dimension tables...")
    
    conn.execute("""
        INSERT INTO dim_transaction_type (transaction_type_id, transaction_type_name)
        SELECT ROW_NUMBER() OVER (), type FROM (SELECT DISTINCT type FROM read_parquet(?));
    """, [PROCESSED_TRANSACTIONS])  #auto generating a unique ID for each type


    # Populate account types for lookup table 
    conn.execute("""
        INSERT INTO dim_account_type VALUES (1, 'Customer'), (2, 'Merchant') ON CONFLICT DO NOTHING;
    """)
    
    conn.execute("""
        INSERT INTO dim_account (account_id, account_name, account_type_id)
        SELECT ROW_NUMBER() OVER (), account_name, CASE WHEN account_name LIKE 'M%' THEN 2 ELSE 1 END
        FROM (SELECT name_orig as account_name FROM read_parquet(?) UNION SELECT name_dest FROM read_parquet(?));
    """, [PROCESSED_TRANSACTIONS, PROCESSED_TRANSACTIONS])
    
    conn.execute("""
        INSERT INTO dim_time (step, sim_day, sim_week, hour_of_day)
        SELECT DISTINCT step, CAST(FLOOR((step - 1) / 24) + 1 AS INT), CAST(FLOOR((step - 1) / 168) + 1 AS INT), CAST((step - 1) % 24 AS INT)
        FROM read_parquet(?);
    """, [PROCESSED_TRANSACTIONS])

    # 3. Stream Fact Table in Chunks (RAM Safe)
    chunk_size = getattr(config, "chunk_size", 500000)
    print(f"\n--- Loading Fact Transactions in chunks of {chunk_size:,} ---")
    
    parquet_file = pq.ParquetFile(PROCESSED_TRANSACTIONS)
    
    for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
        conn.execute("""
            INSERT INTO fact_transactions (
                transaction_type_id,
                step,
                sender_account_id,
                receiver_account_id,
                amount,
                log_amount,
                balance_drain,
                is_fraud,
                is_flagged_fraud,
                old_balance_sender,
                new_balance_sender,
                old_balance_receiver,
                new_balance_receiver
            )
            SELECT 
                tt.transaction_type_id,
                t.step,
                sa.account_id as sender_account_id,
                ra.account_id as receiver_account_id,
                t.amount,
                ln(t.amount + 1) as log_amount,
                (t.oldbalance_org - t.newbalance_orig) as balance_drain,
                t.is_fraud,
                t.is_flagged_fraud,
                t.oldbalance_org as old_balance_sender,
                t.newbalance_orig as new_balance_sender,
                t.oldbalance_dest as old_balance_receiver,
                t.newbalance_dest as new_balance_receiver
            FROM batch t
            JOIN dim_transaction_type tt ON t.type = tt.transaction_type_name
            JOIN dim_account sa ON t.name_orig = sa.account_name
            JOIN dim_account ra ON t.name_dest = ra.account_name
        """)
        print(f"Processed chunk {i + 1}: {len(batch):,} rows inserted.")

    # 4. Verify Row Count
    db_row_count = conn.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
    parquet_row_count = parquet_file.metadata.num_rows

    print(f"\nSource Parquet Rows : {parquet_row_count:,}")
    print(f"DuckDB Loaded Rows  : {db_row_count:,}")

    if db_row_count == parquet_row_count:
        print("SUCCESS: Row counts match!")
    else:
        print(f"MISMATCH: Loaded {db_row_count} rows out of {parquet_row_count} expected rows.")

    conn.close()

    print(f"Total load time: {time.perf_counter() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()




# DuckDB is an embedded database (like SQLite). If multiple processes spawned by
# ProcessPoolExecutor try to execute INSERT statements into the same finflow.
# duckdb file simultaneously, DuckDB throws a lock exception (IO Error: Could not set lock on file)