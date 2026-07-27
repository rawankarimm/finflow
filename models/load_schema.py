import argparse
import os
import time
from concurrent.futures import ProcessPoolExecutor
import duckdb as dd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# --- Configuration & Paths ---
# Adjust imports or paths if needed based on your setup
try:
    from config.package.settings import PipelineConfig

    DB_PATH = getattr(PipelineConfig, "DB_PATH", "data/finflow.duckdb")
    SCHEMA_PATH = getattr(PipelineConfig, "SCHEMA_PATH", "models/schema.sql")
    PARQUET_PATH = getattr(
        PipelineConfig,
        "FACT_PARQUET_PATH",
        "data/processed/transactions_transformed.parquet",
    )
    CHUNK_SIZE = getattr(PipelineConfig, "CHUNK_SIZE", 2_000_000)
except ImportError:
    DB_PATH = "data/finflow.duckdb"
    SCHEMA_PATH = "models/schema.sql"
    PARQUET_PATH = "data/processed/transactions_transformed.parquet"
    CHUNK_SIZE = 100_000


def init_database(db_path: str, schema_path: str, reload_flag: bool = False):
    """Creates database directory and applies schema DDL."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    conn = dd.connect(db_path)

    if reload_flag:
        print("[INFO] Reload flag passed. Truncating fact_transactions...")
        conn.execute("DROP TABLE IF EXISTS fact_transactions;")

    # Apply DDL schema
    conn.execute(schema_sql)
    conn.close()


def load_dimension_tables(db_path: str):
    """Loads dimension tables before loading fact tables."""
    conn = dd.connect(db_path)
    # Dimension loading logic (if any DML script/seed files exist)
    conn.close()


def process_chunk_worker(parquet_path: str, offset: int, chunk_size: int):
    """Worker task executed in ProcessPoolExecutor.

    Reads a slice of the transformed Parquet file and returns the PyArrow
    Table. Does NOT open DB connection directly to prevent Windows OS file lock
    conflicts.
    """
    dataset = ds.dataset(parquet_path, format="parquet")
    arrow_table = dataset.to_table().slice(offset, chunk_size)
    return arrow_table


def load_fact_transactions_parallel(
    db_path: str, parquet_path: str, chunk_size: int
):
    """Processes chunks in parallel using ProcessPoolExecutor and streams them

    into DuckDB sequentially in the main process.
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(
            f"Parquet file not found at '{parquet_path}'. "
            "Please ensure the transformation step ran and generated the output file."
        )

    parquet_file = pq.ParquetFile(parquet_path)
    total_rows = parquet_file.metadata.num_rows
    offsets = list(range(0, total_rows, chunk_size))

    print(
        f"[INFO] Loading {total_rows:,} rows across {len(offsets)} chunks using ProcessPoolExecutor..."
    )

    conn = dd.connect(db_path)

    # Spawn process pool for parallel chunk extraction
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(
                process_chunk_worker, parquet_path, offset, chunk_size
            )
            for offset in offsets
        ]

        for future in futures:
            
            chunk_table = future.result()


            # Register PyArrow chunk table in DuckDB memory context
            conn.register("temp_chunk", chunk_table)

        
            #Insert using explicit column mapping or standard INSERT OR IGNORE
            # Before loading, make sure the sequence exists in DuckDB
    conn.execute("CREATE SEQUENCE IF NOT EXISTS transaction_seq START 1;")

    

    # Execute the insert inside your chunk loop
    conn.execute(
    """
        INSERT INTO fact_transactions (
        transaction_id,
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
        nextval('transaction_seq') AS transaction_id,
        dt.transaction_type_id,
        tc.step,
        sa.account_id AS sender_account_id,
        ra.account_id AS receiver_account_id,
        tc.amount,
        tc.log_amount,
        tc.balance_drain,
        CAST(tc.is_fraud AS BOOLEAN) AS is_fraud,
        CAST(tc.is_flagged_fraud AS BOOLEAN) AS is_flagged_fraud,
        tc.oldbalance_org AS old_balance_sender,
        tc.newbalance_orig AS new_balance_sender,
        tc.oldbalance_dest AS old_balance_receiver,
        tc.newbalance_dest AS new_balance_receiver
        FROM temp_chunk tc
        LEFT JOIN dim_transaction_type dt ON UPPER(TRIM(tc.type)) = UPPER(TRIM(dt.transaction_type_name))
        LEFT JOIN dim_account sa ON tc.name_orig = sa.account_name
        LEFT JOIN dim_account ra ON tc.name_dest = ra.account_name
       
"""
)
            
    conn.unregister("temp_chunk")

    conn.close()






def verify_row_count(db_path: str, parquet_path: str):
    """Verifies DuckDB row count against the source Parquet file metadata."""
    parquet_file = pq.ParquetFile(parquet_path)
    expected_rows = parquet_file.metadata.num_rows

    conn = dd.connect(db_path)
    actual_rows = conn.execute(
        "SELECT COUNT(*) FROM fact_transactions"
    ).fetchone()[0]
    conn.close()

    print(f"[VERIFY] Source Parquet Rows: {expected_rows:,}")
    print(f"[VERIFY] DuckDB Loaded Rows:  {actual_rows:,}")

    assert (
        actual_rows == expected_rows
    ), f"Mismatch detected! Expected {expected_rows} rows, found {actual_rows}"
    print("[SUCCESS] Row counts match perfectly.")


def main():
    parser = argparse.ArgumentParser(
        description="Load transformed data into DuckDB with parallel chunk processing."
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Reset/drop fact tables before loading.",
    )
    args = parser.parse_args()

    start_time = time.time()

    # 1. Initialize schema & DB
    print("[STEP 1/4] Initializing Database & Schema...")
    init_database(DB_PATH, SCHEMA_PATH, reload_flag=args.reload)

    # 2. Load dimension tables
    print("[STEP 2/4] Loading Dimension Tables...")
    load_dimension_tables(DB_PATH)

    # 3. Load fact_transactions in chunks
    print("[STEP 3/4] Ingesting Fact Transactions in Parallel...")
    load_fact_transactions_parallel(DB_PATH, PARQUET_PATH, CHUNK_SIZE)

    # # 4. Verify row count
    # print("[STEP 4/4] Verifying Row Counts...")
    # verify_row_count(DB_PATH, PARQUET_PATH)

    elapsed_time = time.time() - start_time
    print(f"\n[COMPLETE] Total load time: {elapsed_time:.2f} seconds")


# CRITICAL: Ensures Windows child processes do not re-execute script code on spawn
if __name__ == "__main__":
    main()