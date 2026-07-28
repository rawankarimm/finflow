import duckdb
from config.package import logger
import os

class DataQualityError(Exception):
    pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "finflow.duckdb")

conn = duckdb.connect(DB_PATH, read_only=True)

def check():
        # 1- checking for duplicate ids
        duplicates = conn.execute("""
        SELECT transaction_id
        FROM fact_transactions
        GROUP BY transaction_id
        HAVING COUNT(*) > 1 
        """).fetchall()
        
        if len(duplicates) > 0:
            raise DataQualityError (f'{len(duplicates)} found!')
        else:
             logger.info('No duplicates found')


        # 2- Null-fraud rate check
        null_fraud = conn.execute("""
            SELECT 
            COUNT(*) 
            FROM fact_transactions
            WHERE is_fraud IS NULL
        """).fetchone()[0]
        # count(*) results in only one row representing a tuple including an integer representing the number of null values
        # fetchone() reads this only row 
        # [0] extracts the integer in the tuple 

        if null_fraud > 0:
             raise DataQualityError (f'{null_fraud} NULL values found!')


        # 3. Check foreign key matches in dimension tables
        foreign_keys = [
        ("transaction_type_id", "dim_transaction_type", "transaction_type_id"),
        ("step", "dim_time", "step"),
        ("sender_account_id", "dim_account", "account_id"),
        ("receiver_account_id", "dim_account", "account_id"),
        ]

        for fk_col, dim_table, dim_col in foreign_keys:
            unmatched = conn.execute(f"""
                SELECT COUNT(*)
                FROM fact_transactions ft
                LEFT JOIN {dim_table} dt ON ft.{fk_col} = dt.{dim_col}
                WHERE dt.{dim_col} IS NULL
            """).fetchone()[0]

            if unmatched > 0:
                raise DataQualityError(f"Foreign key '{fk_col}' has {unmatched} unmatched rows in '{dim_table}'")

        # 4. Check for negative amounts
        negative_amounts = conn.execute("""
            SELECT COUNT(*)
            FROM fact_transactions
            WHERE amount < 0
        """).fetchone()[0]

        if negative_amounts > 0:
            raise DataQualityError(f"Found {negative_amounts} negative amounts")

        print("All data quality checks passed!")

if __name__ == "__main__":
    check()