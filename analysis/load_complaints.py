import os
import duckdb
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "finflow.duckdb")
PARQUET_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "complaints.parquet")


def create_table_if_needed(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS complaints (
            complaint_id INT PRIMARY KEY,
            date_received DATE,
            product VARCHAR(100) NOT NULL,
            sub_product VARCHAR(100),
            issue VARCHAR(255) NOT NULL,
            company VARCHAR(100) NOT NULL,
            state CHAR(2),
            resolution VARCHAR(255) NOT NULL
        )
        """
    )


def _find_column(columns, candidates):
    normalized = {col.strip().lower().replace(" ", "_"): col for col in columns}
    for candidate in candidates:
        key = candidate.strip().lower().replace(" ", "_")
        if key in normalized:
            return normalized[key]
    return None


def load_complaints() -> None:
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(f"Parquet file not found: {PARQUET_PATH}")

    conn = duckdb.connect(DB_PATH)
    try:
        create_table_if_needed(conn)
        conn.execute("DELETE FROM complaints")

        source_df = conn.execute("SELECT * FROM read_parquet(?)", [PARQUET_PATH]).df()
        complaint_id_col = _find_column(source_df.columns, ["complaint id", "complaint_id", "complaintid", "id"])
        date_col = _find_column(source_df.columns, ["date received", "date_received", "date"])
        product_col = _find_column(source_df.columns, ["product"])
        sub_product_col = _find_column(source_df.columns, ["sub-product", "sub_product", "sub product"])
        issue_col = _find_column(source_df.columns, ["issue"])
        company_col = _find_column(source_df.columns, ["company"])
        state_col = _find_column(source_df.columns, ["state"])
        resolution_col = _find_column(source_df.columns, ["company response to consumer", "company_response_to_consumer", "company response", "resolution"])

        missing = [name for name, col in {
            "complaint_id": complaint_id_col,
            "date_received": date_col,
            "product": product_col,
            "sub_product": sub_product_col,
            "issue": issue_col,
            "company": company_col,
            "state": state_col,
            "resolution": resolution_col,
        }.items() if col is None]
        if missing:
            raise ValueError(f"Could not find expected parquet columns for: {', '.join(missing)}")

        mapped_df = source_df[[complaint_id_col, date_col, product_col, sub_product_col, issue_col, company_col, state_col, resolution_col]].copy()
        mapped_df.columns = ["complaint_id", "date_received", "product", "sub_product", "issue", "company", "state", "resolution"]

        mapped_df["complaint_id"] = pd.to_numeric(mapped_df["complaint_id"], errors="coerce").astype("Int64")
        mapped_df["date_received"] = pd.to_datetime(mapped_df["date_received"], errors="coerce").dt.date
        mapped_df["product"] = mapped_df["product"].fillna("").astype(str)
        mapped_df["sub_product"] = mapped_df["sub_product"].fillna("").astype(str)
        mapped_df["issue"] = mapped_df["issue"].fillna("").astype(str)
        mapped_df["company"] = mapped_df["company"].fillna("").astype(str)
        mapped_df["state"] = mapped_df["state"].fillna("").astype(str)
        mapped_df["resolution"] = mapped_df["resolution"].fillna("").astype(str)

        conn.register("complaints_stage", mapped_df)
        conn.execute(
            """
            INSERT INTO complaints (
                complaint_id,
                date_received,
                product,
                sub_product,
                issue,
                company,
                state,
                resolution
            )
            SELECT
                complaint_id,
                date_received,
                product,
                sub_product,
                issue,
                company,
                state,
                resolution
            FROM complaints_stage
            """
        )
        row_count = conn.execute("SELECT COUNT(*) FROM complaints").fetchone()[0]
        print(f"Loaded {row_count} complaints into the complaints table.")
    finally:
        conn.close()


if __name__ == "__main__":
    load_complaints()
