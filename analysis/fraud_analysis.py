import os
import duckdb
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "finflow.duckdb")


def load_summary_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = """
        SELECT
            ft.is_fraud,
            dtt.transaction_type_name AS transaction_type,
            ft.amount,
            ft.new_balance_sender AS new_balance_orig,
            ft.new_balance_receiver AS new_balance_dest
        FROM fact_transactions ft
        LEFT JOIN dim_transaction_type dtt
            ON ft.transaction_type_id = dtt.transaction_type_id
    """
    df = conn.execute(query).df()
    conn.close()
    return df


def load_balance_drain_conditionals() -> pd.DataFrame:
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = """
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
        FROM abs_balance_drain
    """
    df = conn.execute(query).df()
    conn.close()
    return df


def build_fraud_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "metric",
            "value",
            "group"
        ])

    overall_fraud_rate = df["is_fraud"].mean() * 100

    type_rates = (
        df.groupby("transaction_type")["is_fraud"]
        .mean()
        .mul(100)
        .rename("p_fraud_given_type")
        .reset_index()
    )

    fraud_stats = (
        df.groupby("is_fraud")["amount"]
        .agg(["mean", "median", lambda s: s.quantile(0.95)])
        .rename(columns={"<lambda_0>": "p95", "mean": "mean_amount", "median": "median_amount"})
        .reset_index()
    )
    fraud_stats["is_fraud"] = fraud_stats["is_fraud"].map({True: "fraud", False: "non_fraud"})

    fraud_zero_orig = (
        df.loc[df["is_fraud"] == True, "new_balance_orig"] == 0
    ).mean() * 100
    fraud_zero_dest = (
        df.loc[df["is_fraud"] == True, "new_balance_dest"] == 0
    ).mean() * 100

    summary_rows = [
        {"metric": "overall_fraud_rate", "value": overall_fraud_rate, "group": "overall"},
        {"metric": "mean_amount_fraud", "value": fraud_stats.loc[fraud_stats["is_fraud"] == "fraud", "mean_amount"].iloc[0], "group": "fraud"},
        {"metric": "median_amount_fraud", "value": fraud_stats.loc[fraud_stats["is_fraud"] == "fraud", "median_amount"].iloc[0], "group": "fraud"},
        {"metric": "p95_amount_fraud", "value": fraud_stats.loc[fraud_stats["is_fraud"] == "fraud", "p95"].iloc[0], "group": "fraud"},
        {"metric": "mean_amount_non_fraud", "value": fraud_stats.loc[fraud_stats["is_fraud"] == "non_fraud", "mean_amount"].iloc[0], "group": "non_fraud"},
        {"metric": "median_amount_non_fraud", "value": fraud_stats.loc[fraud_stats["is_fraud"] == "non_fraud", "median_amount"].iloc[0], "group": "non_fraud"},
        {"metric": "p95_amount_non_fraud", "value": fraud_stats.loc[fraud_stats["is_fraud"] == "non_fraud", "p95"].iloc[0], "group": "non_fraud"},
        {"metric": "pct_fraud_new_balance_orig_zero", "value": fraud_zero_orig, "group": "fraud"},
        {"metric": "pct_fraud_new_balance_dest_zero", "value": fraud_zero_dest, "group": "fraud"},
    ]

    summary_df = pd.DataFrame(summary_rows)
    summary_df["value"] = summary_df["value"].astype(float)

    if not type_rates.empty:
        type_rows = []
        for _, row in type_rates.iterrows():
            type_rows.append({
                "metric": "p_fraud_given_type",
                "value": row["p_fraud_given_type"],
                "group": row["transaction_type"],
            })
        summary_df = pd.concat([summary_df, pd.DataFrame(type_rows)], ignore_index=True)

    return summary_df


def print_summary(summary_df: pd.DataFrame) -> None:
    print("\nFraud Analysis Summary")
    print("=" * 90)
    print(summary_df.to_string(index=False, float_format=lambda x: f"{x:,.4f}"))


def main():
    df = load_summary_data()
    summary_df = build_fraud_summary(df)
    print_summary(summary_df)

    conditional_df = load_balance_drain_conditionals()
    print("\nConditional Fraud Probability by Balance-Drain Threshold")
    print("=" * 90)
    print(conditional_df.to_string(index=False, float_format=lambda x: f"{x:,.4f}"))


if __name__ == "__main__":
    main()
