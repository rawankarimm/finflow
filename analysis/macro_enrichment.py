import os
import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from scipy.stats import linregress, pearsonr, spearmanr

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "finflow.duckdb")
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)


def load_macro_inputs():
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = """
        WITH monthly_transactions AS (
            SELECT
                dt.sim_day,
                dt.step,
                ft.transaction_type_id,
                dtt.transaction_type_name,
                COUNT(*) AS transaction_count,
                SUM(ft.amount) AS total_amount
            FROM fact_transactions ft
            JOIN dim_time dt ON ft.step = dt.step
            LEFT JOIN dim_transaction_type dtt ON ft.transaction_type_id = dtt.transaction_type_id
            GROUP BY dt.sim_day, dt.step, ft.transaction_type_id, dtt.transaction_type_name
        ),
        monthly_series AS (
            SELECT
                s.step,
                CASE
                    WHEN s.step IS NULL THEN NULL
                    ELSE DATE '2019-01-01' + INTERVAL (s.step - 1) MONTH
                END AS month_start,
                s.sim_day
            FROM (
                SELECT DISTINCT ft.step, dt.sim_day
                FROM fact_transactions ft
                JOIN dim_time dt ON ft.step = dt.step
            ) s
        )
        SELECT
            ms.month_start AS month,
            mt.transaction_type_name,
            mt.transaction_count,
            mt.total_amount
        FROM monthly_series ms
        LEFT JOIN monthly_transactions mt ON mt.step = ms.step
        ORDER BY ms.month_start, mt.transaction_type_name
    """
    df = conn.execute(query).df()
    conn.close()
    return df


def load_fred_series():
    macro_dir = os.path.join(PROJECT_ROOT, "data", "raw", "macro")

    def read_macro(path, value_col):
        df = pd.read_csv(path)
        date_col = None
        for candidate in ["DATE", "date", "observation_date"]:
            if candidate in df.columns:
                date_col = candidate
                break
        if date_col is None:
            if df.shape[1] >= 2:
                date_col = df.columns[0]
            else:
                raise ValueError(f"Could not find a date column in {path}")

        df = df.rename(columns={date_col: "date"})
        if value_col not in df.columns:
            aliases = {
                "unemployment_rate": ["UNRATE", "UNEMPLOY", "UNEMPLOYMENT", "unrate"],
                "cpi": ["CPIAUCSL", "CPI", "cpi", "cpi_aucsl"],
                "usd_eur": ["DEXUSEU", "USD_EUR", "EXUSEU", "dexuseu"],
            }
            for candidate in aliases.get(value_col, []) + [value_col.upper(), value_col.lower(), value_col.capitalize()]:
                if candidate in df.columns:
                    df = df.rename(columns={candidate: value_col})
                    break
        if value_col not in df.columns:
            raise ValueError(f"Could not find value column '{value_col}' in {path}")

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date", value_col]).copy()
        return df

    unemp = read_macro(os.path.join(macro_dir, "UNRATE.csv"), "unemployment_rate")
    cpi = read_macro(os.path.join(macro_dir, "CPIAUCSL.csv"), "cpi")
    fx = read_macro(os.path.join(macro_dir, "DEXUSEU.csv"), "usd_eur")

    return {"unemployment_rate": unemp, "cpi": cpi, "usd_eur": fx}


def prepare_case_data():
    transaction_df = load_macro_inputs()
    macro_dfs = load_fred_series()

    transaction_df["month"] = pd.to_datetime(transaction_df["month"])

    # Aggregate monthly cash-out volume and transfer amount by month
    case1_df = (
        transaction_df[transaction_df["transaction_type_name"] == "CASH_OUT"]
        .groupby("month", as_index=False)
        .agg(cash_out_volume=("transaction_count", "sum"))
    )
    case1_df = case1_df.sort_values("month")

    case2_df = (
        transaction_df[transaction_df["transaction_type_name"] == "TRANSFER"]
        .groupby("month", as_index=False)
        .agg(transfer_amount=("total_amount", "sum"))
    )
    case2_df = case2_df.sort_values("month")

    unemp = macro_dfs["unemployment_rate"].copy()
    fx = macro_dfs["usd_eur"].copy()
    unemp["month"] = unemp["date"].dt.to_period("M").dt.to_timestamp()
    fx["month"] = fx["date"].dt.to_period("M").dt.to_timestamp()

    case1 = case1_df.merge(unemp[["month", "unemployment_rate"]], on="month", how="inner")
    case2 = case2_df.merge(fx[["month", "usd_eur"]], on="month", how="inner")

    case1 = case1.dropna()
    case2 = case2.dropna()

    return case1, case2


def summarize_relationship(x, y, label):
    corr_p = pearsonr(x, y)[0]
    corr_s = spearmanr(x, y)[0]
    ols = linregress(x, y)
    return {
        "label": label,
        "pearson": corr_p,
        "spearman": corr_s,
        "ols_slope": ols.slope,
        "ols_intercept": ols.intercept,
        "ols_rvalue": ols.rvalue,
        "ols_pvalue": ols.pvalue,
    }


def plot_case(case_df, x_col, y_col, title, xlabel, ylabel, output_name):
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 8), sharex=True)

    axes[0].plot(case_df["month"], case_df[x_col], color="steelblue", marker="o", linewidth=2)
    axes[0].set_title(f"{title} - {xlabel}")
    axes[0].set_ylabel(xlabel)

    axes[1].plot(case_df["month"], case_df[y_col], color="crimson", marker="s", linewidth=2)
    axes[1].set_title(f"{title} - {ylabel}")
    axes[1].set_ylabel(ylabel)
    axes[1].set_xlabel("Month")

    plt.tight_layout()
    fig.savefig(os.path.join(REPORTS_DIR, output_name), dpi=300, bbox_inches="tight")
    plt.close(fig)


def print_summary(summary):
    print("\nMacro-enrichment analysis summary")
    print("=" * 100)
    for item in summary:
        print(f"Case: {item['label']}")
        print(f"  Pearson r: {item['pearson']:.4f}")
        print(f"  Spearman rho: {item['spearman']:.4f}")
        print(f"  OLS slope: {item['ols_slope']:.4f}")
        print(f"  OLS intercept: {item['ols_intercept']:.4f}")
        print(f"  OLS R^2: {item['ols_rvalue'] ** 2:.4f}")
        print(f"  OLS p-value: {item['ols_pvalue']:.4f}")
        print()


def interpret_case(label, case_df, x_col, y_col):
    result = summarize_relationship(case_df[x_col], case_df[y_col], label)

    if label == "Case 1: unemployment vs cash-out volume":
        theory = (
            "A positive relationship may reflect the standard consumption-smoothing channel: when unemployment rises, "
            "households cut back on discretionary spending and draw down liquid savings, which can increase cash withdrawals. "
            "However, this link is not strictly causal unless the data show a stable lag and the relationship survives after "
            "controlling for other macro factors such as income growth, credit conditions, and interest rates."
        )
    else:
        theory = (
            "A correlation between FX volatility and transfer amounts could reflect cross-border remittance and trade-related demand, "
            "but it may also be spurious if both series are driven by a common trend such as global risk sentiment or inflation. "
            "In macroeconomics, exchange-rate movements often influence remittances through transaction costs and incentives, "
            "yet causal interpretation requires a plausible transmission mechanism and evidence that the effect is not just a shared trend."
        )

    print(f"\n{label}")
    print("-" * 70)
    print(f"Pearson r: {result['pearson']:.4f}")
    print(f"Spearman rho: {result['spearman']:.4f}")
    print(f"OLS slope: {result['ols_slope']:.4f}; R^2: {result['ols_rvalue'] ** 2:.4f}")
    print("Interpretation:")
    print(theory)


def main():
    case1_df, case2_df = prepare_case_data()

    plot_case(case1_df, "unemployment_rate", "cash_out_volume", "Case 1", "Unemployment Rate", "CASH_OUT Volume", "case1_macro_vs_cashout.png")
    plot_case(case2_df, "usd_eur", "transfer_amount", "Case 2", "USD/EUR Exchange Rate", "TRANSFER Amount", "case2_macro_vs_transfer.png")

    summary = [
        summarize_relationship(case1_df["unemployment_rate"], case1_df["cash_out_volume"], "Case 1: unemployment vs cash-out volume"),
        summarize_relationship(case2_df["usd_eur"], case2_df["transfer_amount"], "Case 2: usd/eur vs transfer amount"),
    ]
    print_summary(summary)

    interpret_case("Case 1: unemployment vs cash-out volume", case1_df, "unemployment_rate", "cash_out_volume")
    interpret_case("Case 2: usd/eur vs transfer amount", case2_df, "usd_eur", "transfer_amount")


if __name__ == "__main__":
    main()
