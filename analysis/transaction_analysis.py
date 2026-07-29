import os
import time
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from concurrent.futures import ThreadPoolExecutor

# Ensure absolute path to finflow.duckdb in project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "finflow.duckdb")

print(f"Connecting to database at: {DB_PATH}") #checking path 

# 1.Time Series
def query_daily_volume():
    conn = duckdb.connect(DB_PATH, read_only=True)
    result = conn.execute("""
        SELECT 
            dt.sim_day, 
            ft.transaction_type_id, 
            COUNT(*) AS volume
        FROM fact_transactions ft
        JOIN dim_time dt ON ft.step = dt.step
        GROUP BY ft.transaction_type_id, dt.sim_day
    """).df()
    conn.close()
    return result


def query_daily_fraud():
    conn = duckdb.connect(DB_PATH, read_only=True)
    result = conn.execute("""
        SELECT 
            dt.sim_day, 
            COUNT(*) AS fraud_count
        FROM fact_transactions ft
        JOIN dim_time dt ON ft.step = dt.step
        WHERE ft.is_fraud = TRUE
        GROUP BY dt.sim_day
    """).df()
    conn.close()
    return result


def query_daily_mean_amount():
    conn = duckdb.connect(DB_PATH, read_only=True)
    result = conn.execute("""
        SELECT 
            dt.sim_day, 
            ft.transaction_type_id, 
            AVG(ft.amount) AS mean_amount
        FROM fact_transactions ft
        JOIN dim_time dt ON ft.step = dt.step
        GROUP BY dt.sim_day, ft.transaction_type_id
    """).df()
    conn.close()
    return result


def plot_time_series(par_results):
    df_volume, df_fraud, df_mean_amount = par_results

    # Ensure output directory relative to project root
    reports_dir = os.path.join(PROJECT_ROOT, "reports")
    os.makedirs(reports_dir, exist_ok=True)

    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(12, 14), sharex=True)
    sns.set_theme(style="whitegrid")

    # PANEL 1: Daily Volume
    sns.lineplot(
        data=df_volume, 
        x="sim_day", 
        y="volume", 
        hue="transaction_type_id", 
        marker="o", 
        ax=axes[0]
    )
    axes[0].set_title("Daily Transaction Volume by Type", fontsize=14, fontweight="bold")
    axes[0].set_ylabel("Transaction Count")
    axes[0].legend(title="Type", bbox_to_anchor=(1.02, 1), loc="upper left")

    # PANEL 2: Daily Fraud Count
    sns.lineplot(
        data=df_fraud, 
        x="sim_day", 
        y="fraud_count", 
        color="crimson", 
        linewidth=2, 
        marker="s", 
        ax=axes[1]
    )
    axes[1].set_title("Daily Fraud Count", fontsize=14, fontweight="bold")
    axes[1].set_ylabel("Fraud Count")

    # PANEL 3: Daily Mean Amount
    sns.lineplot(
        data=df_mean_amount, 
        x="sim_day", 
        y="mean_amount", 
        hue="transaction_type_id", 
        marker="d", 
        ax=axes[2]
    )
    axes[2].set_title("Daily Mean Amount by Type ($)", fontsize=14, fontweight="bold")
    axes[2].set_xlabel("Simulation Day")
    axes[2].set_ylabel("Mean Amount ($)")
    axes[2].legend(title="Type", bbox_to_anchor=(1.02, 1), loc="upper left")

    plt.tight_layout()

    output_path = os.path.join(reports_dir, "time_series_analysis.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"\nChart successfully saved to {output_path}")

# 2.Amount Distribution

def query_amount_distribution():
    conn = duckdb.connect(DB_PATH, read_only=True)
    result = conn.execute("""
        SELECT
            dtt.transaction_type_name AS transaction_type,
            ft.amount
        FROM fact_transactions ft
        JOIN dim_transaction_type dtt
            ON ft.transaction_type_id = dtt.transaction_type_id
        WHERE dtt.transaction_type_name IN ('TRANSFER', 'CASH_OUT')
    """).df()
    conn.close()
    return result


def _normal_pdf(x, mu, sigma):
    if sigma <= 0:
        return np.zeros_like(x, dtype=float)
    z = (x - mu) / sigma
    return np.exp(-0.5 * z**2) / (sigma * np.sqrt(2 * np.pi))


def plot_amount_distribution():
    df = query_amount_distribution()
    df["log_amount_plus_one"] = np.log1p(df["amount"])

    reports_dir = os.path.join(PROJECT_ROOT, "reports")
    os.makedirs(reports_dir, exist_ok=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.set_theme(style="whitegrid")

    for transaction_type, group in df.groupby("transaction_type", sort=True):
        sns.kdeplot(
            data=group,
            x="log_amount_plus_one",
            label=transaction_type,
            fill=True,
            alpha=0.2,
            linewidth=2,
            ax=ax
        )

        mu = float(group["log_amount_plus_one"].mean())
        sigma = float(group["log_amount_plus_one"].std(ddof=0))

        x_vals = np.linspace(
            group["log_amount_plus_one"].min(),
            group["log_amount_plus_one"].max(),
            400
        )
        ax.plot(
            x_vals,
            _normal_pdf(x_vals, mu, sigma),
            linestyle="--",
            linewidth=2,
            label=f"{transaction_type} fitted normal"
        )

    # If the KDE follows the fitted normal curve closely, the empirical distribution is
    # consistent with log-normality on the original amount scale. That matters because
    # fraud-detection thresholds are often set on a log scale, where a near-Gaussian shape
    # makes extreme-value cutoffs more interpretable and statistically stable.


    ax.set_title("Log(Amount + 1) Distribution: TRANSFER vs CASH_OUT")
    ax.set_xlabel("log(amount + 1)")
    ax.set_ylabel("Density")
    ax.legend(title="Transaction Type")
    plt.tight_layout()

    output_path = os.path.join(reports_dir, "amount_distribution.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"\nChart successfully saved to {output_path}")


def query_balance_drain_distribution():
    conn = duckdb.connect(DB_PATH, read_only=True)
    result = conn.execute("""
        SELECT balance_drain
        FROM fact_transactions
        WHERE balance_drain IS NOT NULL
    """).df()
    conn.close()
    return result


def plot_balance_drain_distribution():
    df = query_balance_drain_distribution()
    df = df.dropna(subset=["balance_drain"])

    reports_dir = os.path.join(PROJECT_ROOT, "reports")
    os.makedirs(reports_dir, exist_ok=True)

    _, ax = plt.subplots(figsize=(10, 6))
    sns.set_theme(style="whitegrid")

    sns.histplot(
        data=df,
        x="balance_drain",
        bins=50,
        kde=True,
        color="teal",
        edgecolor="black",
        ax=ax
    )

    inconsistency_pct = (np.abs(df["balance_drain"]) > 1).mean() * 100
    ax.axvline(0, color="black", linestyle="--", linewidth=1)
    ax.text(
        0.98,
        0.95,
        f"% with |balance_drain| > 1: {inconsistency_pct:.2f}%",
        transform=ax.transAxes,
        ha="right",
        va="top",
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8}
    )

    ax.set_title("Balance Drain Distribution")
    ax.set_xlabel("balance_drain")
    ax.set_ylabel("Count")
    plt.tight_layout()

    output_path = os.path.join(reports_dir, "balance_drain_distribution.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"\nBalance drain inconsistency rate (> 1): {inconsistency_pct:.2f}%")
    print(f"Chart successfully saved to {output_path}")


def main():
    queries = [query_daily_volume, query_daily_fraud, query_daily_mean_amount]

    print("Executing sequential benchmark...")
    seq_start = time.perf_counter()
    seq_results = [query() for query in queries]
    seq_end = time.perf_counter()
    seq_duration = seq_end - seq_start

    print("Executing parallel benchmark...")
    par_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=3) as executor:
        # map takes each function from tasks and executes it concurrently
        par_results = list(executor.map(lambda fn: fn(), queries))

    par_end = time.perf_counter()
    par_duration = par_end - par_start

    print(f"Sequential Execution Time: {seq_duration:.4f} seconds")
    print(f"Parallel Execution Time: {par_duration:.4f} seconds")

    # Calculate & Log Speedup
    speedup = seq_duration / par_duration
    print(f"Speedup: {speedup:.2f}x")

    plot_time_series(par_results)
    plot_amount_distribution()
    plot_balance_drain_distribution()


if __name__ == "__main__":
    main()