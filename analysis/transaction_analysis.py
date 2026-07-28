import os
import time
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor

# Ensure absolute path to finflow.duckdb in project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "finflow.duckdb")

print(f"Connecting to database at: {DB_PATH}")

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


def main():
    queries = [query_daily_volume, query_daily_fraud, query_daily_mean_amount]

    print("Executing sequential benchmark...")
    seq_start = time.perf_counter()
    seq_results = [query() for query in queries]
    seq_end = time.perf_counter()
    seq_duration = seq_end - seq_start

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
    print(f"Daily Volume Rows      : {len(seq_results[0])}")
    print(f"Daily Fraud Rows       : {len(seq_results[1])}")
    print(f"Daily Mean Amount Rows : {len(seq_results[2])}")


    plot_time_series(par_results)
if __name__ == "__main__":
    main()