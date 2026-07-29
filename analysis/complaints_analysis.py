import os
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "finflow.duckdb")
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)


def load_complaints_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = """
        SELECT
            complaint_id,
            product,
            issue,
            resolution
        FROM complaints
    """
    df = conn.execute(query).df()
    conn.close()
    return df


def plot_monthly_complaints(df: pd.DataFrame) -> None:
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = """
        WITH complaint_series AS (
            SELECT
                product,
                CAST(strftime(date_received, '%Y-%m') AS VARCHAR) AS month
            FROM complaints
            WHERE product IN ('Credit card', 'Checking or savings account')
        )
        SELECT
            product,
            month,
            COUNT(*) AS complaint_count
        FROM complaint_series
        GROUP BY product, month
        ORDER BY product, month
    """
    plot_df = conn.execute(query).df()
    conn.close()

    plot_df["month"] = pd.to_datetime(plot_df["month"])

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 6))

    for product in ["Credit card", "Checking or savings account"]:
        subset = plot_df[plot_df["product"] == product]
        ax.plot(
            subset["month"],
            subset["complaint_count"],
            marker="o",
            linewidth=2,
            label=product,
        )

    ax.set_title("Monthly Complaint Volume: Credit Card vs Checking/Savings")
    ax.set_xlabel("Month")
    ax.set_ylabel("Complaint Count")
    ax.legend()
    plt.tight_layout()

    output_path = os.path.join(REPORTS_DIR, "monthly_complaints.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved monthly complaint chart to {output_path}")


def top_issues_by_product(df: pd.DataFrame) -> pd.DataFrame:
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = """
        WITH grouped AS (
            SELECT
                product,
                issue,
                COUNT(*) AS issue_count
            FROM complaints
            GROUP BY product, issue
        ),
        ranked AS (
            SELECT
                product,
                issue,
                issue_count,
                RANK() OVER (
                    PARTITION BY product
                    ORDER BY issue_count DESC
                ) AS issue_rank
            FROM grouped
        )
        SELECT
            product,
            issue,
            issue_count,
            issue_rank
        FROM ranked
        WHERE issue_rank <= 5
        ORDER BY product, issue_rank
    """
    result = conn.execute(query).df()
    conn.close()
    return result


def print_top_issues(top_issues: pd.DataFrame) -> None:
    print("\nTop 5 issues by product")
    print("=" * 90)
    print(top_issues.to_string(index=False, float_format=lambda x: f"{x:,.0f}"))


def main():
    df = load_complaints_data()
    plot_monthly_complaints(df)
    top_issues = top_issues_by_product(df)
    print_top_issues(top_issues)


if __name__ == "__main__":
    main()
