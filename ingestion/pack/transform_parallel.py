
import os
import time
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from ingestion.pack.ingest_all_sequential import transaction_output_path, processed_dir
from config.package import logger

# Where the transformed dataset gets saved
# processed_dir = os.path.join("data", "processed")
transformed_output_path = os.path.join(processed_dir, "transactions_transformed.parquet")
df = pd.read_parquet(transaction_output_path)

def transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies type coercion + derived columns to a single chunk.
    """
    df = df.copy()

    df["amount"] = df["amount"].astype("float64")
    df["oldbalance_org"] = df["oldbalance_org"].astype("float64")
    df["newbalance_orig"] = df["newbalance_orig"].astype("float64")

    df["balance_drain"] = df["oldbalance_org"] - df["newbalance_orig"] - df["amount"]
    df["log_amount"] = np.log1p(df["amount"])  # log1p handles amount == 0 safely

    return df


def make_chunks(df: pd.DataFrame, chunk_size: int) -> list[pd.DataFrame]:
    """
    Split a DataFrame into chunks of ~chunk_size rows.

    """
    return [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]


def transform_sequential(chunks: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat([transform_chunk(c) for c in chunks], ignore_index=True)


def transform_parallel(chunks: list[pd.DataFrame], n_workers: int) -> pd.DataFrame:
    """Applies transform_chunk across all chunks using a process pool."""
    results = [None] * len(chunks)
    # switching ThreadPoolExecutor and ProcessPoolExecutor leads to much faster results
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        # submit all chunks up front, then collect -- do NOT call
        # future.result() inside the submit loop, that serializes everything
        futures_map = {executor.submit(transform_chunk, c): i for i, c in enumerate(chunks)}

        for future in as_completed(futures_map):
            i = futures_map[future]
            try:
                results[i] = future.result(timeout=60)
            except Exception as e:
                logger.error(f"Chunk {i} failed to transform: {e}")
                raise

    return pd.concat(results, ignore_index=True)


def benchmark_transform(chunk_size: int, n_workers: int):
    print(f"\n>>> Benchmarking chunk_size={chunk_size:,} with n_workers={n_workers}")

    df = pd.read_parquet(transaction_output_path)
    chunks = make_chunks(df, chunk_size)

    seq_start = time.perf_counter()
    transform_sequential(chunks)
    seq_time = time.perf_counter() - seq_start

    par_start = time.perf_counter()
    par_result = transform_parallel(chunks, n_workers)
    par_time = time.perf_counter() - par_start

    speedup = seq_time / par_time if par_time > 0 else 0
    time_saved = seq_time - par_time

    print("-" * 65)
    print(f" {'Method':<25} | {'Execution Time (s)':<20} | {'Status':<10}")
    print("-" * 65)
    print(f" {'Sequential (Baseline)':<25} | {seq_time:<20.4f} | {'Baseline':<10}")
    print(f" {'Parallel (' + str(n_workers) + ' Workers)':<25} | {par_time:<20.4f} | {'SUCCESS':<10}")
    print("-" * 65)
    print(f" Speedup Factor : {speedup:.2f}x faster")
    print(f" Time Saved     : {time_saved:.4f} seconds")

    return {
        "chunk_size": chunk_size,
        "sequential_seconds": round(seq_time, 4),
        "parallel_seconds": round(par_time, 4),
        "speedup": round(speedup, 2),
        "result_df": par_result,
    }


def run_transform_benchmarks():
    print("\n" + "=" * 50)
    print("   STARTING TRANSFORM BENCHMARK")
    print("=" * 50)

    n_workers = os.cpu_count()
    # n_workers = 4

    chunk_sizes_to_try = [500_000, 1_000_000, 2_000_000]  # DESIGN CHOICE B

    all_results = [benchmark_transform(cs, n_workers) for cs in chunk_sizes_to_try]

    best = max(all_results, key=lambda r: r["speedup"])

    os.makedirs(processed_dir, exist_ok=True)
    best["result_df"].to_parquet(transformed_output_path, index=False)

    print("\n" + "=" * 65)
    print(f" Saved transformed parquet ({best['chunk_size']:,} row chunks) to:")
    print(f" {transformed_output_path}")
    print("=" * 65)

    print("\nBenchmark summary:")
    for r in all_results:
        print(f"  chunk_size={r['chunk_size']:>10,} | seq={r['sequential_seconds']}s "
              f"| par={r['parallel_seconds']}s | speedup={r['speedup']}x")

    return all_results


if __name__ == "__main__":
    # ProcessPoolExecutor requires this guard so worker processes don't
    # re-import and re-execute this module's top-level code.
    run_transform_benchmarks()