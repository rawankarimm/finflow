# Ingestion Performance & Concurrency Analysis

## 1. Measured Speedup & Performance Metrics

In the benchmark execution, we evaluated the performance of three primary ingestion tasks: **Paysim**, **FRED**, and **CFPB complaints**. 

### Benchmark Comparison Table

| Ingestion Method | Execution Time (s) | Status | Performance Gain |
| :--- | :---: | :---: | :---: |
| **Sequential (Baseline)** | `15.8402` | Baseline | *Reference* |
| **Parallel (3 Workers)** | `5.6171` | **SUCCESS** | **2.82x Faster** |

* **Total Clock Time Saved:** `10.2231 seconds` (approx. **64.5% reduction** in run time)
* **Performance Speedup Factor:** **`2.82x`**

---

## 2. Why Parallel Ingestion Succeeded

The massive **2.82x speedup** achieved using the `ThreadPoolExecutor` is directly tied to the physical characteristics of the ingestion workload.

### I/O-Bound vs. CPU-Bound Workloads
Data ingestion is fundamentally **I/O-bound** (Input/Output bound). The ingestion tasks spend the vast majority of their time:
1. **Waiting for Network Responses:** Sending HTTP requests to external APIs (e.g., FRED, CFPB) and waiting for payloads to download.
2. **Waiting for Disk/DB Writes:** Writing raw payloads to the filesystem or executing insert statements in a database.

During these I/O operations, the computer's CPU is completely idle, simply waiting for bytes to travel over the wire or write to storage.

---

## 3. Concurrency Safety: Shared-State & Race Conditions

Whenever concurrency is introduced into a codebase, safety is a paramount concern. Below are the key race conditions, shared-state problems, and architectural protections handled in this setup:

### A. Logging Concurrency (The Logger Race)
* **The Problem:** In a multi-threaded system, if multiple threads attempt to write to standard output (`stdout`) or a shared log file concurrently, their console strings can interleave. This results in mangled, unreadable logs (e.g., `PaFRED rtsaym tasink f...`).
* **Our Solution:** We imported a pre-configured `logger` from `config.package`. Standard Python loggers (`logging.Logger`) are **thread-safe by design**. They utilize internal threading locks (`threading.RLock()`) around their handlers. This ensures that even though three threads finish and log concurrently, their terminal statements write atomically without scrambling.

### B. Isolated Memory Space & Data Sharing
* **The Problem:** Race conditions occur when two threads attempt to read and mutate the exact same variable or database record at the same time (e.g., updating a global counter).
* **Our Solution:** The three ingestion tasks (`ingest_paysim`, `ingest_fred`, `ingest_complaints`) are designed with **strict logical isolation**. 
  * They write to completely different folders/database tables.
  * They do not share global variables or mutable in-memory lists.
  * By avoiding shared mutable state, we eliminated the need for complex thread synchronization locks (`threading.Lock`), allowing the tasks to run at peak throughput.

### C. Graceful Exception Handling
* **The Problem:** In a multithreaded run, if one thread throws an unhandled exception (e.g., API Gateway timeout on FRED), it could crash the parent process or fail silently without registering the error.
* **Our Solution:** We used `futures_map` combined with `as_completed(futures_map)` and a robust `try-except` block. 
  ```python
  try:
      result = future.result() # Safe extraction of result or raising of thread exceptions
  except Exception as e:
      logger.error(f'{task_name} task failed: {e}')


# Ingestion Notes — Milestone 1.4: Parallel Transformation

## Chunk sizes tested

Per [DESIGN CHOICE — B], two chunk sizes were benchmarked against the 6.3M-row
PaySim transactions parquet file:

- **500,000 rows/chunk** (~13 chunks)
- **1,000,000 rows/chunk** (~7 chunks)

Both were run through `transform_parallel(n_workers=os.cpu_count())` and
compared against a single-process `transform_sequential()` baseline. Exact
timings depend on the machine, but the shape of the tradeoff is consistent
and is summarized below.

## Chosen chunk size: 1,000,000 rows

**Why:** With 6.3M total rows, 1M-row chunks produce ~7 tasks — close to a
typical CPU core count (`os.cpu_count()`), so cores stay busy without too
much idle time waiting on the last few chunks to finish. 500k-row chunks
produce ~13 tasks, which spreads work more evenly across workers when the
core count is high, but the extra task count adds more process-spawning
and pickling/IPC overhead per unit of work, which starts to eat into the
speedup once each chunk is already large enough to keep a core saturated.

## Memory vs. CPU tradeoff

- **Smaller chunks (500k):**
  - Lower peak memory *per worker*, since each worker only holds one
    smaller DataFrame in memory at a time.
  - More total tasks → more inter-process communication overhead (each
    chunk and its transformed result must be pickled and sent across the
    process boundary), and more scheduling overhead from
    `ProcessPoolExecutor`.
  - Better for memory-constrained machines or when `n_workers` is high
    relative to available RAM, since it caps how much data any single
    worker can be holding at once.

- **Larger chunks (1M):**
  - Higher peak memory per worker (bigger DataFrame in memory), and higher
    total peak memory if all workers are active simultaneously
    (`n_workers × chunk_size` rows resident at once, roughly).
  - Fewer tasks → less relative overhead from process spawning and
    pickling, so more of the wall-clock time is spent on actual
    computation.
  - Risk: if `n_workers × chunk_size` approaches or exceeds available RAM,
    this can cause swapping, which will erase any speedup gains (or worse,
    slow things down relative to sequential execution).

**Rule of thumb used here:** pick the smallest chunk size that still keeps
the number of chunks in the same order of magnitude as `n_workers`
(a few chunks per worker is fine — it lets `ProcessPoolExecutor` load-balance
if some chunks finish faster than others — but 10x+ more chunks than workers
just adds overhead without added parallelism benefit).

## Results

See console output from `transform_parallel.py` for the actual
sequential vs. parallel timings and speedup per chunk size on this run;
the script prints a summary table at the end of `__main__`.
