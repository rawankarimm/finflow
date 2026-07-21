from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from ingestion.pack.ingest_all_sequential import ingest_paysim
from ingestion.pack.ingest_all_sequential import ingest_fred
from ingestion.pack.ingest_all_sequential import ingest_complaints
from ingestion.pack.ingest_all_sequential import run_sequential
from config.package import logger
import time #measures short durations and 'wall-clock' elapsed time (including system lag and sleep)

def run_parallel():
    print('Starting parallel ingestion...\n')

    start_time = time.perf_counter()
    #with ProcessPoolExecutor(max_workers=3) as executor: 
    with ThreadPoolExecutor(max_workers=3) as executor :
        #mapping future objects to their names
        futures_map = {executor.submit(ingest_paysim): 'Paysim',
        executor.submit(ingest_fred): 'FRED',
        executor.submit(ingest_complaints): 'CFPB complaints'}
        #submit(): kicks of the fn to a background thread, returns a future object
        #passes by kwargs (order does not matter)

        for future in as_completed(futures_map):  #loops over the compelted tasks regardless of thier order

            task_name = futures_map[future]
            try:
                result = future.result()  #retreives the return value of the fn, raises the exact same errors in the fn (if exist) and catches them in the try-except block 
                print(f'{task_name} task finished successfully!')


            except Exception as e: 
                logger.error(f'{task_name} task failed: {e}')

    elapsed_time = time.perf_counter() - start_time
    logger.info(f"Parallel Ingestion completed in {elapsed_time:.4f} seconds.")
    return elapsed_time


def benchmark_ingestion():
    print("\n" + "="*50)
    print("   STARTING PIPELINE INGESTION BENCHMARK")
    print("="*50)
    
    # 1. Run & Time Sequential Execution
    print("\n>>> Phase 1: Running Sequential Baseline...")
    seq_start = time.perf_counter()
    run_sequential()  # Runs all 3 sequentially
    seq_time = time.perf_counter() - seq_start
    
    # 2. Run & Time Parallel Execution
    print("\n>>> Phase 2: Running Parallel Pipeline...")
    par_time = run_parallel() #fn returns the elapsed time
    
    # 3. Calculate metrics
    speedup = seq_time / par_time if par_time > 0 else 0 
    time_saved = seq_time - par_time
    
    # 4. Print the Comparison Table
    print("\n" + "="*65)
    print("             INGESTION PERFORMANCE BENCHMARK")
    print("="*65)
    print(f" {'Ingestion Method':<25} | {'Execution Time (s)':<20} | {'Status':<10}")
    print("-" * 65)
    print(f" {'Sequential (Baseline)':<25} | {seq_time:<20.4f} | {'Baseline':<10}")
    print(f" {'Parallel (3 Workers)':<25} | {par_time:<20.4f} | {'SUCCESS':<10}")
    print("-" * 65)
    print(f" Performance Speedup Factor : {speedup:.2f}x faster")
    print(f" Total Clock Time Saved     : {time_saved:.4f} seconds")
    print("="*65 + "\n")


if __name__ == "__main__":
    benchmark_ingestion()



            


    