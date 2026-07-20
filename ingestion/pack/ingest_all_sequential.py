import os  # creating directories and defining their paths
import pandas as pd #for filtering tabular data
from config.package.settings import PipelineConfig
from config.package import logger
import requests #sending HTTP requests
from time import perf_counter #performance counter to measure execution time 
import kagglehub
from kagglehub import KaggleDatasetAdapter

#Global paths to be imported in transform_parallel.py
processed_dir = os.path.join("data", "processed") #cross-platform path
transaction_output_path = os.path.join(processed_dir, "transactions.parquet")

def ingest_paysim():
    # 1- Reading CSV file
    df = pd.read_csv(f"{PipelineConfig().raw_dir}/PS_20174392719_1491204439457_log.csv")
    print('loading')
    print("Before:\n", df.columns.tolist())  #converts df column headers to a python list

    # 2- Built-in function to check if the object is a an instance of a Pandas DataFrame
    if isinstance(df, pd.DataFrame):  
        print('Dataframe loaded successfully')
    else:
            logger.error('Failed to load Dataframe')
    # 3- Rename columns to snake_case
    #regex 'regular expression' indicates that we are searching for a pattern rather than a literal text.
    df.columns = (df.columns.str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True).str.lower())
    print("After:\n", df.columns.tolist())

    # 4- Save the DataFrame to a Parquet file in the processed directory

    # 4.1. Define and create the directory, ignore this if it already exists
    #processed_dir = os.path.join("data", "processed") #cross-platform path
    os.makedirs(processed_dir, exist_ok=True)
    
    # 4.2. Define the output file path, combining the processed directory with the destination file name
    #transaction_output_path = os.path.join(processed_dir, "transactions.parquet")
    
    # 4.3. Write the DataFrame to a proper Parquet file
    df.to_parquet(transaction_output_path, index=False) #prevents creating a column of row-number
    print(f"Successfully saved to {transaction_output_path} | Rows: {len(df)}")

    return df

#standard way to fetch an existing or a web URL.
# def ingest_fred():
#     print("\n>>> Starting FRED Macro Ingestion...")
#     try:
#         # 1. Create the local directory to store raw macro data
#         macro_dir = os.path.join("data", "raw", "macro")
#         os.makedirs(macro_dir, exist_ok=True)
        
#         # 2. Define the URLs for the three series
#         #CPIAUCSL: Consumer Price Index for All Urban Consumers: All Items in U.S. City Average
#         # It measures the average change over time in the prices paid by urban consumers for a market basket.
#         series_ids = {
#             "CPIAUCSL": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL",
#             "UNRATE": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE",
#             "DEXUSEU": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXUSEU"
#         }
        
#         # 3. Loop through each item, fetch it, and save it locally
#         for name, url in series_ids.items():
#             print(f"Fetching {name} data from FRED...")
            
#             # Pandas downloads and parses the CSV directly from the web URL
#             df = pd.read_csv(url)
            
#             # Save the DataFrame to your local data folder
#             csv_path = os.path.join(macro_dir, f"{name}.csv")
#             df.to_csv(csv_path, index=False)
#             print(f"Successfully saved {name}.csv | Rows: {len(df)}")
            
#     except Exception as e:
#         print(f"Error in ingest_fred: {e}")

#using requests library to get the URL and save it locally.    
def ingest_fred():
    try:
        macro_dir = os.path.join("data", "raw", "macro")
        os.makedirs(macro_dir, exist_ok=True)
        
        series_ids = {
            "CPIAUCSL": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL",
            "UNRATE": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE",
            "DEXUSEU": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXUSEU"
        }
        
        for name, url in series_ids.items():
            print(f"Downloading {name} from FRED...")
            
            # Send an HTTP GET request to the FRED URL
            response = requests.get(url)
            
            # Check if the download was successful (HTTP Status Code 200)
            if response.status_code == 200:
                csv_path = os.path.join(macro_dir, f"{name}.csv")
                
                # Write the raw bytes directly to a local CSV file
                with open(csv_path, "wb") as f:
                    f.write(response.content)
                
                # Verify it worked by loading it quickly to count rows
                df = pd.read_csv(csv_path)
                print(f"Successfully downloaded {name}.csv | Rows: {len(df)}")
            else:
                logger.error(f"Failed to fetch {name}. HTTP Status Code: {response.status_code}")
    except Exception as e:
        logger.error(f"An error occurred while downloading FRED data: {e}")


def ingest_complaints():
    try:

        processed_dir = os.path.join("data", "processed")
        os.makedirs(processed_dir, exist_ok=True)

        path = kagglehub.dataset_download("selener/consumer-complaint-database")
        
        # Find the CSV file inside the downloaded path
        csv_file = os.path.join(path, "rows.csv")
        df = pd.read_csv(csv_file, dtype=str) #treats text in cols as str
        
        # 3. Standardize column names to lowercase
        df.columns = df.columns.str.lower()
        
        # 4. Apply strict target filter
        filtered_df = df[
            df['product'].str.contains('credit card', case=False, na=False) |
            df['product'].str.contains('checking or savings', case=False, na=False)
        ]
        
        # 5. Save the filtered results directly to parquet
        output_path = os.path.join(processed_dir, "complaints.parquet")
        filtered_df.to_parquet(output_path, index=False)

        logger.info(f"CFPB Ingestion Successful! Row count: {len(filtered_df)}")
        print(f"Saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"Error in ingest_complaints: {e}")


def run_sequential():

    steps = [('Paysim Ingestion', ingest_paysim),
             ('FRED Macro Ingestion', ingest_fred),
             ('CFPB Complaints Ingestion', ingest_complaints)]
    
    overall_start_time = perf_counter()

    for step_name, step_func in steps:
        print(f'Starting: {step_name}')
        step_start_time = perf_counter()
        try:
            step_func()
            step_elapsed_time = perf_counter() - step_start_time
            print(f'Completed {step_name} in {step_elapsed_time} seconds\n')
        except Exception as e:
            logger.error(f'Error during {step_name}: {e}\n')
    overall_elapsed_time = perf_counter() - overall_start_time
    print(f'All steps completed in {overall_elapsed_time} seconds')
    


if __name__ == "__main__":
    run_sequential()



