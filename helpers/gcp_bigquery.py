import os
import sys
from google.cloud import bigquery
from google.api_core import exceptions
from dotenv import load_dotenv


load_dotenv() 

# Get Bucket Name
BUCKET_NAME = os.getenv("BUCKET_NAME") 

# BigQuery Configuration
DATASET_ID = os.getenv("DATASET_ID") 
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

# List of tables to load
TABLES_TO_LOAD = [
    ('merged_institutions.csv', 'institutions'),
    ('merged_prodi_final.csv', 'prodi')
]

# --- Smart Authentication ---
def get_bigquery_client():
    """Handles auth for local and production."""
    if os.getenv('CLOUD_RUN_JOB') or os.getenv('K_SERVICE'):
        print("✅ Authenticating BigQuery using Google Cloud default credentials...")
        return bigquery.Client()
    else:
        print("ℹ️ Authenticating BigQuery locally using service_account_key.json...")
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"❌ ERROR: {SERVICE_ACCOUNT_FILE} not found.")
            return None
        return bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)

# --- Main Load Logic ---
def load_tables_to_bq(is_timestamp=False):
    print("--- Starting BigQuery Load Job ---")
    
    client = get_bigquery_client()
    if not client:
        return

    if is_timestamp:
        tables = [('last_updated.csv', 'last_updated_timestamp')]
    else:
        tables = TABLES_TO_LOAD

    for csv_name, table_name in tables:
        
        gcs_uri = f"gs://{BUCKET_NAME}/{csv_name}"
        table_id = f"{client.project}.{DATASET_ID}.{table_name}"

        # --- THIS IS THE KEY PART ---
        job_config = bigquery.LoadJobConfig(
            # 1. Tell BigQuery it's a CSV
            source_format=bigquery.SourceFormat.CSV,
            
            # 2. Skip the header row so it doesn't become data
            skip_leading_rows=1,
            
            # 3. Automatically figure out if columns are strings or integers
            autodetect=True,
            
            # 4. THE CRITICAL SETTING: Overwrite the table
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        # -----------------------------

        try:
            print(f"Loading {gcs_uri} into {table_id}...")
            
            load_job = client.load_table_from_uri(
                gcs_uri, 
                table_id, 
                job_config=job_config
            )

            load_job.result()  # Wait for the job to complete

            # Check how many rows were loaded
            destination_table = client.get_table(table_id)
            print(f"✅ Loaded {destination_table.num_rows} rows into '{table_name}'.")

        except exceptions.NotFound:
            print(f"❌ ERROR: Dataset '{DATASET_ID}' or GCS file '{gcs_uri}' not found.")
        except Exception as e:
            print(f"❌ An error occurred loading {table_name}: {e}")

    print("--- BigQuery Load Job Finished ---")

if __name__ == "__main__":
    load_tables_to_bq()