import os
import tempfile
from google.cloud import storage
from google.api_core import exceptions


SERVICE_ACCOUNT_FILE = 'service_account_key.json'
# DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
# SCOPES = ['https://www.googleapis.com/auth/drive.file']
BASE_PATH = tempfile.gettempdir()

# if not DRIVE_FOLDER_ID:
#     print("❌ ERROR: DRIVE_FOLDER_ID environment variable not set.")
# else:
#     print(f"✅ DRIVE_FOLDER_ID found: {DRIVE_FOLDER_ID}")

BUCKET_NAME = "manpro_csv_res" 



files_to_upload = [
    'banpt_institution_clean.csv',
    'banpt_prodi_clean.csv',
    'quipper_institution_clean.csv',
    'quipper_prodi_clean.csv',
    'quipper_faculty_clean.csv',
    'pddikti_nasional_clean.csv',
    'rencanamu_institutions_preprocessed.csv',
    'rencanamu_prodi_preprocessed.csv',
    'unirank_nasional_clean.csv'
]

def upload_files_to_gcs():
    print("--- Starting Google Cloud Storage Upload ---")
    
    try:
        # --- SMART AUTHENTICATION ---
        # Check if we are running in Google Cloud Run
        if os.getenv('K_SERVICE'):
            print("✅ Authenticating using Google Cloud Run default credentials...")
            storage_client = storage.Client()
        else:
            # We are running locally, use the service account JSON file
            print("ℹ️ Authenticating locally using service_account_key.json...")
            
            if not os.path.exists(SERVICE_ACCOUNT_FILE):
                print(f"❌ ERROR: {SERVICE_ACCOUNT_FILE} not found.")
                print("Make sure your service_account_key.json file is")
                print(f"in your project root folder: {PROJECT_ROOT}")
                return

            storage_client = storage.Client.from_service_account_json(
                SERVICE_ACCOUNT_FILE
            )
        # --- END OF AUTH ---

        # 2. Get the bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        print(f"✅ Authentication successful. Uploading to bucket: {BUCKET_NAME}")

    except Exception as e:
        print(f"❌ ERROR: Failed to authenticate with GCS: {e}")
        print("Please ensure your service account has the 'Storage Object Admin' role.")
        return

    # Loop through each file
    for file_name in files_to_upload:
        local_file_path = os.path.join(BASE_PATH, file_name)

        # 3. Check if the local file actually exists
        if not os.path.exists(local_file_path):
            print(f"⚠️ WARNING: Skipping {file_name}, file not found at {local_file_path}")
            continue

        try:
            # 4. Define the file's destination path *inside* the bucket
            # (We'll just use its filename, so it appears at the root)
            destination_blob_name = file_name
            
            blob = bucket.blob(destination_blob_name)

            print(f"Uploading {file_name} to gs://{BUCKET_NAME}/{destination_blob_name}...")
            
            # 5. Execute the upload
            blob.upload_from_filename(local_file_path)

            print(f"✅ Successfully uploaded {file_name}")

        except exceptions.NotFound:
            print(f"❌ ERROR: Bucket not found: {BUCKET_NAME}")
            break # Stop the loop if the bucket is wrong
        except exceptions.Forbidden as e:
            print(f"❌ ERROR uploading {file_name}: Permission denied. {e}")
        except Exception as e:
            print(f"❌ An unknown error occurred with {file_name}: {e}")

    print("--- Google Cloud Storage Upload Finished ---")


upload_files_to_gcs()