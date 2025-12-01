import os
import tempfile
from google.cloud import storage
from google.api_core import exceptions
from dotenv import load_dotenv

load_dotenv()

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
BASE_PATH = tempfile.gettempdir()

BUCKET_NAME = os.getenv("BUCKET_NAME")
print(f"Using GCS Bucket: {BUCKET_NAME}")



files_list = [

    # webscraping
    'banpt_institution.csv',
    'banpt_prodi.csv',
    'quipper_institution.csv',
    'quipper_prodi.csv',
    'quipper_faculty.csv',
    'pddikti_nasional.csv',
    'rencanamu.csv',   
    'unirank_nasional.csv',

    # preprocess 
    'banpt_institution_clean.csv',
    'banpt_prodi_clean.csv',
    'quipper_institution_clean.csv',
    'quipper_prodi_clean.csv',
    'quipper_faculty_clean.csv',
    'pddikti_nasional_clean.csv',
    'rencanamu_institutions_preprocessed.csv',
    'rencanamu_prodi_preprocessed.csv',
    'unirank_nasional_clean.csv',

    # end result
    'merged_institutions.csv',
    'merged_prodi_final.csv',

]

def get_storage_client():
    """
    Handles auth for both local (with JSON key) and
    production (with default credentials).
    """
    # Check if we are running in a Cloud Run Job or Service
    if os.getenv('CLOUD_RUN_JOB') or os.getenv('K_SERVICE'):
        print("✅ Authenticating using Google Cloud default credentials...")
        return storage.Client()
    else:
        # We are local, use the service account key
        print("ℹ️ Authenticating locally using service_account_key.json...")
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"❌ ERROR: {SERVICE_ACCOUNT_FILE} not found.")
            return None
        return storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)


def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """
    Uploads a local file to Google Cloud Storage.
    
    Args:
        bucket_name (str): The name of your GCS bucket.
        source_file_path (str): The local path of the file to upload.
        destination_blob_name (str): The name you want the file to have in the bucket.
    """
    storage_client = get_storage_client()
    if not storage_client:
        print("❌ Auth failed, cannot upload.")
        return

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        print(f"Uploading {source_file_path} to gs://{bucket_name}/{destination_blob_name}...")
        
        blob.upload_from_filename(source_file_path)

        print(f"✅ Successfully uploaded {destination_blob_name}")
        return True

    except exceptions.NotFound:
        print(f"❌ ERROR: Bucket not found: {bucket_name}")
    except exceptions.Forbidden as e:
        print(f"❌ ERROR uploading {destination_blob_name}: Permission denied. {e}")
    except Exception as e:
        print(f"❌ An unknown error occurred uploading {destination_blob_name}: {e}")
    
    return False


def download_from_gcs(bucket_name, source_blob_name, destination_file_path):
    """
    Downloads a file from Google Cloud Storage to a local path.

    Args:
        bucket_name (str): The name of your GCS bucket.
        source_blob_name (str): The name of the file in the bucket.
        destination_file_path (str): The local path to save the file.
    """
    storage_client = get_storage_client()
    if not storage_client:
        print("❌ Auth failed, cannot download.")
        return False

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        
        print(f"Downloading gs://{bucket_name}/{source_blob_name} to {destination_file_path}...")
        
        blob.download_to_filename(destination_file_path)
        
        print(f"✅ Successfully downloaded {source_blob_name}")
        return True

    except exceptions.NotFound:
        print(f"⚠️ WARNING: File {source_blob_name} not found in bucket.")
    except exceptions.Forbidden as e:
        print(f"❌ ERROR downloading {source_blob_name}: Permission denied. {e}")
    except Exception as e:
        print(f"❌ An unknown error occurred downloading {source_blob_name}: {e}")
    
    return False



def download_all_files():   
    print("--- Starting GCS File Download ---")

    for file_name in files_list:
        destination_path = os.path.join(BASE_PATH, file_name)
        download_from_gcs(BUCKET_NAME, file_name, destination_path)

    print("--- Finished GCS File Download ---")


def upload_all_files():
    print("--- Starting GCS File Upload ---")

    for file_name in files_list:
        source_path = os.path.join(BASE_PATH, file_name)
        upload_to_gcs(BUCKET_NAME, source_path, file_name)
        
    print("--- Finished GCS File Upload ---")