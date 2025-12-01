import subprocess
import sys
import os
import helpers.gcp_store as gcp_store
import datetime
import helpers.gcp_bigquery as gcp_bq


my_env = os.environ.copy()
my_env["PYTHONIOENCODING"] = "utf-8" # supaya emoji gak error


start_time = datetime.datetime.now()

print(f"--- Main Job Started [{start_time}] ---")

# HARUS URUT ORDERNYA
scripts_to_run = {
    'web-scraping' : [
        "quipper.py",
        "rencanamu.py",
        "pddikti.py",
        "banpt.py",
        "unirank.py"
    ],
    'preprocessing' : [
        'quipper_preprocess.py',
        'rencanamu_preprocess.py',
        "pddikti_preprocess.py",
        "banpt_preprocess.py", 
        "unirank_preprocess.py",

        "merge_instansi.py",
        "merge_prodi.py",
        "merge_institution_prodi.py"
    ]
    
}


# downloads csvs from GCS
gcp_store.download_all_files()


for folder, scripts in scripts_to_run.items():
    for script in scripts:

        script_path = os.path.join(folder, script)

        module_path = script_path.replace('.py', '').replace(os.sep, '.')


        try:
            print(f"\n[Main] === Running {module_path} as module ===")
            
            # Use sys.executable to ensure we use the same Python
            process = subprocess.run(
                [sys.executable, "-m", module_path],
                capture_output=True, 
                encoding='utf-8',  
                errors='replace',
                env=my_env
            )
            
            # Print the script's output
            print(process.stdout)
            
            # Check for errors
            if process.returncode != 0:
                print(f"[Main] === ERROR in {script} ===")
                print(process.stderr)
                print("[Main] === Halting job due to error. ===")
                # Exit the main script with an error code
                sys.exit(1) 
            else:
                print(f"[Main] === Finished {script_path} successfully ===")
        except FileNotFoundError:
            # This catches if the script_path is wrong
            print(f"[Main] === ERROR: Script not found at {script_path} ===")
            continue
            # sys.exit(1) 
            
        except subprocess.CalledProcessError as e:
            # This replaces your 'if process.returncode != 0' block
            print(f"[Main] === ERROR in {script_path} (return code {e.returncode}) ===")
            print(e.stderr)
            continue
            # sys.exit(1) 
            
        except Exception as e:
            # Catches any other unexpected error
            print(f"[Main] === An unexpected error occurred running {script_path}: {e}")
            continue
            # sys.exit(1) 


# uploads csvs to GCS
gcp_store.upload_all_files()

end_time = datetime.datetime.now()

print(f"=== LOADING DATA TO BIGQUERY [{end_time}] ===")
gcp_bq.load_tables_to_bq()



end_time = datetime.datetime.now()
print(f"Time taken: {end_time - start_time}")
print(f"--- Main Job Finished [{end_time}] ---")