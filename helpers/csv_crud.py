import pandas as pd
import os
import tempfile

# CONSTANTS ---------------------------------------

BASE_PATH = tempfile.gettempdir()

# -------------------------------------------------

def read_csv_file(file_path):
    return pd.read_csv(os.path.join(BASE_PATH, file_path))

def save_csv_file(df, file_path):
    df.to_csv(os.path.join(BASE_PATH, file_path), index=False, encoding='utf-8-sig')
    print(f"File saved to {file_path}")
