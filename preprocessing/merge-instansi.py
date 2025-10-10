import pandas as pd
import re
import os

# constants - perbaiki path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
BASE_PATH = os.path.join(parent_dir, "csv_result") + os.sep

df_institution_quipper = pd.read_csv(BASE_PATH + "quipper_institution_clean.csv")
df_institution_rencanamu = pd.read_csv(BASE_PATH + "rencanamu_institutions_preprocessed.csv")
df_institution_pddikti = pd.read_csv(BASE_PATH +"pddikti_nasional_clean.csv")

# 1. Convert columns to sets
rencanamu_cols = set(df_institution_rencanamu.columns)
quipper_cols = set(df_institution_quipper.columns)
pddikti_cols = set(df_institution_pddikti.columns)

# 2. Find the set of columns common to ALL three DataFrames (the intersection)
common_cols = rencanamu_cols.intersection(quipper_cols, pddikti_cols)

# 3. Calculate the unique columns for each DataFrame using set subtraction
# (Remove the common columns from the full list)
unique_rencanamu = rencanamu_cols - common_cols
unique_quipper = quipper_cols - common_cols
unique_pddikti = pddikti_cols - common_cols

# 4. Print the result
print("--- Unique Columns Per DataFrame (Excluding Common Keys) ---")
print(f"Rencanamu Unique Columns: {sorted(list(unique_rencanamu))}")
print(f"Quipper Unique Columns:   {sorted(list(unique_quipper))}")
print(f"PDDIKTI Unique Columns:   {sorted(list(unique_pddikti))}")