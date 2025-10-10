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




# temporary drop, erase after andika's fix .........
df_institution_pddikti.drop(columns=['regency'], inplace=True, errors='ignore')
# ...................................................



# MERGING DATAFRAMES -----------------------------------------------------------

# add province column to quipper
df_institution_quipper['province'] = '-'

# add campus_accreditation column to quipper and pddikti
df_institution_quipper['campus_accreditation'] = '-'
df_institution_pddikti['campus_accreditation'] = '-'

# combine all into one big dataframe
df_merged = pd.concat([df_institution_quipper, df_institution_rencanamu, df_institution_pddikti], ignore_index=True, sort=False)
print(df_merged.columns)


# erase duplicate institution_name rows, keep one with biggest average_yearly_fee.
# also fill in the '-' or -1 columns if possible
print(f"\nTotal rows before cleaning duplicates: {len(df_merged)}")
df_merged.sort_values(by='average_yearly_fee', ascending=False, inplace=True)
duplicate_mask = df_merged.duplicated(subset=['institution_name'], keep=False)
df_duplicates = df_merged[duplicate_mask]
df_merged = df_merged.drop_duplicates(subset=['institution_name'], keep=False)

# try to fill in the '-' or -1 columns if possible
for idx, row in df_duplicates.iterrows():
    institution_name = row['institution_name']
    if institution_name in df_merged['institution_name'].values:
        for col in df_merged.columns:
            if col in ['institution_code', 'institution_name']:
                continue
            val_existing = df_merged.loc[df_merged['institution_name'] == institution_name, col].values[0]
            val_new = row[col]
            if pd.isna(val_existing) or val_existing in ['-', -1, '']:
                if not (pd.isna(val_new) or val_new in ['-', -1, '']):
                    df_merged.loc[df_merged['institution_name'] == institution_name, col] = val_new
    else:
        df_merged = pd.concat([df_merged, pd.DataFrame([row])], ignore_index=True, sort=False)


print(f"Total rows after cleaning duplicates: {len(df_merged)}")


# merge with institution banpt (accreditation & )
# load banpt data
# df_banpt = pd.read_csv(BASE_PATH + "banpt.csv")



# save to csv
output_path = BASE_PATH + "merged_institutions.csv"
df_merged.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\nMerging Selesai! File final '{output_path}' telah dibuat.")