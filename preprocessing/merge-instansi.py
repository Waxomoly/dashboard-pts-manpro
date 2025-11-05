import numpy as np
import pandas as pd
import re
import os
from tqdm import tqdm

# constants - perbaiki path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
BASE_PATH = os.path.join(parent_dir, "csv_result") + os.sep

df_institution_quipper = pd.read_csv(BASE_PATH + "quipper_institution_clean.csv")
df_institution_rencanamu = pd.read_csv(BASE_PATH + "rencanamu_institutions_preprocessed.csv")
df_institution_pddikti = pd.read_csv(BASE_PATH +"pddikti_nasional_clean.csv")
df_institution_banpt = pd.read_csv(BASE_PATH +"banpt_institution_clean.csv")

# 1. Convert columns to sets
rencanamu_cols = set(df_institution_rencanamu.columns)
quipper_cols = set(df_institution_quipper.columns)
pddikti_cols = set(df_institution_pddikti.columns)
banpt_cols = set(df_institution_banpt.columns)

# 2. Find the set of columns common to ALL three DataFrames (the intersection)
common_cols = rencanamu_cols.intersection(quipper_cols, pddikti_cols)

# 3. Calculate the unique columns for each DataFrame using set subtraction
# (Remove the common columns from the full list)
unique_rencanamu = rencanamu_cols - common_cols
unique_quipper = quipper_cols - common_cols
unique_pddikti = pddikti_cols - common_cols
unique_banpt = banpt_cols - common_cols

# 4. Print the result
print("--- Unique Columns Per DataFrame (Excluding Common Keys) ---")
print(f"Rencanamu Unique Columns: {sorted(list(unique_rencanamu))}")
print(f"Quipper Unique Columns:   {sorted(list(unique_quipper))}")
print(f"PDDIKTI Unique Columns:   {sorted(list(unique_pddikti))}")
print(f"banpt Unique Columns:   {sorted(list(unique_banpt))}")



# temporary drop, erase after andika's fix .........
# df_institution_pddikti.drop(columns=['regency'], inplace=True, errors='ignore')
# output_path = BASE_PATH + "DEBUG PDDIKTI.csv"
# df_institution_pddikti.to_csv(output_path, index=False, encoding='utf-8-sig')
# exit()
# ...................................................



# MERGING DATAFRAMES -----------------------------------------------------------

# add province column to quipper
df_institution_quipper['province'] = '-'

# add campus_accreditation column to quipper and pddikti
df_institution_quipper['campus_accreditation'] = '-'
df_institution_pddikti['campus_accreditation'] = '-'
# rename for banpt
df_institution_banpt.rename(columns={'akreditasi_institusi': 'campus_accreditation'}, inplace=True)

# combine all into one big dataframe
df_merged = pd.concat([df_institution_quipper, df_institution_rencanamu, df_institution_pddikti, df_institution_banpt], ignore_index=True, sort=False)
df_merged['quipper_code'] = '-'
df_merged['pddikti_code'] = '-'
df_merged['rencanamu_code'] = '-'
df_merged['banpt_code'] = '-'
source_prefix = df_merged['institution_code'].str.partition('-')[0].str.lower()


df_merged['quipper_code'] = np.where(
    source_prefix == 'quipper',                             # Condition: Is the prefix 'quipper'?
    df_merged['institution_code'],                          # Value if True: Use the full institution_code
    df_merged['quipper_code']                               # Value if False: Keep the existing value ('-')
)

df_merged['pddikti_code'] = np.where(
    source_prefix == 'pddikti',
    df_merged['institution_code'],
    df_merged['pddikti_code']
)

df_merged['rencanamu_code'] = np.where(
    source_prefix == 'rencanamu',
    df_merged['institution_code'],
    df_merged['rencanamu_code']
)

df_merged['banpt_code'] = np.where(
    source_prefix == 'banpt',
    df_merged['institution_code'],
    df_merged['banpt_code']
)



print(df_merged['institution_code'].unique())


# erase duplicate institution_name rows, keep one with biggest average_yearly_fee.
# also fill in the '-' or -1 columns if possible
print(f"\nTotal rows before cleaning duplicates: {len(df_merged)}")
df_merged.sort_values(by='average_yearly_fee', ascending=False, inplace=True)
duplicate_mask = df_merged.duplicated(subset=['institution_name'], keep=False)
df_duplicates = df_merged[duplicate_mask]
df_merged = df_merged.drop_duplicates(subset=['institution_name'], keep=False)


# try to fill in the '-' or -1 columns if possible
for idx, row in tqdm(df_duplicates.iterrows()):
    institution_name = row['institution_name']
    if institution_name in df_merged['institution_name'].values:
        for col in df_merged.columns:
            if col == 'institution_name':
                continue
            val_existing = df_merged.loc[df_merged['institution_name'] == institution_name, col].values[0]
            val_new = row[col]
            
            if col == 'institution_code':
                # print(val_new)
                val_new_web = val_new.partition('-')[0].lower()
                val_existing_web = val_existing.partition('-')[0].lower()
                web_col = '-'


                if(val_new_web != val_existing_web):
                    if val_new_web == "quipper":
                        web_col = 'quipper_code'
                    elif val_new_web == "rencanamu":
                        web_col = 'rencanamu_code'
                    elif val_new_web == "pddikti":
                        web_col = 'pddikti_code'
                    elif val_new_web == "banpt":
                        web_col = 'banpt_code'
                    else:
                        print(f"Unknown source code prefix: {val_new_web}")



                    try:
                        df_merged.loc[df_merged['institution_name'] == institution_name, web_col] = val_new
                    except KeyError as e:
                        print(f"KeyError: {e} for column {web_col}")
                    # prefer the one with higher priority source
                    # df_merged.loc[df_merged['institution_name'] == institution_name, col] = val_new
                
            elif pd.isna(val_existing) or val_existing in ['-', -1, '']:
                if not (pd.isna(val_new) or val_new in ['-', -1, '']):
                    df_merged.loc[df_merged['institution_name'] == institution_name, col] = val_new


                try:
                    val_existing_num = float(val_existing)
                except (ValueError, TypeError):
                    val_existing_num = -1
                try:
                    val_new_num = float(val_new)
                except (ValueError, TypeError):
                    val_new_num = -1

                if val_existing_num == -1 and val_new_num != -1:
                    df_merged.loc[df_merged['institution_name'] == institution_name, col] = val_new_num

            # ensure newest reasonable price is used
            if col in ['average_semester_fee', 'starting_semester_fee', 'ending_semester_fee',
                       'average_yearly_fee', 'starting_yearly_fee', 'ending_yearly_fee']\
                        and val_new > val_existing and val_new < 999999999:
                df_merged.loc[df_merged['institution_name'] == institution_name, col] = val_new
    else:
        df_merged = pd.concat([df_merged, pd.DataFrame([row])], ignore_index=True, sort=False)


print(f"Total rows after cleaning duplicates: {len(df_merged)}")


print(f"Rows lack accreditation info: {len(df_merged[df_merged['campus_accreditation'].isin(['-', '', pd.NA])])}")
print(f"Rows lack price info: {len(df_merged[df_merged['average_yearly_fee'] == -1])}")
print(f"Rows lacking both accreditation and price info: {len(df_merged[(df_merged['campus_accreditation'].isin(['-', '', pd.NA])) & (df_merged['average_yearly_fee'] == -1)])}")
print(f"Rows with banpt source: {len(df_merged[df_merged['banpt_code'] != '-'])}")


# remove rows with empty accreditation and price info
initial_len = len(df_merged)
df_merged = df_merged[~((df_merged['campus_accreditation'].isin(['-', '', pd.NA])) | (df_merged['average_yearly_fee'] == -1))]
print(f"Removed {initial_len - len(df_merged)} rows lacking both accreditation and price info.")

# re-index institution_code
df_merged.reset_index(drop=True, inplace=True)
df_merged['institution_code'] = (df_merged.index + 1)

# number of institutions left
print(f"Total institutions after final cleaning: {len(df_merged)}")

# save to csv
output_path = BASE_PATH + "merged_institutions.csv"
df_merged.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\nMerging Selesai! File final '{output_path}' telah dibuat.")