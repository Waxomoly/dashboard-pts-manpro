import numpy as np
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
df_institution_banpt = pd.read_csv(BASE_PATH +"banpt_institution_clean.csv")

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

# combine all into one big dataframe
df_merged = pd.concat([df_institution_quipper, df_institution_rencanamu, df_institution_pddikti], ignore_index=True, sort=False)
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
for idx, row in df_duplicates.iterrows():
    institution_name = row['institution_name']
    if institution_name in df_merged['institution_name'].values:
        for col in df_merged.columns:
            if col == 'institution_name':
                continue
            val_existing = df_merged.loc[df_merged['institution_name'] == institution_name, col].values[0]
            val_new = row[col]
            
            if col == 'institution_code':
                print(val_new)
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
                
            if pd.isna(val_existing) or val_existing in ['-', -1, '']:
                if not (pd.isna(val_new) or val_new in ['-', -1, '']):
                    df_merged.loc[df_merged['institution_name'] == institution_name, col] = val_new
    else:
        df_merged = pd.concat([df_merged, pd.DataFrame([row])], ignore_index=True, sort=False)


print(f"Total rows after cleaning duplicates: {len(df_merged)}")


# merge with institution banpt (accreditation & )
# load banpt data
df_banpt = pd.read_csv(BASE_PATH + "merged_prodi_final.csv")
values_not_in_df2 = df_merged[~df_merged['institution_code'].isin(df_banpt['institution_code'])]['institution_code']
# df_merged = df_merged.merge(df_banpt[['akreditasi_institusi']], on='', how='left')
# print(f"\nTotal rows after merging with banpt data: {len(df_merged)}")

print(f"\nTotal institution_code in merged_institutions not in banpt data: {len(values_not_in_df2)}")
print(values_not_in_df2.tolist())

# save to csv
output_path = BASE_PATH + "merged_institutions.csv"
df_merged.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\nMerging Selesai! File final '{output_path}' telah dibuat.")