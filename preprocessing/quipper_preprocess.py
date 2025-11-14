import pandas as pd
import re
import helpers.csv_crud as csv_crud

# functions
def apply_uppercase_vectorized(df, columns_list):
    for col in columns_list:
        if col in df.columns:
            # .fillna('') ensures NaN values are treated as empty strings first, 
            # preventing errors when .str.upper() is called.
            df.loc[:, col] = df[col].fillna('').str.upper()
    return df

# TREAT 'fee' FOR EACH DATAFRAME SEPARATELY FIRST (final product is 3 columns: average_semester_fee, starting_semester_fee, ending_semester_fee, average_yearly_fee, starting_yearly_fee, ending_yearly_fee)
df_faculty = csv_crud.read_csv_file("quipper_faculty.csv")
df_institution = csv_crud.read_csv_file("quipper_institution.csv")
df_prodi = csv_crud.read_csv_file("quipper_prodi.csv")


# uppercase fee
df_institution = apply_uppercase_vectorized(df_institution, ['fee'])

df_institution['fee'] = (
    df_institution['fee']
    .astype(str)                      
    .str.replace('RP', '', regex=False) 
    .str.replace(' JUTAAN', '000000', regex=False)
    .str.replace(',00', '', regex=False)
    .str.replace('.', '', regex=False)  
    .str.replace(',', '', regex=False)
    .str.replace('-', ' ', regex=False)  
    .str.replace(r'GRATIS|DITANGGUNG', '0 SEMESTER', regex=True) 
)
df_institution['fee'] = df_institution['fee'].str.findall(r'[0-9,.]+|SEMESTER|TAHUN|BULAN\s')
df_institution['fee'] = df_institution['fee'].str.join(' ').str.strip()

# hapus instansi apabila tidak memiliki fee
print("Row count before empty fee discard:", len(df_institution))
df_institution = df_institution[
    df_institution['fee'].notna() &                     # Check for non-null values
    df_institution['fee'].astype(str).str.strip().ne('') & # Check for non-empty/non-whitespace strings
    df_institution['fee'].astype(str).str.strip().ne('-') &
    df_institution['fee'].astype(str).str.strip().ne('$') &
    df_institution['fee'].astype(str).str.strip().ne('USD')
]
print("Row count after empty fee discard:", len(df_institution))

# set default values of fees
df_institution['average_semester_fee'] = -1.0
df_institution['starting_semester_fee'] = -1.0
df_institution['ending_semester_fee'] = -1.0
df_institution['average_yearly_fee'] = -1.0
df_institution['starting_yearly_fee'] = -1.0
df_institution['ending_yearly_fee'] = -1.0

for index, row in df_institution.iterrows():
    pattern = r'([\d\s,]+)(\s*(SEMESTER|BULAN|TAHUN))?'
    
    matches = re.findall(pattern, row['fee'])
    
    semester_fees = []
    yearly_fees = []
    # monthly_fees = []

    for match in matches:
        numbers = match[0]
        individual_amounts_str = re.findall(r'\d+', match[0])

        try:
            individual_amounts_int = [int(amount) for amount in individual_amounts_str]
                
        except ValueError as e:
            print(f"Warning: Could not convert amount to integer. Error: {e}. Skipping row.")
            continue

        period = 'SEMESTER'
        if match[1]:
            period = match[1]
        

        if (period == 'SEMESTER'):
            semester_fees.extend(individual_amounts_str)
        elif (period == 'TAHUN'):
            yearly_fees.extend(individual_amounts_str)  
        elif (period == 'BULAN'):
            semester_fees.extend(individual_amounts_str * 6)  # assume 6 months in a semester

        
    semester_fees = pd.Series(semester_fees)
    yearly_fees = pd.Series(yearly_fees)

    if semester_fees.empty and yearly_fees.empty:
        print(f"Warning: No valid fee data found for institution_code {row['institution_code']}. Skipping row.")
        continue

    if not semester_fees.empty:
        df_institution.at[index, 'average_semester_fee'] = semester_fees.astype(int).mean()
        df_institution.at[index, 'starting_semester_fee'] = semester_fees.astype(int).min()
        df_institution.at[index, 'ending_semester_fee'] = semester_fees.astype(int).max()
    elif not yearly_fees.empty:
        df_institution.at[index, 'average_semester_fee'] = (yearly_fees.astype(int).mean() / 2)
        df_institution.at[index, 'starting_semester_fee'] = (yearly_fees.astype(int).min() / 2)
        df_institution.at[index, 'ending_semester_fee'] = (yearly_fees.astype(int).max() / 2)

    if not yearly_fees.empty:
        df_institution.at[index, 'average_yearly_fee'] = yearly_fees.astype(int).mean()
        df_institution.at[index, 'starting_yearly_fee'] = yearly_fees.astype(int).min()
        df_institution.at[index, 'ending_yearly_fee'] = yearly_fees.astype(int).max()
    elif not semester_fees.empty:
        df_institution.at[index, 'average_yearly_fee'] = (semester_fees.astype(int).mean() * 2)
        df_institution.at[index, 'starting_yearly_fee'] = (semester_fees.astype(int).min() * 2)
        df_institution.at[index, 'ending_yearly_fee'] = (semester_fees.astype(int).max() * 2)
    

    # print(row['institution_code'], semester_fees.values, yearly_fees.values)

    # now fill in the fee columns
    
df_institution = df_institution.drop(columns=['fee'])
print((df_institution[['institution_code', 'average_semester_fee', 'starting_semester_fee', 'ending_semester_fee', 'average_yearly_fee', 'starting_yearly_fee', 'ending_yearly_fee']] == -1).sum())







# GENERAL PIPELINE---------------------------------------------------------------------

# 2. UPPERCASE semua text di seluruh kolom object
faculty_columns_to_uppercase = ['faculty']
institution_columns_to_uppercase = ['institution_name', 'body_type', 'fee']
prodi_columns_to_uppercase = ['faculty', 'prodi']
df_faculty = apply_uppercase_vectorized(df_faculty, faculty_columns_to_uppercase)
df_institution = apply_uppercase_vectorized(df_institution, institution_columns_to_uppercase)
df_prodi = apply_uppercase_vectorized(df_prodi, prodi_columns_to_uppercase)

# 3. Hapus semua kurung dan isinya
def remove_parentheses(text):
    text = re.sub(r"\(.*?\)", "", text)
    
    # 2. Remove any remaining single parentheses (fixes the "haha)" and "Address)" issues)
    text = re.sub(r"[()]", "", text)
    
    # 3. Strip whitespace
    return text.strip()

df_faculty = df_faculty.map(lambda x: remove_parentheses(x) if isinstance(x, str) else x)
df_institution = df_institution.map(lambda x: remove_parentheses(x) if isinstance(x, str) else x)
df_prodi = df_prodi.map(lambda x: remove_parentheses(x) if isinstance(x, str) else x)

# 4. TRIM spasi berlebih
df_faculty = df_faculty.map(lambda x: x.strip() if isinstance(x, str) else x)
df_institution = df_institution.map(lambda x: x.strip() if isinstance(x, str) else x)
df_prodi = df_prodi.map(lambda x: x.strip() if isinstance(x, str) else x)

# 5. Hapus duplikat (baris yang identik seluruh kolomnya)
df_faculty = df_faculty.drop_duplicates()
df_institution = df_institution.drop_duplicates()
df_prodi = df_prodi.drop_duplicates()

# 6. Modify data
# unknown amount to -1
df_institution['student_amount'] = df_institution['student_amount'].fillna(-1)
df_institution['lecturer_amount'] = df_institution['lecturer_amount'].fillna(-1)

# deskripsi kosong ubah menjadi "-"
df_institution['description'] = df_institution['description'].fillna('-')
df_institution['description'] = df_institution['description'].replace(r'^\s*$', '-', regex=True)
df_institution['student_amount'] = df_institution['student_amount'].fillna(-1)
df_institution['lecturer_amount'] = df_institution['lecturer_amount'].fillna(-1)

# normalize body_type
#uncomment for debugging--------------------------------------------------------------
# print(df_institution['body_type'].unique())
# target_values = ["LAINNYA", "KURSUS BERSERTIFIKASI"]
# mask = df_institution['body_type'].isin(target_values)
# count = mask.sum()
# print(f"Number of rows with 'LAINNYA' or 'KURSUS BERSERTIFIKASI' in body_type: {count}")
#---------------------------------------------------------------------------------------

pattern = r'(SWASTA|NEGERI|NEGRI)'

df_institution['body_type'] = (
    df_institution['body_type']
    .astype(str) 
    .str.extract(pattern, expand=False) 
    .fillna('-') 
)

#uncomment for debugging--------------------------------------------------------------
# print('body type unique values:' + df_institution['body_type'].unique())
# print(df_institution['body_type'].unique())
# target_values = ["-"]
# mask = df_institution['body_type'].isin(target_values)
# count = mask.sum()
# print(f"Number of rows with '-' in body_type: {count}")
#---------------------------------------------------------------------------------------

# only keep swasta institutions
df_institution = df_institution[df_institution['body_type'] == 'SWASTA']
print("Row count after keeping only SWASTA institutions:", len(df_institution))


# 7. Hapus row dengan prodi kosong di df_prodi
df_prodi = df_prodi[
    df_prodi['prodi'].notna() &                     # Check for non-null values
    df_prodi['prodi'].astype(str).str.strip().ne('') & # Check for non-empty/non-whitespace strings
    df_prodi['prodi'].astype(str).str.strip().ne('-')
]

# 8. Drop make similar to other institution datasets
df_institution = df_institution.drop(columns=['unknown_field'], errors='ignore')
df_institution['address'] = '-'

# 9. Check null columns
faculty_null_columns = df_faculty.columns[df_faculty.isnull().any()].tolist()
institution_null_columns = df_institution.columns[df_institution.isnull().any()].tolist()
prodi_null_columns = df_prodi.columns[df_prodi.isnull().any()].tolist()
if faculty_null_columns or institution_null_columns or prodi_null_columns:
    print(" Faculty null columns:", faculty_null_columns)
    print(" Institution null columns:", institution_null_columns)
    print(" Prodi null columns:", prodi_null_columns)
else:
    print(f"Banyak baris faculty: {len(df_faculty)}")
    print(f"Banyak baris institution: {len(df_institution)}")
    print(f"Banyak baris prodi: {len(df_prodi)}")
    print("✅ Tidak ada kolom NULL")


# 10. Special handling for binus & president university
# binus
df_institution.loc[df_institution['institution_name'] == 'BINUS UNIVERSITY', 'institution_name'] = 'UNIVERSITAS BINA NUSANTARA'
df_institution.loc[df_institution['institution_name'] == 'PRESIDENT UNIVERSITY', 'institution_name'] = 'UNIVERSITAS PRESIDEN'


# 11. Simpan hasil bersih ke file baru
csv_crud.save_csv_file(df_faculty, "quipper_faculty_clean.csv")
csv_crud.save_csv_file(df_institution, "quipper_institution_clean.csv") 
csv_crud.save_csv_file(df_prodi, "quipper_prodi_clean.csv")
print("🎉 Preprocessing selesai!")
