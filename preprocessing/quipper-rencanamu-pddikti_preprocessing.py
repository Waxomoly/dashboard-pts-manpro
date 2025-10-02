import pandas as pd
import re

# constants
BASE_PATH = "csv_result/"

# 1. Load data (NANTI SEMUA DIGABUNGIN DI SINI)
df_faculty = pd.read_csv(BASE_PATH + "quipper_faculty.csv")
df_institution = pd.read_csv(BASE_PATH + "quipper_institution.csv")
df_prodi = pd.read_csv(BASE_PATH +"quipper_prodi.csv")

# 2. UPPERCASE semua text di seluruh kolom object
def apply_uppercase_vectorized(df, columns_list):
    for col in columns_list:
        if col in df.columns:
            # .fillna('') ensures NaN values are treated as empty strings first, 
            # preventing errors when .str.upper() is called.
            df.loc[:, col] = df[col].fillna('').str.upper()
    return df

faculty_columns_to_uppercase = ['faculty']
institution_columns_to_uppercase = ['institution_name', 'body_type']
prodi_columns_to_uppercase = ['faculty']
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

# 6. Handle null/empty columns
# hapus instansi apabila tidak memiliki prodi atau fee
print("Row count before institution discard:", len(df_institution))
df_institution = df_institution[
    df_institution['fee'].notna() &                     # Check for non-null values
    df_institution['fee'].astype(str).str.strip().ne('') # Check for non-empty/non-whitespace strings
]
print("Row count after institution discard:", len(df_institution))
# unknown amount to -1
df_institution['student_amount'] = df_institution['student_amount'].fillna(-1)
df_institution['lecturer_amount'] = df_institution['lecturer_amount'].fillna(-1)

# check null columns
faculty_null_columns = df_faculty.columns[df_faculty.isnull().any()].tolist()
institution_null_columns = df_institution.columns[df_institution.isnull().any()].tolist()
prodi_null_columns = df_prodi.columns[df_prodi.isnull().any()].tolist()
if faculty_null_columns or institution_null_columns or prodi_null_columns:
    print(" Faculty null columns:", faculty_null_columns)
    print(" Institution null columns:", institution_null_columns)
    print(" Prodi null columns:", prodi_null_columns)
else:
    print("✅ Tidak ada kolom NULL")

# deskripsi kosong ubah menjadi "-"
df_institution['description'] = df_institution['description'].fillna('-')
df_institution['description'] = df_institution['description'].replace(r'^\s*$', '-', regex=True)
df_institution['student_amount'] = df_institution['student_amount'].fillna(-1)
df_institution['lecturer_amount'] = df_institution['lecturer_amount'].fillna(-1)

# 7. Simpan hasil bersih ke file baru
df_faculty.to_csv(BASE_PATH + "faculty_clean.csv", index=False)
df_institution.to_csv(BASE_PATH + "institution_clean.csv", index=False)
df_prodi.to_csv(BASE_PATH + "prodi_clean.csv", index=False)
print("🎉 Preprocessing selesai! Data disimpan di unirank_nasional_clean.csv")
