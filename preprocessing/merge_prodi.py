import pandas as pd
import os
import re

WORDS_TO_REMOVE = [
    'TEKNIK', 'PENDIDIKAN', 'KEGURUAN', 'ILMU', 'SAINS', 'MANAJEMEN', 
    'BISNIS', 'EKONOMI', 'SOSIAL', 'POLITIK', 'HUKUM', 'ADMINISTRASI',
    'SENI', 'SASTRA', 'TERAPAN', 'AGAMA',

    
]

WORDS_TO_CHANGE = {
    'FOOD TECHNOLOGY': 'TEKNOLOGI PANGAN',
    
}


def normalize_prodi_name(series):
    """
    Membersihkan dan menstandardisasi nama prodi dengan menghapus kata-kata umum.
    """
    # 1. Mulai dengan pembersihan dasar dari input `series`.
    #    Di sinilah variabel 'cleaned' pertama kali dibuat.
    cleaned = (series.astype(str).str.split(',')
                   .str[0]
                   .str.replace(r'\s*\([^)]*\)', '', regex=True)
                   .str.upper().str.strip())
    
    # 2. Hapus kata-kata "noise" seperti S1, PRODI, dll.
    generic_words = [r'\bS1\b', r'\bSARJANA\b', r'\bPROGRAM STUDI\b', r'\bPRODI\b', r'\bJURUSAN\b']
    for word in generic_words:
        cleaned = cleaned.str.replace(word, '', regex=True)
        
    # 3. Hapus kata-kata umum dari daftar WORDS_TO_REMOVE
    for word in WORDS_TO_REMOVE:
        cleaned = cleaned.str.replace(r'\b' + word + r'\b', '', regex=True)
        
    # 4. Hapus spasi ganda yang mungkin muncul dan rapikan
    cleaned = cleaned.str.replace(r'\s+', ' ', regex=True).str.strip()
    
    return cleaned

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    BASE_PATH = os.path.join(parent_dir, "csv_result") + os.sep
except NameError:
    BASE_PATH = "csv_result/"

try:
    df_prodi_rencanamu = pd.read_csv(BASE_PATH + "rencanamu_prodi_preprocessed.csv")
    df_prodi_quipper = pd.read_csv(BASE_PATH + "quipper_prodi_clean.csv")
    df_prodi_banpt = pd.read_csv(BASE_PATH + "banpt_prodi_clean.csv")
    df_inst_rencanamu = pd.read_csv(BASE_PATH + "rencanamu_institutions_preprocessed.csv")
    df_inst_quipper = pd.read_csv(BASE_PATH + "quipper_institution_clean.csv")
    df_inst_banpt = pd.read_csv(BASE_PATH + "banpt_institution_clean.csv")
except FileNotFoundError as e:
    print(f"Error: File tidak ditemukan: {e.filename}")
    exit()

map_rencanamu_code_to_name = pd.Series(df_inst_rencanamu.institution_name.values, index=df_inst_rencanamu.institution_code).to_dict()
map_quipper_code_to_name = pd.Series(df_inst_quipper.institution_name.values, index=df_inst_quipper.institution_code).to_dict()
map_banpt_code_to_name = pd.Series(df_inst_banpt.institution_name.values, index=df_inst_banpt.institution_code).to_dict()

# Proses setiap sumber
# A. Proses RENCANAMU
df_prodi_rencanamu['institution_name'] = df_prodi_rencanamu['institution_code'].map(map_rencanamu_code_to_name)
df_prodi_rencanamu.rename(columns={'prodi': 'prodi_name', 'akreditasi_prodi': 'accreditation'}, inplace=True)
df_prodi_rencanamu['faculty'] = df_prodi_rencanamu['faculty'].replace('UMUM', 'UNKNOWN')
df_prodi_rencanamu['edu_level'] = 'S1'
df_prodi_rencanamu['source'] = 'rencanamu'
processed_rencanamu = df_prodi_rencanamu
# B. Proses QUIPPER
df_prodi_quipper['institution_name'] = df_prodi_quipper['institution_code'].map(map_quipper_code_to_name)
df_prodi_quipper.rename(columns={'prodi': 'prodi_name'}, inplace=True)
df_prodi_quipper['accreditation'] = '-'
df_prodi_quipper['edu_level'] = 'S1'
df_prodi_quipper['source'] = 'quipper'
processed_quipper = df_prodi_quipper
# C. Proses BAN-PT
df_prodi_banpt['institution_name'] = df_prodi_banpt['institution_code'].map(map_banpt_code_to_name)
df_prodi_banpt.rename(columns={'jenjang': 'edu_level', 'akreditasi_prodi': 'accreditation'}, inplace=True)
df_prodi_banpt['faculty'] = 'UNKNOWN'
df_prodi_banpt['source'] = 'banpt'
processed_banpt = df_prodi_banpt

df_combined = pd.concat([processed_rencanamu, processed_quipper, processed_banpt], ignore_index=True)
df_combined['faculty'] = df_combined['faculty'].fillna('UNKNOWN')
print(f"\nTotal baris setelah digabung: {len(df_combined)}")

df_combined['prodi_name_normalized'] = normalize_prodi_name(df_combined['prodi_name'])
df_combined['faculty_priority'] = df_combined['faculty'].apply(lambda x: 1 if x == 'UNKNOWN' else 0)
df_combined.sort_values(
    by=['institution_name', 'prodi_name_normalized', 'faculty_priority'], 
    ascending=True, 
    inplace=True
)
df_final = df_combined.drop_duplicates(subset=['institution_name', 'prodi_name_normalized'], keep='first')
print(f"Total baris setelah deduplikasi lanjutan: {len(df_final)}")


df_final = df_final.drop(columns=['institution_name', 'prodi_name_normalized', 'faculty_priority'])
final_columns = ['institution_code', 'faculty', 'prodi_name', 'edu_level', 'accreditation']
df_final = df_final.reindex(columns=final_columns)
df_final.rename(columns={'prodi_name': 'prodi'}, inplace=True)
output_path = BASE_PATH + "merged_prodi_final.csv"
df_final.to_csv(output_path, index=False, encoding='utf-8-sig')