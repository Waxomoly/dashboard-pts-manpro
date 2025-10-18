import pandas as pd
import os
import re

WORDS_TO_REMOVE = [
    'TEKNIK', 'PENDIDIKAN', 'KEGURUAN', 'ILMU', 'SAINS', 'MANAJEMEN', 
    'BISNIS', 'EKONOMI', 'SOSIAL', 'POLITIK', 'HUKUM', 'ADMINISTRASI',
    'SENI', 'SASTRA', 'TERAPAN', 'AGAMA'
]
# Prioritas Sumber: Quipper (0) > Rencanamu (1) > BAN-PT (2)
SOURCE_PRIORITY = {'quipper': 0, 'rencanamu': 1, 'banpt': 2}

def normalize_prodi_name(series):
    # 1. Pembersihan dasar
    cleaned = (series.astype(str).str.split(',')
                    .str[0]
                    .str.replace(r'\s*\([^)]*\)', '', regex=True)
                    .str.upper().str.strip())
    
    # 2. Hapus kata-kata "noise"
    generic_words = [r'\bS1\b', r'\bSARJANA\b', r'\bPROGRAM STUDI\b', r'\bPRODI\b', r'\bJURUSAN\b']
    for word in generic_words:
        cleaned = cleaned.str.replace(word, '', regex=True)
        
    # 3. Hapus kata-kata umum dari daftar WORDS_TO_REMOVE
    for word in WORDS_TO_REMOVE:
        cleaned = cleaned.str.replace(r'\b' + word + r'\b', '', regex=True)
        
    # 4. Hapus spasi ganda dan rapikan
    cleaned = cleaned.str.replace(r'\s+', ' ', regex=True).str.strip()
    
    return cleaned

def aggregate_by_priority(series, priority_map):
    temp_df = pd.DataFrame({
        'value': series.dropna(),
        'source': series.dropna().index.get_level_values('source')
    })
    
    if temp_df.empty:
        return '-' # Kembalikan '-' jika tidak ada data
    
    temp_df['priority'] = temp_df['source'].map(priority_map).fillna(99)
    temp_df.sort_values(by='priority', inplace=True)
    
    return temp_df['value'].iloc[0]

def get_best_prodi_name(series):
    """Mengambil prodi_name yang paling panjang (paling lengkap) dari prioritas terbaik."""
    temp_df = pd.DataFrame({
        'name': series.astype(str).dropna(),
        'source': series.dropna().index.get_level_values('source')
    })
    
    if temp_df.empty:
        return '-'

    # 1. Tambahkan kolom prioritas sumber
    temp_df['priority'] = temp_df['source'].map(SOURCE_PRIORITY).fillna(99)
    
    # 2. Tambahkan kolom rank berdasarkan panjang nama
    #    rank(ascending=False) memberi rank 1 pada nama terpanjang
    temp_df['len_rank'] = temp_df['name'].str.len().rank(ascending=False) 
    
    # 3. Urutkan berdasarkan kedua kolom
    #    Kita urutkan berdasarkan 'priority' (angka kecil lebih baik) lalu 'len_rank' (angka kecil/rank 1 lebih baik)
    temp_df.sort_values(by=['priority', 'len_rank'], inplace=True) 
    
    return temp_df['name'].iloc[0]


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


required_cols = ['institution_name', 'prodi_name', 'edu_level', 'accreditation', 'faculty', 'source', 
                 'quipper_code', 'rencanamu_code', 'banpt_code', 'pddikti_code']

# A. Proses RENCANAMU
df_prodi_rencanamu['institution_name'] = df_prodi_rencanamu['institution_code'].map(map_rencanamu_code_to_name)
df_prodi_rencanamu.rename(columns={'prodi': 'prodi_name', 'akreditasi_prodi': 'accreditation'}, inplace=True)
df_prodi_rencanamu['faculty'] = df_prodi_rencanamu['faculty'].replace('UMUM', 'UNKNOWN').fillna('UNKNOWN')
df_prodi_rencanamu['edu_level'] = 'S1'
df_prodi_rencanamu['source'] = 'rencanamu'
df_prodi_rencanamu['rencanamu_code'] = df_prodi_rencanamu['institution_code'] 
processed_rencanamu = df_prodi_rencanamu.drop(columns=['institution_code'], errors='ignore')

# B. Proses QUIPPER
df_prodi_quipper['institution_name'] = df_prodi_quipper['institution_code'].map(map_quipper_code_to_name)
df_prodi_quipper.rename(columns={'prodi': 'prodi_name'}, inplace=True)
df_prodi_quipper['accreditation'] = 'UNKNOWN' 
df_prodi_quipper['edu_level'] = 'S1'
df_prodi_quipper['source'] = 'quipper'
df_prodi_quipper['faculty'] = df_prodi_quipper['faculty'].fillna('UNKNOWN')
df_prodi_quipper['quipper_code'] = df_prodi_quipper['institution_code'] 
processed_quipper = df_prodi_quipper.drop(columns=['institution_code'], errors='ignore')

# C. Proses BAN-PT
df_prodi_banpt['institution_name'] = df_prodi_banpt['institution_code'].map(map_banpt_code_to_name)
df_prodi_banpt.rename(columns={'jenjang': 'edu_level', 'akreditasi_prodi': 'accreditation'}, inplace=True)
df_prodi_banpt['faculty'] = 'UNKNOWN'
df_prodi_banpt['source'] = 'banpt'
df_prodi_banpt['banpt_code'] = df_prodi_banpt['institution_code']
processed_banpt = df_prodi_banpt.drop(columns=['institution_code'], errors='ignore')

for df in [processed_rencanamu, processed_quipper, processed_banpt]:
    for col in ['quipper_code', 'rencanamu_code', 'banpt_code', 'pddikti_code']:
        if col not in df.columns:
            df[col] = '-'

df_combined = pd.concat([
    processed_rencanamu.reindex(columns=required_cols), 
    processed_quipper.reindex(columns=required_cols), 
    processed_banpt.reindex(columns=required_cols)
], ignore_index=True)

df_combined['prodi_name_normalized'] = normalize_prodi_name(df_combined['prodi_name'])
print(f"\nTotal baris setelah digabung: {len(df_combined)}")

agg_funcs = {
    'quipper_code': lambda x: x[x != '-'].iloc[0] if (x != '-').any() else '-',
    'rencanamu_code': lambda x: x[x != '-'].iloc[0] if (x != '-').any() else '-',
    'banpt_code': lambda x: x[x != '-'].iloc[0] if (x != '-').any() else '-',
    'pddikti_code': 'first', 
    'prodi_name': get_best_prodi_name,
    'accreditation': lambda x: aggregate_by_priority(x.str.upper().str.strip().replace('', '-'), SOURCE_PRIORITY),
    'faculty': lambda x: aggregate_by_priority(x, SOURCE_PRIORITY),
    'edu_level': 'first', 
}

df_final = (
    df_combined
    .set_index(['institution_name', 'prodi_name_normalized', 'source'])
    .groupby(level=['institution_name', 'prodi_name_normalized'])
    .agg(agg_funcs)
)

# Kembalikan indeks ke kolom
df_final = df_final.reset_index()

print(f"Total baris setelah agregasi: {len(df_final)}")

final_columns = [
    'quipper_code', 
    'rencanamu_code', 
    'banpt_code', 
    'pddikti_code', 
    'faculty',
    'prodi_name', 
    'edu_level', 
    'accreditation', 
]

df_final = df_final.reindex(columns=final_columns)
df_final.rename(columns={'prodi_name': 'prodi'}, inplace=True)
output_path = BASE_PATH + "merged_prodi.csv"
df_final.to_csv(output_path, index=False, encoding='utf-8-sig')