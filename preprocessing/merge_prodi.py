import pandas as pd
import os
import re
from googletrans import Translator, constants
from tqdm import tqdm
translator = Translator()

# INI KATA-KATA UMUM YANG INGIN DIHAPUS DARI NAMA PRODI
WORDS_TO_REMOVE = [
    'TEKNIK', 'ILMU', 'DAN', 'PENDIDIKAN'
]

# Prioritas Sumber: Quipper (0) > Rencanamu (1) > BAN-PT (2)
SOURCE_PRIORITY = {'quipper': 0, 'rencanamu': 1, 'banpt': 2}

def normalize_prodi_name(series):
    # 1. Pembersihan dasar
    cleaned = (series.astype(str).str.split(',')
                    .str[0]
                    .str.replace(r'\s*\([^)]*\)', '', regex=True)
                    .str.upper().str.strip())
    
    # 2. Hapus kata-kata "noise" - INI BUAT HAPUS YG GELARAN, JURUSAN, PRODI, DLL
    generic_words = [r'\bS1\b', r'\bSARJANA\b', r'\bPROGRAM STUDI\b', r'\bPRODI\b', r'\bJURUSAN\b', r'\bPROGRAM\b']
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

# translate all prodi to indonesian
unique_prodi_series = pd.concat([
    df_prodi_quipper['prodi'], 
    df_prodi_rencanamu['prodi'], 
    df_prodi_banpt['prodi_name']
]).dropna().astype(str).drop_duplicates()

print(f"Total unique prodi names to translate: {len(unique_prodi_series)}")

tqdm.pandas(desc="Translating Program Names")
detected_langs_series = unique_prodi_series.progress_apply(lambda x: translator.translate(x, dest='id').text)
print("Translation completed.")

debug_df = pd.DataFrame({
    'prodi_name': unique_prodi_series,  # Use the original list of unique names
    'indonesian_name': detected_langs_series.values, # Use the detection results
})
print(debug_df)

mapping_series = debug_df.set_index('prodi_name')['indonesian_name']

# df_prodi_quipper['prodi_slug'] = df_prodi_quipper['prodi'].map(mapping_series)
print("Mapping prodi names to Indonesian...")
print("Mapping quipper prodi names...")
df_prodi_quipper['prodi'] = df_prodi_quipper['prodi'].map(mapping_series)
print("Mapping rencanamu prodi names...")
df_prodi_rencanamu['prodi'] = df_prodi_rencanamu['prodi'].map(mapping_series)
print("Mapping banpt prodi names...")
df_prodi_banpt['prodi_name'] = df_prodi_banpt['prodi_name'].map(mapping_series)

# df_prodi_quipper['prodi'] = df_prodi_quipper['prodi'].apply(lambda x: translator.translate(x, dest='id').text)
# df_prodi_rencanamu['prodi'] = df_prodi_rencanamu['prodi'].apply(lambda x: translator.translate(x, dest='id').text)
# df_prodi_banpt['prodi'] = df_prodi_banpt['prodi'].apply(lambda x: translator.translate(x, dest='id').text)

map_rencanamu_code_to_name = pd.Series(df_inst_rencanamu.institution_name.values, index=df_inst_rencanamu.institution_code).to_dict()
map_quipper_code_to_name = pd.Series(df_inst_quipper.institution_name.values, index=df_inst_quipper.institution_code).to_dict()
map_banpt_code_to_name = pd.Series(df_inst_banpt.institution_name.values, index=df_inst_banpt.institution_code).to_dict()


required_cols = ['institution_name', 'prodi_name', 'edu_level', 'accreditation', 'faculty', 'source', 
                 'quipper_code', 'rencanamu_code', 'banpt_code', 'pddikti_code']

# A. Proses RENCANAMU
df_prodi_rencanamu['institution_name'] = df_prodi_rencanamu['institution_code'].map(map_rencanamu_code_to_name)
df_prodi_rencanamu.rename(columns={'prodi': 'prodi_name', 'akreditasi_prodi': 'accreditation'}, inplace=True)
df_prodi_rencanamu['faculty'] = df_prodi_rencanamu['faculty'].replace('UNKNOWN', '-').fillna('-')
df_prodi_rencanamu['edu_level'] = 'S1'
df_prodi_rencanamu['source'] = 'rencanamu'
df_prodi_rencanamu['rencanamu_code'] = df_prodi_rencanamu['institution_code'] 
processed_rencanamu = df_prodi_rencanamu.drop(columns=['institution_code'], errors='ignore')

# B. Proses QUIPPER
df_prodi_quipper['institution_name'] = df_prodi_quipper['institution_code'].map(map_quipper_code_to_name)
df_prodi_quipper.rename(columns={'prodi': 'prodi_name'}, inplace=True)
df_prodi_quipper['accreditation'] = '-' 
df_prodi_quipper['edu_level'] = 'S1'
df_prodi_quipper['source'] = 'quipper'
df_prodi_quipper['faculty'] = df_prodi_quipper['faculty'].fillna('-')
df_prodi_quipper['quipper_code'] = df_prodi_quipper['institution_code'] 
processed_quipper = df_prodi_quipper.drop(columns=['institution_code'], errors='ignore')

# C. Proses BAN-PT
df_prodi_banpt['institution_name'] = df_prodi_banpt['institution_code'].map(map_banpt_code_to_name)
df_prodi_banpt.rename(columns={'jenjang': 'edu_level', 'akreditasi_prodi': 'accreditation'}, inplace=True)
df_prodi_banpt['faculty'] = '-'
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
    'prodi_name': 'first',
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
    'prodi_name_normalized', 
    'edu_level', 
    'accreditation', 
]

df_final = df_final.reindex(columns=final_columns)
df_final.rename(columns={'prodi_name_normalized': 'prodi'}, inplace=True)
output_path = BASE_PATH + "merged_prodi.csv"
df_final.to_csv(output_path, index=False, encoding='utf-8-sig')