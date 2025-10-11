import pandas as pd
import os
import re

WILAYAH_TO_ROMAN = {
    1: 'I', 2: 'II', 3: 'III', 4: 'IV', 5: 'V', 6: 'VI', 7: 'VII',
    8: 'VIII', 9: 'IX', 10: 'X', 11: 'XI', 12: 'XII', 13: 'XIII',
    14: 'XIV', 15: 'XV', 16: 'XVI'
}
ROMAN_TO_PROVINCE = {
    'I': 'SUMATERA UTARA',
    'II': 'SUMATERA SELATAN, LAMPUNG, BENGKULU, KEPULAUAN BANGKA BELITUNG',
    'III': 'DKI JAKARTA',
    'IV': 'JAWA BARAT, BANTEN',
    'V': 'DAERAH ISTIMEWA YOGYAKARTA',
    'VI': 'JAWA TENGAH',
    'VII': 'JAWA TIMUR',
    'VIII': 'BALI DAN NUSA TENGGARA BARAT',
    'IX': 'SULAWESI SELATAN, SULAWESI TENGGARA, SULAWESI BARAT',
    'X': 'SUMATERA BARAT, JAMBI',
    'XI': 'KALIMANTAN',
    'XII': 'MALUKU DAN MALUKU UTARA',
    'XIII': 'ACEH',
    'XIV': 'PAPUA',
    'XV': 'NUSA TENGGARA TIMUR',
    'XVI': 'GORONTALO, SULAWESI UTARA, SULAWESI TENGAH'
}

def clean_dataframe_text(df):
    # hapus teks dalam tanda kurung, ambil teks sblm koma, ubah ke uppercase, dan trim spasi
    df_clean = df.copy()
    for col in df_clean.select_dtypes(include=['object']).columns:
        df_clean[col] = df_clean[col].str.split(',').str[0].replace(r'\s*\([^)]*\)', '', regex=True).str.upper().str.strip()
    return df_clean

def preprocess_institutions(raw_file_path, clean_file_path):
    print("--PREPROCESS INSTITUSI--")
    df = pd.read_csv(raw_file_path)

    df['wilayah_num'] = pd.to_numeric(df['wilayah'], errors='coerce')
    df.dropna(subset=['wilayah_num'], inplace=True) # Buang baris yang wilayahnya tidak valid
    df['wilayah_num'] = df['wilayah_num'].astype(int)

    df_clean = df[df['wilayah_num'].between(1, 16)].copy()
    print(f"Ditemukan {len(df_clean)} institusi PTS di wilayah 1-16.")

    df_clean['wilayah_lldikti'] = df_clean['wilayah_num'].map(WILAYAH_TO_ROMAN)
    df_clean['wilayah_kerja'] = df_clean['wilayah_lldikti'].map(ROMAN_TO_PROVINCE)

    df_clean = clean_dataframe_text(df_clean)

    initial_rows = len(df_clean)
    df_clean.drop_duplicates(subset=['institution_name', 'wilayah_lldikti'], inplace=True)
    print(f"Menghapus {initial_rows - len(df_clean)} baris duplikat.")

    final_columns = [
        'institution_code', 'institution_name', 'akreditasi_institusi',
        'wilayah_lldikti', 'wilayah_kerja'
    ]
    df_final = df_clean[final_columns]

    df_final.to_csv(clean_file_path, index=False, encoding='utf-8-sig')
    # print jumlah institusi
    print(f"Total institusi bersih: {len(df_final)}\n")
    return df_final

def preprocess_prodi(df_prodi_raw_path, df_institutions_clean, clean_file_path):
    df_prodi = pd.read_csv(df_prodi_raw_path)

    # 1. ambil prodi berdasarkan atributnya sendiri (jenjang dan wilayah).
    df_prodi['wilayah_num'] = pd.to_numeric(df_prodi['wilayah'], errors='coerce')
    df_prodi.dropna(subset=['wilayah_num'], inplace=True)
    df_prodi['wilayah_num'] = df_prodi['wilayah_num'].astype(int)

    is_s1 = df_prodi['jenjang'].str.strip().str.upper() == 'S1'
    is_pts_region = df_prodi['wilayah_num'].between(1, 16)
    df_prodi_filtered = df_prodi[is_s1 & is_pts_region].copy()
    print(f"Ditemukan {len(df_prodi_filtered)} prodi S1 di wilayah PTS (1-16).")
    
    # bersihkan nama institusi prodi (sama seperti di institusi)
    df_prodi_filtered['parent_institution_name'] = (
        df_prodi_filtered['parent_institution_name']
        .str.split(',')
        .str[0]
        .str.replace(r'\s*\([^)]*\)', '', regex=True)
        .str.upper()
        .str.strip()
    )
    
    # 3. Buat peta dari institusi bersih (Nama Bersih -> Kode Bersih).
    inst_clean_map = pd.Series(
        df_institutions_clean.institution_code.values, 
        index=df_institutions_clean.institution_name
    ).to_dict()

    # 4. cocokin dengan institusi bersih
    df_prodi_filtered['institution_code'] = df_prodi_filtered['parent_institution_name'].map(inst_clean_map)
    
    # 5. tangani prodi yang gagal dicocokkan.
    unmatched_count = df_prodi_filtered['institution_code'].isnull().sum()
    if unmatched_count > 0:
        print(f"Ditemukan {unmatched_count} prodi yang tidak cocok.")
        
        unmatched_rows = df_prodi_filtered['institution_code'].isnull()
        unmatched_inst_names = df_prodi_filtered.loc[unmatched_rows, 'parent_institution_name']


        normalized_for_code = unmatched_inst_names.str.upper().str.replace(r'\s+', ' ', regex=True)
        placeholder = 'BANPT-' + normalized_for_code
        
        df_prodi_filtered['institution_code'] = df_prodi_filtered['institution_code'].fillna(placeholder)

    # 6. Hapus duplikat dan buat 'prodi_code'
    # Membersihkan kolom lain seperti prodi_name
    df_prodi_filtered['prodi_name'] = clean_dataframe_text(df_prodi_filtered[['prodi_name']])['prodi_name']
    
    initial_rows = len(df_prodi_filtered)
    df_prodi_filtered.drop_duplicates(subset=['prodi_name', 'institution_code'], inplace=True)
    print(f"Menghapus {initial_rows - len(df_prodi_filtered)} baris duplikat.")
    
    
    # 7. SIMPAN: Pilih kolom final dan simpan ke CSV.
    final_columns = ['prodi_name', 'jenjang', 'akreditasi_prodi', 'institution_code']
    df_final = df_prodi_filtered[final_columns]
    df_final.to_csv(clean_file_path, index=False, encoding='utf-8-sig')
    print(f"Total prodi bersih: {len(df_final)}\n")
    return df_final

if __name__ == "__main__":
    base_folder = "csv_result/"
    
    raw_inst_file = os.path.join(base_folder, "banpt_institution.csv")
    raw_prodi_file = os.path.join(base_folder, "banpt_prodi.csv")

    clean_inst_file = os.path.join(base_folder, "banpt_institution_clean.csv")
    clean_prodi_file = os.path.join(base_folder, "banpt_prodi_clean.csv")
    
    if os.path.exists(raw_inst_file) and os.path.exists(raw_prodi_file):
        df_clean_institutions = preprocess_institutions(raw_inst_file, clean_inst_file)
        preprocess_prodi(raw_prodi_file, df_clean_institutions, clean_prodi_file)
    else:
        print(f"Error: Pastikan file '{raw_inst_file}' dan '{raw_prodi_file}' ada.")