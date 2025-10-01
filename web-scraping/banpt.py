# link prodi: https://www.banpt.or.id/direktori/prodi/pencarian_prodi.php
# link institusi: https://www.banpt.or.id/direktori/institusi/pencarian_institusi.php
# hasil akhir adalah 2 dataframe dengan kolom: 
# 1. institution_code (act as primary key, increasing integer) | institution_name | akreditasi_institusi | wilayah
# 2. prodi_code (act as primary key, increasing integer) prodi_code | prodi_name | jenjang | akreditasi_prodi | institution_code (sama kyk yg di dataframe pertama)

# Yang perlu di cleaning:
# 1. Pastikan institusi yang diambil hanya PTS (wilayah 01-16)
# 2. Pastikan prodi yang diambil hanya S1

import requests
import pandas as pd
from bs4 import BeautifulSoup
import time

# Konfigurasi
INSTITUTION_API_URL = "https://www.banpt.or.id/direktori/model/dir_aipt/get_data_institusi.php"
PRODI_API_URL = "https://www.banpt.or.id/direktori/model/dir_prodi/get_data_prodi.php"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

def normalize_name(name):
    # Normalisasi nama institusi untuk pencocokan
    return name.split(',')[0].lower().strip()

def preprocess_df(df, df_name="DataFrame"):
    """
    1. Uppercase semua text
    2. Hapus semua kurung dan isinya
    3. TRIM semua text
    4. Hapus duplikat
    5. Cek kolom NULL
    """
    df = df.copy()
    initial_rows = len(df)

    print(f"Membersihkan '{df_name}' ({initial_rows} baris)")
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.replace(r'\s*\([^)]*\)', '', regex=True).str.upper().str.strip()

    #is_duplicate = df.duplicated(keep=False)
    # if is_duplicate.any():
    #     print("\n--- Baris Duplikat yang Ditemukan ---")
    #     print(df[is_duplicate].sort_values(by=list(df.columns)).to_string())
    #     print("--------------------------------------------------\n")

    # Hapus duplikat 
    df.drop_duplicates(inplace=True)
    rows_after_dedup = len(df)
    print(f"Menghapus {initial_rows - rows_after_dedup} baris duplikat.")
    
    # Cek  data NULL
    null_rows_df = df[df.isnull().any(axis=1)]
    if not null_rows_df.empty:
        print(f"\n {len(null_rows_df)} null")
        print(null_rows_df.to_string())
        
    else:
        print("Tidak ditemukan baris dengan data kosong.")

    print(f"Jumlah baris setelah pembersihan: {len(df)}")
    print("-------------------------------------------")
    
    return df

def scrape_instansi(api_url, headers):
    """
    DataFrame yang bakal dihasilkan:
    1. DataFrame instansi di Indonesia
    """
    all_institutions = []
    params = {'_': int(time.time() * 1000)}

    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        json_data = response.json()
        full_database = json_data.get('data', [])
        print(f"Berhasil mendapatkan {len(full_database)} data institusi")
    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"Gagal mengambil data institusi dari API: {e}")
        return None
    
    
    for item in full_database:
        try:
            institution_name = item[0]
            akreditasi_institusi = item[1]
            wilayah = item[4]
            
            # if wilayah != '07': # Hapus kalau mau semua wilayah
            #     continue
            
            soup = BeautifulSoup(institution_name, 'html.parser')
            institution_name = soup.get_text(strip=True)
            
            all_institutions.append({
                'institution_name': institution_name,
                'normalized_name': normalize_name(institution_name),
                'akreditasi_institusi': akreditasi_institusi,
                'wilayah': f"Wilayah {wilayah}"
            })
        except IndexError:
            continue
    
    df = pd.DataFrame(all_institutions)
    df.insert(0, 'institution_code', range(1, len(df) + 1))
    return df

def scrape_prodi(api_url, headers, df_institutions):
    """
    Ada 2 DataFrame yang bakal dihasilkan:
    1. DataFrame prodi yang berhasil dicocokkan dengan institusi
    2. DataFrame debug lengkap untuk semua prodi S1 yang berhasil cocok dan tidak dengan institusi
    """

    all_prodi_accepted = []
    all_prodi_debug_report = []

    payload = {'length': '-1'}
    try:
        response = requests.post(api_url, headers=headers, data=payload, timeout=180)
        response.raise_for_status()
        json_data = response.json()
        full_prodi_database = json_data.get('data', [])
        print(f"Berhasil mendapatkan {len(full_prodi_database)} data prodi")
    except Exception as e:
        print(f"Gagal mengambil data prodi dari API: {e}")
        return None, None
    
    institution_map = pd.Series(df_institutions.institution_code.values, index=df_institutions.normalized_name).to_dict()
    
    for prodi_item in full_prodi_database:
        try:
            jenjang = prodi_item[2]
            #prodi_wilayah_id = prodi_item[3]

            # # 1. hanya proses prodi S1 dari Wilayah 07
            # if not (jenjang.strip().upper() == 'S1' and prodi_wilayah_id == '07'):
            #     continue

            parent_institution_name = prodi_item[0]
            prodi_name = prodi_item[1]
            akreditasi_prodi = prodi_item[6]
            
            debug_entry = {
                'institution_name': parent_institution_name,
                'prodi_name': prodi_name,
                'jenjang': jenjang,
                'akreditasi_prodi': akreditasi_prodi
            }

            normalized_parent_name = normalize_name(parent_institution_name)
            institution_code = institution_map.get(normalized_parent_name)
            
            if institution_code:
                all_prodi_accepted.append({
                    'prodi_name': prodi_name,
                    'jenjang': jenjang,
                    'akreditasi_prodi': akreditasi_prodi,
                    'institution_code': institution_code
                })
                debug_entry['status_pencocokan'] = 'Berhasil'
            else:
                debug_entry['status_pencocokan'] = 'Gagal Cocok'

            all_prodi_debug_report.append(debug_entry)
        except IndexError:
            continue

    df_prodi_accepted = pd.DataFrame(all_prodi_accepted) # DF prodi untuk yang cocok
    
    if not df_prodi_accepted.empty:
        df_prodi_accepted.insert(0, 'prodi_code', range(1, len(df_prodi_accepted) + 1))

    df_prodi_debug = pd.DataFrame(all_prodi_debug_report) # DF prodi debug untuk yang cocok dan tidak
    return df_prodi_accepted, df_prodi_debug

if __name__ == "__main__":
    # Scraping data
    df_institutions_scrap = scrape_instansi(INSTITUTION_API_URL, HEADERS)
    
    if df_institutions_scrap is not None:
        df_prodi_scrap, df_prodi_debug_report = scrape_prodi(PRODI_API_URL, HEADERS, df_institutions_scrap)
        
        if df_prodi_scrap is not None and not df_prodi_scrap.empty:
            s1_institution_codes = df_prodi_scrap['institution_code'].unique()
            df_institutions_final = df_institutions_scrap[df_institutions_scrap['institution_code'].isin(s1_institution_codes)]
            
            df_prodi_final = df_prodi_scrap

            # Preprocess dataframes
            df_institutions_clean = preprocess_df(df_institutions_final, "Institusi")
            df_prodi_clean = preprocess_df(df_prodi_final, "Prodi")
            df_prodi_debug_clean = preprocess_df(df_prodi_debug_report, "Debug Prodi")

            base_folder = "./csv_result"

            inst_filename = base_folder + '1_institution_code.csv'
            prodi_filename = base_folder + '2_prodi_code.csv'
            debug_filename = base_folder + '3_debug_prodi_jatim.csv'
            
            # Simpan DataFrame yang SUDAH DIBERSIHKAN
            df_institutions_clean.drop(columns=['normalized_name']).to_csv(inst_filename, index=False, encoding='utf-8-sig')
            df_prodi_clean.to_csv(prodi_filename, index=False, encoding='utf-8-sig')
            df_prodi_debug_clean.to_csv(debug_filename, index=False, encoding='utf-8-sig')
