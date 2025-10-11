# link prodi: https://www.banpt.or.id/direktori/prodi/pencarian_prodi.php
# link institusi: https://www.banpt.or.id/direktori/institusi/pencarian_institusi.php
# Code ini akan ambil semua data prodi dan institusi dari BAN-PT via API mereka

import requests
import pandas as pd
import time
import html


# Konfigurasi
INSTITUTION_API_URL = "https://www.banpt.or.id/direktori/model/dir_aipt/get_data_institusi.php"
PRODI_API_URL = "https://www.banpt.or.id/direktori/model/dir_prodi/get_data_prodi.php"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}


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
            institution_name = html.unescape(item[0])
            all_institutions.append({
                'institution_name': institution_name,
                'akreditasi_institusi': item[1],
                'no_sk': item[2],
                'tahun_sk': item[3],
                'wilayah': f"{item[4]}", 
                'tanggal_kadaluarsa': item[5]
            })
        except IndexError:
            continue
    
    df = pd.DataFrame(all_institutions)
    df.insert(0, 'institution_code', [f'banpt-{i}' for i in range(1, len(df) + 1)])
    return df

def scrape_prodi(api_url, headers):
    """
    DataFrame yang bakal dihasilkan:
    1. DataFrame prodi di Indonesia
    """

    all_prodi= []
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
    
    
    for item in full_prodi_database:
        try: 
            parent_institution_name = html.unescape(item[0])
            all_prodi.append({
                'parent_institution_name': parent_institution_name,
                'prodi_name': item[1],
                'jenjang': item[2],
                'wilayah': item[3],
                'no_sk': item[4],
                'tahun_sk': item[5],
                'akreditasi_prodi': item[6],
                'tanggal_kadaluarsa': item[7]

                })
            
        except IndexError:
            continue

    df_prodi = pd.DataFrame(all_prodi) # DF prodi untuk yang cocok

    return df_prodi

if __name__ == "__main__":
    base_folder = "./csv_result/"
    # Scraping data
    df_institutions_scrap = scrape_instansi(INSTITUTION_API_URL, HEADERS)
    if df_institutions_scrap is not None:
        inst_filename = base_folder + 'banpt_institution.csv'
        df_institutions_scrap.to_csv(inst_filename, index=False, encoding='utf-8-sig')
    
    df_prodi_scrap = scrape_prodi(PRODI_API_URL, HEADERS)
    if df_prodi_scrap is not None:
        prodi_filename = base_folder + 'banpt_prodi.csv'
        df_prodi_scrap.to_csv(prodi_filename, index=False, encoding='utf-8-sig')
