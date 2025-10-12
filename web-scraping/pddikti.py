import requests
import time
import pandas as pd
import os

# --- 1. KONFIGURASI ---
# URL API untuk daftar pencarian dan detail
SEARCH_API_URL = "https://api-pddikti.kemdiktisaintek.go.id/v2/pt/search/filter"
DETAIL_API_URL_TEMPLATE = "https://api-pddikti.kemdiktisaintek.go.id/pt/detail/{}"

# Headers untuk meniru browser dan menghindari error 401
HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Origin': 'https://pddikti.kemdiktisaintek.go.id',
    'Referer': 'https://pddikti.kemdiktisaintek.go.id/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
}

# --- 2. LANGKAH AWAL: DAPATKAN TOTAL HALAMAN ---
print("Mengecek jumlah total halaman...")
semua_data_lengkap = []
total_pages = 0

try:
    # Lakukan satu request awal untuk mendapatkan metadata (termasuk total halaman)
    initial_params = {'limit': 50, 'page': 1}
    response = requests.get(SEARCH_API_URL, headers=HEADERS, params=initial_params)
    response.raise_for_status()
    initial_data = response.json()
    total_pages = initial_data.get('totalPages', 0)
    
    if total_pages == 0:
        print("Tidak ditemukan data atau total halaman. Skrip berhenti.")
        exit()

    print(f"Ditemukan total {total_pages} halaman data (dengan 50 data per halaman).")

except requests.exceptions.RequestException as e:
    print(f"Gagal melakukan request awal: {e}. Skrip berhenti.")
    exit()

# --- 3. LOOP UTAMA: MENGAMBIL SEMUA DATA ---
# Loop sebanyak total halaman yang ditemukan
for page_number in range(1, total_pages + 1):
    params = {
        'limit': 50,
        'page': page_number,
    }
    
    print(f"Mengambil daftar dari halaman {page_number}/{total_pages}...")
    
    try:
        response = requests.get(SEARCH_API_URL, headers=HEADERS, params=params)
        response.raise_for_status()
        data_halaman_ini = response.json().get('data', [])

        if not data_halaman_ini:
            print(f"  -> Halaman {page_number} kosong, melanjutkan...")
            continue

        # LOOP KEDUA: Untuk setiap kampus, ambil data detailnya
        for kampus_dasar in data_halaman_ini:
            kampus_id = kampus_dasar.get('id_sp')
            if not kampus_id:
                semua_data_lengkap.append(kampus_dasar) # Simpan data dasar jika ID tidak ada
                continue

            # Bentuk URL API detail menggunakan ID unik kampus
            detail_url = DETAIL_API_URL_TEMPLATE.format(kampus_id)
            
            try:
                # Lakukan request KEDUA ke API detail
                detail_response = requests.get(detail_url, headers=HEADERS)
                detail_response.raise_for_status()
                detail_data = detail_response.json()
                
                # Gabungkan data dasar dengan data detail
                kampus_dasar.update(detail_data)
                
            except requests.exceptions.RequestException:
                # Jika detail gagal, kita tetap simpan data dasarnya
                print(f"  -> Gagal mengambil detail untuk {kampus_dasar.get('nama_pt')}, data dasar tetap disimpan.")

            # Tambahkan data yang sudah lengkap ke list utama
            semua_data_lengkap.append(kampus_dasar)
            time.sleep(0.3) # Jeda kecil antar request detail
        
        time.sleep(0.3)
        
    except requests.exceptions.RequestException as e:
        print(f"Error pada halaman daftar {page_number}: {e}. Melanjutkan ke halaman berikutnya.")
        continue

# --- 4. MENYIMPAN DATA KE CSV ---
print(f"\nProses scraping selesai. Total data terkumpul: {len(semua_data_lengkap)}")

# --- LANGKAH PEMBERSIHAN DATA TAMBAHAN ---
print("Membersihkan data dari karakter baris baru (newline)...")
data_bersih_final = []
for data_kampus in semua_data_lengkap:
    item_bersih = {}
    for key, value in data_kampus.items():
        # Kita hanya membersihkan data yang tipenya string
        if isinstance(value, str):
            # Ganti karakter newline (\n) dan carriage return (\r) dengan spasi
            item_bersih[key] = value.replace('\n', ' ').replace('\r', ' ')
        else:
            # Jika bukan string (misalnya angka), biarkan apa adanya
            item_bersih[key] = value
    data_bersih_final.append(item_bersih)

# --- MENYIMPAN DATA KE CSV ---
if data_bersih_final:
    print("Membersihkan nilai kosong dan menyimpan data ke file CSV...")
    # Gunakan variabel yang sudah bersih total: data_bersih_final
    df = pd.DataFrame(data_bersih_final)
    
    # Ganti semua nilai string kosong ("") dengan "-"
    df.replace("", "-", inplace=True)

     # Loop melalui semua kolom yang tipenya string
    for col in df.select_dtypes(include=['object']).columns:
        # Hapus karakter kutip ganda
        df[col] = df[col].str.replace('"', '', regex=False)
        
        df[col] = df[col].str.replace(',', ' ', regex=False)
    
    # Simpan ke file CSV di folder root proyek
    nama_file_output = 'pddikti_nasional.csv'
    df.to_csv(nama_file_output, index=False, encoding='utf-8-sig')
    
    print(f"Data berhasil disimpan di: {os.path.join(os.getcwd(), nama_file_output)}")
else:
    print("Tidak ada data untuk disimpan.")