import pandas as pd
import numpy as np
import re
import json

# --- 1. FUNGSI-FUNGSI BANTU ---

def bersihkan_teks(df):
    # ... (fungsi ini tidak berubah)
    print("Membersihkan teks (uppercase, trim, hapus kurung)...")
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.upper()
            df[col] = df[col].str.replace(r'\s*\(.*\)\s*', '', regex=True)
            df[col] = df[col].str.strip()
    return df

def proses_biaya(range_biaya_str):
    # =================================================================
    # FUNGSI INI YANG DIPERBARUI
    # =================================================================
    if not isinstance(range_biaya_str, str) or range_biaya_str in ['-', '']:
        return [np.nan] * 6

    # 1. Bersihkan dari 'Rp', titik, dan spasi di awal/akhir
    cleaned_str = str(range_biaya_str).replace('Rp', '').replace('.', '').strip()
    
    # 2. PENANGANAN BARU: Jika string dimulai dengan '-', hapus tanda itu
    if cleaned_str.startswith('-'):
        cleaned_str = cleaned_str[1:].strip()

    biaya_min, biaya_maks = np.nan, np.nan
    
    # 3. Lanjutkan logika seperti biasa
    if '-' in cleaned_str:
        try:
            parts = cleaned_str.split('-')
            min_str, max_str = parts[0].strip(), parts[1].strip()
            biaya_min = int(min_str) if min_str else np.nan
            biaya_maks = int(max_str) if max_str else biaya_min
        except (ValueError, IndexError): return [np.nan] * 6
    else:
        try:
            biaya_min = biaya_maks = int(cleaned_str) if cleaned_str else np.nan
        except ValueError: return [np.nan] * 6
    
    if pd.isna(biaya_min) or pd.isna(biaya_maks): return [np.nan] * 6
    
    avg_semester = (biaya_min + biaya_maks) / 2
    return [avg_semester, biaya_min, biaya_maks, avg_semester * 2, biaya_min * 2, biaya_maks * 2]


def gabung_kontak(row):
    # ... (fungsi ini tidak berubah)
    phone_parts = [str(row.get('no_tel', '')), str(row.get('no_fax', ''))]
    phone_string = ' / '.join(part for part in phone_parts if part and part not in ['-', 'NAN'])
    contact_dict = {"website": str(row.get('website', '')), "email": str(row.get('email', '')), "phone": phone_string}
    return json.dumps(contact_dict)


# --- (SISA KODE PROSES UTAMA DI BAWAH INI TIDAK ADA YANG BERUBAH) ---
try:
    df = pd.read_csv('pddikti_nasional.csv', engine='python')
    print("File 'pddikti_nasional.csv' berhasil dimuat.")
except FileNotFoundError:
    print("Error: File 'pddikti_nasional.csv' tidak ditemukan.")
    exit()

df = bersihkan_teks(df)
df.drop_duplicates(inplace=True)
df.rename(columns={'range_biaya_kuliah': 'fee'}, inplace=True)
fee_columns = ['average_semester_fee', 'starting_semester_fee', 'ending_semester_fee', 
               'average_yearly_fee', 'starting_yearly_fee', 'ending_yearly_fee']
df[fee_columns] = df['fee'].apply(lambda x: pd.Series(proses_biaya(x)))
df['contact'] = df.apply(gabung_kontak, axis=1)
rename_map = {'nama_pt': 'institution_name', 'alamat': 'address', 'kab_kota_pt': 'regency', 'provinsi_pt': 'province', 'status_pt': 'ownership'}
df.rename(columns=rename_map, inplace=True)
df.reset_index(drop=True, inplace=True)
df['pddikti_institution_code'] = df.index + 1
kolom_final_yang_diinginkan = [
    'pddikti_institution_code','institution_name','ownership','address','regency','province','contact',
    'fee', 'average_semester_fee','starting_semester_fee','ending_semester_fee','average_yearly_fee','starting_yearly_fee','ending_yearly_fee',
    'body_type','link','student_amount','lecturer_amount','description'
]
for col in kolom_final_yang_diinginkan:
    if col not in df.columns:
        df[col] = np.nan
df_final = df[kolom_final_yang_diinginkan]
nama_file_output = 'pddikti_nasional_clean.csv'
df_final.to_csv(nama_file_output, index=False, encoding='utf-8-sig')

print(f"\nPreprocessing Selesai! File final '{nama_file_output}' telah dibuat.")