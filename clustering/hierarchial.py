import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import os

# --- 1. KONFIGURASI PATH ---
path_folder = r"C:\laragon\www\dashboard-pts-manpro\csv_result"
file_path = os.path.join(path_folder, "merged_institutions.csv")
output_path = os.path.join(path_folder, "hierarchial_hasil_clustering_final_revisi.csv")

print(f"Membaca file dari: {file_path}")
try:
    df = pd.read_csv(file_path, engine='python', on_bad_lines='skip')
except FileNotFoundError:
    print("Error: File tidak ditemukan. Pastikan path benar.")
    exit()

# --- 2. DATA CLEANING & NORMALISASI ---

# A. Normalisasi Nama Provinsi (PERBAIKAN SINTAKSIS DI SINI)
norm_map = {
    'D.I. YOGYAKARTA': 'DI YOGYAKARTA',
    'DAERAH ISTIMEWA YOGYAKARTA': 'DI YOGYAKARTA',
    'DIY': 'DI YOGYAKARTA',
    'YOGYAKARTA': 'DI YOGYAKARTA',
    'D.K.I. JAKARTA': 'DKI JAKARTA',
    'JKT': 'DKI JAKARTA',
    'JAKARTA': 'DKI JAKARTA',
    'NANGGROE ACEH DARUSSALAM': 'ACEH'
}

# Pastikan kolom province string dulu, lalu upper, lalu STR.strip (tambah .str)
df['province'] = df['province'].astype(str).str.upper().str.strip().replace(norm_map)

# Hapus baris jika province kosong, '-', atau 'NAN' (string)
df = df[ (df['province'] != '-') & (df['province'] != 'NAN') & (df['province'].notna()) ]

# B. Mapping Akreditasi
acc_map = {
    'A': 4, 'UNGGUL': 4,
    'B': 3, 'BAIK SEKALI': 3,
    'C': 2, 'BAIK': 2,
    '-': 1, np.nan: 1
}
df['acc_score'] = df['campus_accreditation'].str.upper().map(acc_map).fillna(1)

# C. Cleaning Biaya
df['average_yearly_fee'] = df['average_yearly_fee'].replace([-1.0, 0, -1], np.nan)
# Isi NaN dengan median per provinsi
df['average_yearly_fee'] = df['average_yearly_fee'].fillna(
    df.groupby('province')['average_yearly_fee'].transform('median')
)
df['average_yearly_fee'] = df['average_yearly_fee'].fillna(df['average_yearly_fee'].median())

# --- 3. PROSES CLUSTERING ---

def run_clustering_advanced(dataframe):
    final_results = []
    
    # Urutkan provinsi agar proses rapi
    list_provinsi = sorted(dataframe['province'].unique())
    print(f"Total Provinsi Valid: {len(list_provinsi)}") 
    
    for prov in list_provinsi:
        prov_data = dataframe[dataframe['province'] == prov].copy()
        
        # JIKA DATA SANGAT SEDIKIT (< 3 Kampus)
        if len(prov_data) < 3:
            fee = prov_data['average_yearly_fee'].mean()
            if fee > 15_000_000: label_manual = "General (Mahal)"
            else: label_manual = "General (Terjangkau)"
            prov_data['kategori_final'] = f"{prov} - {label_manual}"
            final_results.append(prov_data)
            continue
            
        # TENTUKAN JUMLAH CLUSTER
        n_clusters = 5 if len(prov_data) >= 15 else 3
        
        # SCALING & K-MEANS
        features = prov_data[['average_yearly_fee', 'acc_score']]
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)
        
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        prov_data['cluster_id'] = kmeans.fit_predict(features_scaled)
        
        # --- LABELING BARU ---
        label_map = {}
        for cid in prov_data['cluster_id'].unique():
            subset = prov_data[prov_data['cluster_id'] == cid]
            
            # 1. Label Harga
            med_fee = subset['average_yearly_fee'].median()
            
            if med_fee >= 100_000_000:    
                fee_str = "Sultan (> 100 Jt)"
            elif med_fee >= 40_000_000:   
                fee_str = "Mahal (40-100 Jt)"
            elif med_fee >= 15_000_000:
                fee_str = "Menengah (15-40 Jt)"
            elif med_fee >= 5_000_000:
                fee_str = "Terjangkau (5-15 Jt)"
            else:
                fee_str = "Sangat Hemat (< 5 Jt)"
            
            # 2. Label Akreditasi
            mode_acc = subset['acc_score'].mode()[0]
            
            if mode_acc >= 4: acc_str = "Unggul (A)"
            elif mode_acc >= 3: acc_str = "Baik Sekali (B)"
            else: acc_str = "Baik"
            
            label_map[cid] = f"{prov} - {fee_str} - {acc_str}"
            
        prov_data['kategori_final'] = prov_data['cluster_id'].map(label_map)
        final_results.append(prov_data)
        
    return pd.concat(final_results, ignore_index=True)

# --- 4. EKSEKUSI & SORTING ---
print("Sedang memproses clustering...")
df_hasil = run_clustering_advanced(df)

# SORTING
df_hasil = df_hasil.sort_values(by=['province', 'average_yearly_fee'], ascending=[True, False])

# --- 5. SIMPAN CSV ---
cols_to_save = ['institution_name', 'province', 'campus_accreditation', 'average_yearly_fee', 'kategori_final', 'link']
df_hasil[cols_to_save].to_csv(output_path, index=False)

print(f"\nSelesai! File tersimpan di: {output_path}")