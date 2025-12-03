import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from scipy.cluster.hierarchy import linkage, fcluster
import os

# ============================
# PENGATURAN FILE DAN KLUSTER
# ============================
# Ganti dengan path yang benar jika file ada di subfolder (misalnya: "csv_result/merged_institutions.csv")
csv_filename = "merged_institutions.csv" 
output_file = "hasil_clustering_dua_level_final_detailed.csv"
NUM_CLUSTERS = 3 # Jumlah klaster yang diinginkan untuk Level 2

# ============================
# 1. LOAD & CLEAN DATA
# ============================

try:
    df = pd.read_csv(csv_filename)
except FileNotFoundError:
    print(f"Error: File '{csv_filename}' tidak ditemukan. Pastikan file berada di direktori yang benar.")
    exit()

print(f"Data awal dimuat: {len(df)} baris.")

# Pembersihan Data
df_clean = df[df['average_yearly_fee'] > 0].copy()
invalid_accreditation = ['-', '-1.0', np.nan]
df_clean = df_clean[~df_clean['campus_accreditation'].astype(str).isin(invalid_accreditation)].copy()
df_clean['province'] = df_clean['province'].fillna('')

# Standardisasi Data dan Feature Engineering
df_clean['campus_accreditation'] = df_clean['campus_accreditation'].str.upper().str.strip()
df_clean['province'] = df_clean['province'].str.replace('D.K.I. JAKARTA', 'DKI JAKARTA', regex=False)
df_clean['province'] = df_clean['province'].str.replace('DI YOGYAKARTA', 'D.I. YOGYAKARTA', regex=False)
df_clean['province'] = df_clean['province'].str.replace('DAERAH ISTIMEWA YOGYAKARTA', 'D.I. YOGYAKARTA', regex=False)
df_clean['province'] = df_clean['province'].str.replace('JAWA TIMUR', 'JAWA TIMUR', regex=False).str.strip()

acc_map = {"UNGGUL": 5, "A": 4, "BAIK SEKALI": 3, "B": 2, "BAIK": 1}
# Membuat fitur numerik acc_score (5=Unggul, 1=Baik)
df_clean["acc_score"] = df_clean["campus_accreditation"].map(acc_map).fillna(0)
df_clean["cost_per_year"] = df_clean["average_yearly_fee"]

print(f"Data setelah pembersihan: {len(df_clean)} baris.")

# ============================
# 2. CLUSTERING LEVEL 1: LOKASI BESAR
# ============================

# Menentukan provinsi dengan jumlah institusi terbanyak
top_province = df_clean["province"].value_counts().idxmax()
print(f"\n--- Clustering Level 1: Lokasi Besar ---")
print(f"Lokasi Besar Terpilih (Provinsi dengan Institusi terbanyak): {top_province}")

cluster1_df = df_clean[df_clean["province"] == top_province].copy()

# ============================
# 3. CLUSTERING LEVEL 2 (INSTITUSI)
# ============================

sub_features = ["acc_score", "cost_per_year"]
cluster1_sub = cluster1_df[sub_features].copy()

# Standardisasi fitur
scaler2 = StandardScaler()
scaled_sub = scaler2.fit_transform(cluster1_sub)

print(f"Melakukan Hierarchical Clustering pada {len(cluster1_df)} institusi di {top_province}...")

# Hierarchical Clustering (Metode Ward)
Z2 = linkage(scaled_sub, method="ward")

# Pemotongan klaster
cluster1_df["cluster_lvl2"] = fcluster(Z2, NUM_CLUSTERS, criterion="maxclust")

print(f"Institusi dikelompokkan menjadi {NUM_CLUSTERS} klaster.")

# ============================
# 4. RINGKASAN & OUTPUT CSV
# ============================

# Menghitung ringkasan klaster
cluster_summary = cluster1_df.groupby('cluster_lvl2').agg(
    Avg_Acc_Score=('acc_score', 'mean'),
    Avg_Yearly_Fee=('cost_per_year', 'mean'),
    Total_Institutions=('institution_name', 'count')
).reset_index()
cluster_summary['cluster_lvl2'] = 'Cluster ' + cluster_summary['cluster_lvl2'].astype(str)

print("\n--- Ringkasan Clustering Level 2 di Provinsi Terpilih ---")
print(f"Provinsi: {top_province}")

# Mencetak ringkasan dengan error handling untuk library 'tabulate'
try:
    print(cluster_summary.to_markdown(index=False, numalign="left", stralign="left", floatfmt=".2f"))
except ImportError:
    print("\n[PERINGATAN: Library 'tabulate' tidak ditemukan. Mencetak output dalam format default.]")
    print(cluster_summary.to_string(index=False, float_format="{:.2f}".format))
    print("\nSARAN: Jalankan 'pip install tabulate' di terminal Anda untuk mendapatkan format tabel yang rapi.")


# --- Penyiapan DataFrame Akhir untuk CSV ---
# Merge hasil cluster
df_clean = df_clean.merge(
    cluster1_df[["cluster_lvl2"]],
    left_index=True,
    right_index=True,
    how="left"
)
df_clean = df_clean.rename(columns={'cluster_lvl2': 'cluster_institusi_lvl2'})
df_clean['cluster_lokasi_lvl1'] = np.where(df_clean['province'] == top_province, 1, np.nan)

# Kolom yang ingin disimpan di output CSV akhir (membuatnya lebih rapi)
COLUMNS_TO_KEEP = [
    'institution_name',
    'province',
    'campus_accreditation', # Akreditasi Asli
    'acc_score',            # Skor Akreditasi Numerik (5=Unggul, 1=Baik)
    'cost_per_year',        # Biaya Tahunan (Digunakan dalam Clustering)
    'cluster_lokasi_lvl1',
    'cluster_institusi_lvl2'
]

# Ambil hanya kolom yang relevan
df_output = df_clean[COLUMNS_TO_KEEP].copy()

# Menyimpan hasil akhir ke CSV
df_output.to_csv(output_file, index=False)

print(f"\nSelesai! Hasil pengelompokan yang lebih detail disimpan sebagai: {output_file}")