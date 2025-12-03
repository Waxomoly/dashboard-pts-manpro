import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from scipy.cluster.hierarchy import linkage, fcluster
import os
# import matplotlib.pyplot as plt # Dihapus karena tidak memerlukan visualisasi

# ============================
# PENGATURAN FILE
# ============================
csv_filename = "merged_institutions.csv"
output_file = "hasil_clustering_dua_level_final.csv"

# ============================
# 1. LOAD & CLEAN DATA
# ============================

# Mengakses file CSV secara langsung
try:
    df = pd.read_csv(csv_filename)
except FileNotFoundError:
    print(f"Error: File '{csv_filename}' tidak ditemukan. Pastikan file berada di direktori yang sama.")
    exit()

print(f"Data awal dimuat: {len(df)} baris.")

# Menghapus institusi yang tidak memiliki data akreditasi atau biaya yang valid
# Memastikan 'average_yearly_fee' > 0
df_clean = df[df['average_yearly_fee'] > 0].copy()

# Mendefinisikan nilai akreditasi yang tidak valid
invalid_accreditation = ['-', '-1.0', np.nan]
# Menghapus baris di mana 'campus_accreditation' adalah salah satu dari nilai yang tidak valid
df_clean = df_clean[~df_clean['campus_accreditation'].astype(str).isin(invalid_accreditation)].copy()

# Mengganti NaN pada 'province' dengan string kosong sementara sebelum standardisasi
df_clean['province'] = df_clean['province'].fillna('')

# Standardisasi penulisan Akreditasi (huruf kapital dan tanpa spasi ekstra)
df_clean['campus_accreditation'] = df_clean['campus_accreditation'].str.upper().str.strip()

# Standardisasi penulisan Provinsi untuk konsistensi
df_clean['province'] = df_clean['province'].str.replace('D.K.I. JAKARTA', 'DKI JAKARTA', regex=False)
df_clean['province'] = df_clean['province'].str.replace('DI YOGYAKARTA', 'D.I. YOGYAKARTA', regex=False)
df_clean['province'] = df_clean['province'].str.replace('DAERAH ISTIMEWA YOGYAKARTA', 'D.I. YOGYAKARTA', regex=False)
df_clean['province'] = df_clean['province'].str.replace('JAWA TIMUR', 'JAWA TIMUR', regex=False)
df_clean['province'] = df_clean['province'].str.strip() # Menghapus spasi di awal/akhir

print(f"Data setelah pembersihan: {len(df_clean)} baris.")

# --- Fitur Akreditasi ---
# Menggunakan mapping skor yang konsisten: Unggul(5) > A(4) > Baik Sekali(3) > B(2) > Baik(1)
acc_map = {
    "UNGGUL": 5, "A": 4, "BAIK SEKALI": 3, "B": 2, "BAIK": 1
}
# Menghitung skor akreditasi, mengisi 0 untuk nilai yang tidak terdaftar/valid
df_clean["acc_score"] = df_clean["campus_accreditation"].map(acc_map).fillna(0)

# --- Fitur Biaya Tahunan ---
df_clean["cost_per_year"] = df_clean["average_yearly_fee"]

# ============================
# 2. CLUSTERING LEVEL 1: PENENTUAN LOKASI BESAR
# ============================

# Menentukan provinsi dengan jumlah institusi terbanyak sebagai "Lokasi Besar"
# Menggunakan value_counts dan idxmax untuk mendapatkan nama provinsi
top_province = df_clean["province"].value_counts().idxmax()
print(f"\n--- Clustering Level 1: Lokasi Besar ---")
print(f"Lokasi Besar Terpilih (Provinsi dengan Institusi terbanyak): {top_province}")

# Filter data untuk Lokasi Besar terpilih
cluster1_df = df_clean[df_clean["province"] == top_province].copy()

# ============================
# 3. CLUSTERING LEVEL 2 (INSTITUSI)
# ============================

sub_features = ["acc_score", "cost_per_year"]
cluster1_sub = cluster1_df[sub_features].copy()

# Standardisasi fitur (Penting untuk Hierarchical Clustering)
scaler2 = StandardScaler()
scaled_sub = scaler2.fit_transform(cluster1_sub)

print(f"Melakukan Hierarchical Clustering pada {len(cluster1_df)} institusi di {top_province}...")

# Hierarchical Clustering (Metode Ward - meminimalkan varians di dalam setiap klaster)
Z2 = linkage(scaled_sub, method="ward")

# Pemotongan dendrogram untuk 3 klaster (sesuai permintaan)
# criterion="maxclust" digunakan untuk menentukan jumlah klaster maksimum
NUM_CLUSTERS = 3
cluster1_df["cluster_lvl2"] = fcluster(Z2, NUM_CLUSTERS, criterion="maxclust")

print(f"Institusi dikelompokkan menjadi {NUM_CLUSTERS} klaster.")

# ============================
# 4. RINGKASAN & SAVE CSV
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
print(cluster_summary.to_markdown(index=False, numalign="left", stralign="left", floatfmt=".2f"))

# Merge hasil cluster level 2 kembali ke DataFrame awal (df_clean)
# Menggunakan index untuk merge agar lebih aman
df_clean = df_clean.merge(
    cluster1_df[["cluster_lvl2"]],
    left_index=True,
    right_index=True,
    how="left"
)
df_clean = df_clean.rename(columns={'cluster_lvl2': 'cluster_institusi_lvl2'})

# Menandai seluruh institusi di Lokasi Besar sebagai "Cluster Level 1" = 1
# Institusi di luar top_province akan memiliki nilai NaN di kolom ini
df_clean['cluster_lokasi_lvl1'] = np.where(df_clean['province'] == top_province, 1, np.nan)


# Menyimpan hasil akhir ke CSV
df_clean.to_csv(output_file, index=False)

print(f"\nSelesai! Hasil pengelompokan (termasuk kolom cluster_institusi_lvl2) disimpan sebagai: {output_file}")