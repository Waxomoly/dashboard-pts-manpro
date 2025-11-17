import pandas as pd
from kmodes.kmodes import KModes
import os

# 1. LOAD DATA
print("Loading data prodi...")
# Cek path otomatis (biar aman mau dijalankan dari folder manapun)
path_root = 'csv_result/merged_prodi_final.csv'
path_inside = '../csv_result/merged_prodi_final.csv'

if os.path.exists(path_root):
    file_path = path_root
elif os.path.exists(path_inside):
    file_path = path_inside
else:
    # Default fallback ke path folder csv_result kalau script dijalankan dari root
    file_path = 'csv_result/merged_prodi_final.csv'

try:
    df = pd.read_csv(file_path)
except FileNotFoundError:
    # Coba mundur satu folder lagi kalau masih error (case script di folder clustering)
    df = pd.read_csv('../csv_result/merged_prodi_final.csv')

# 2. PILIH FITUR
features = ['faculty', 'prodi_normalized', 'edu_level']
df_cluster = df[features].copy()
df_cluster = df_cluster.fillna('Lainnya')

# 3. EKSEKUSI K-MODES (6 CLUSTER)
print("Menjalankan K-Modes Final dengan 6 Cluster...")
# init='Cao' sesuai request tim
km = KModes(n_clusters=6, init='Cao', n_init=5, verbose=1)
clusters = km.fit_predict(df_cluster)

# 4. MAPPING NAMA (LABELING)
# Ini hasil analisis kita dari terminal tadi
mapping_nama = {
    0: "Ekonomi & Bisnis",      # Manajemen, Akuntansi
    1: "Teknik & Rekayasa",     # Sipil, Mesin, Elektro
    2: "Pendidikan & Keguruan", # Guru, Pendidikan
    3: "Hukum",                 # Hukum
    4: "Teknologi Industri",    # SI, Industri
    5: "Sains & Teknologi"      # Farmasi, DKV, Sains
}

# Simpan ID Cluster
df['cluster_kmodes'] = clusters
# Simpan Nama Kategori
df['cluster_category'] = df['cluster_kmodes'].map(mapping_nama)

# 5. SIMPAN HASIL AKHIR
# Tentukan lokasi simpan (di folder csv_result)
if os.path.exists('csv_result'):
    output_path = 'csv_result/prodi_clustered_kmodes.csv'
else:
    output_path = '../csv_result/prodi_clustered_kmodes.csv'

df.to_csv(output_path, index=False)

print("\n" + "="*50)
print(f"SUKSES! Data Prodi berhasil dikelompokkan & diberi nama.")
print(f"File tersimpan di: {output_path}")
print("="*50)

# Tampilkan contoh hasil
print(df[['prodi', 'faculty', 'cluster_category']].head(10))