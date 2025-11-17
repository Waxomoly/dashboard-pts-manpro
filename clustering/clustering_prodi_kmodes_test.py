import pandas as pd
from kmodes.kmodes import KModes
import os

# 1. LOAD DATA
print("Loading data prodi...")

# Trik supaya aman: Kita cek dulu filenya ada di mana
# Opsi 1: Kalau dijalankan dari Root project
path_root = 'csv_result/merged_prodi_final.csv'
# Opsi 2: Kalau dijalankan dari dalam folder 'clustering' (mundur satu langkah)
path_inside = '../csv_result/merged_prodi_final.csv'

if os.path.exists(path_root):
    file_path = path_root
elif os.path.exists(path_inside):
    file_path = path_inside
else:
    raise FileNotFoundError("File merged_prodi_final.csv tidak ditemukan di path biasa.")

df = pd.read_csv(file_path)

# 2. PILIH FITUR
# Kolom penentu karakteristik prodi
features = ['faculty', 'prodi_normalized', 'edu_level']
df_cluster = df[features].copy()
df_cluster = df_cluster.fillna('Lainnya')

# 3. JALANKAN K-MODES
# Kita set 6 Cluster
n_clusters = 6
print(f"Sedang menjalankan K-Modes dengan {n_clusters} cluster (sabar ya, agak lama)...")

# init='Cao' sesuai request tim
km = KModes(n_clusters=n_clusters, init='Cao', n_init=5, verbose=1)
clusters = km.fit_predict(df_cluster)

# Simpan label sementara
df['cluster_temp'] = clusters

# 4. INTIP ISINYA (PROFILING)
print("\n" + "="*50)
print("HASIL KELOMPOK PRODI (Copy hasil ini ke chat)")
print("="*50)

for i in range(n_clusters):
    print(f"\n>>> CLUSTER {i} <<<")
    
    # Cek Fakultas dominan
    fakultas = df[df['cluster_temp'] == i]['faculty'].mode()[0]
    
    # Cek 7 Prodi terbanyak muncul
    prodi = df[df['cluster_temp'] == i]['prodi_normalized'].value_counts().head(7).index.tolist()
    
    print(f"Fakultas Dominan : {fakultas}")
    print(f"Contoh Prodi     : {', '.join(prodi)}")