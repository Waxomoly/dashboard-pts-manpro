import pandas as pd
import numpy as np
import os
from datetime import datetime
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    BASE_PATH = os.path.join(parent_dir, "csv_result") + os.sep
    OUTPUT_PATH = os.path.join(parent_dir, "csv_mary") + os.sep
except NameError:
    BASE_PATH = "csv_result/"
    OUTPUT_PATH = "csv_mary/"

def kategori_akreditasi(akred):
    """Kategorikan akreditasi dengan detail lengkap"""
    akred = str(akred).upper().strip()
    
    if 'UNGGUL' in akred:
        return 'Unggul', 5
    elif 'A' in akred and 'BAIK' not in akred:
        return 'A', 4
    elif 'BAIK SEKALI' in akred or 'B' in akred:
        return 'Baik Sekali', 3
    elif 'BAIK' in akred and 'SEKALI' not in akred:
        return 'Baik', 2
    elif 'C' in akred:
        return 'C', 1
    elif 'TIDAK' in akred or 'BELUM' in akred:
        return 'Belum Terakreditasi', 0
    else:
        return 'Belum Terakreditasi', 0

def kategori_harga(biaya):
    """Kategorikan harga kuliah per tahun"""
    if biaya < 5000000:
        return 'Murah (< 5 juta)', 1
    elif biaya < 10000000:
        return 'Terjangkau (5-10 juta)', 2
    elif biaya < 20000000:
        return 'Menengah (10-20 juta)', 3
    else:
        return 'Mahal (> 20 juta)', 4

def kategori_lokasi(provinsi):
    """Kategorikan lokasi kampus"""
    prov = str(provinsi).upper()
    
    if 'JAKARTA' in prov or 'DKI' in prov:
        return 'DKI Jakarta', 'Jabodetabek', 'Kota Besar'
    elif any(x in prov for x in ['BEKASI', 'DEPOK', 'TANGERANG', 'BOGOR']):
        return provinsi, 'Jabodetabek', 'Kota Besar'
    elif 'JAWA BARAT' in prov or 'BANDUNG' in prov:
        return 'Jawa Barat', 'Jawa', 'Kota Besar'
    elif 'JAWA TIMUR' in prov or 'SURABAYA' in prov:
        return 'Jawa Timur', 'Jawa', 'Kota Besar'
    elif 'JAWA TENGAH' in prov or 'SEMARANG' in prov:
        return 'Jawa Tengah', 'Jawa', 'Kota Besar'
    elif 'YOGYA' in prov:
        return 'D.I. Yogyakarta', 'Jawa', 'Kota Besar'
    elif 'BANTEN' in prov:
        return 'Banten', 'Jawa', 'Kota Besar'
    elif any(x in prov for x in ['SUMATERA', 'SUMATRA', 'MEDAN', 'PADANG', 'PALEMBANG']):
        return provinsi, 'Sumatera', 'Kota Sedang'
    elif 'BALI' in prov:
        return 'Bali', 'Bali', 'Kota Sedang'
    else:
        return provinsi, 'Luar Jawa', 'Daerah'

def buat_nama_cluster(data):
    """Buat nama cluster - PAKSA deteksi Belum Terakreditasi"""
    avg_akred = data['skor_akreditasi'].mean()
    avg_biaya = data['biaya_tahunan'].mean()
    pct_kota_besar = (data['tipe_lokasi'] == 'Kota Besar').sum() / len(data) * 100
    
    has_belum = (data['skor_akreditasi'] == 0).any()
    pct_belum = (data['skor_akreditasi'] == 0).sum() / len(data) * 100
    
    if has_belum and avg_akred < 0.3:
        kualitas = 'Belum Terakreditasi'
    elif pct_belum >= 40:  # Turunkan threshold dari 60% ke 40%
        kualitas = 'Belum Terakreditasi'
    elif avg_akred >= 4.5:
        kualitas = 'Unggul'
    elif avg_akred >= 3.5:
        kualitas = 'Akreditasi A'
    elif avg_akred >= 2.5:
        kualitas = 'Baik Sekali'
    elif avg_akred >= 1.5:
        kualitas = 'Baik'
    elif avg_akred >= 0.5:
        kualitas = 'Akreditasi C'
    else:
        kualitas = 'Belum Terakreditasi'
    
    # Harga
    if avg_biaya < 5000000:
        harga = 'Murah'
    elif avg_biaya < 10000000:
        harga = 'Terjangkau'
    elif avg_biaya < 20000000:
        harga = 'Menengah'
    else:
        harga = 'Mahal'
    
    # Lokasi
    if pct_kota_besar > 70:
        lokasi = 'Kota Besar'
    else:
        lokasi = 'Daerah'
    
    return f"{kualitas} - {harga} - {lokasi}"

# MAIN 
try:
    df_institutions = pd.read_csv(BASE_PATH + "merged_institutions.csv")
    df_prodi = pd.read_csv(BASE_PATH + "merged_prodi.csv")
    print(f"Loaded {len(df_institutions):,} kampus")
    print(f"Loaded {len(df_prodi):,} program studi")
except FileNotFoundError as e:
    print(f"Error: File tidak ditemukan - {e.filename}")
    exit(1)

# Create mapping dict untuk institutions berdasarkan codes
inst_map = {}
for _, row in df_institutions.iterrows():
    for code_col in ['quipper_code', 'rencanamu_code', 'banpt_code', 'pddikti_code']:
        if code_col in row and row[code_col] != '-':
            inst_map[row[code_col]] = row

# Merge prodi dengan institutions
merged_data = []
matched = 0
no_match = 0

for _, prodi_row in df_prodi.iterrows():
    inst_row = None
    
    for code_col in ['quipper_code', 'rencanamu_code', 'banpt_code', 'pddikti_code']:
        if prodi_row[code_col] != '-' and prodi_row[code_col] in inst_map:
            inst_row = inst_map[prodi_row[code_col]]
            matched += 1
            break
    
    if inst_row is None:
        no_match += 1
        continue
    
    prodi_akred = str(prodi_row['accreditation']).strip()
    kampus_akred = str(inst_row['campus_accreditation']).strip()
    
    if prodi_akred not in ['-', '', 'nan', 'None']:
        final_akred = prodi_akred
    elif kampus_akred not in ['-', '', 'nan', 'None']:
        final_akred = kampus_akred
    else:
        final_akred = 'Belum Terakreditasi'
    
    merged_data.append({
        'kode_kampus': inst_row['institution_code'],
        'nama_kampus': inst_row['institution_name'],
        'provinsi': inst_row['province'],
        'nama_prodi': prodi_row['prodi'],
        'prodi_normalized': prodi_row['prodi_normalized'],
        'fakultas': prodi_row['faculty'],
        'akreditasi': final_akred,
        'biaya_tahunan': inst_row['average_yearly_fee']
    })

df_merged = pd.DataFrame(merged_data)
print(f"✓ Matched: {matched:,} prodi")
print(f"✓ No match: {no_match:,} prodi (skipped)")
print(f"✓ Final dataset: {len(df_merged):,} kombinasi kampus-prodi")

# 3. CLEAN & PROCESS DATA
initial_len = len(df_merged)
df_merged = df_merged.dropna(subset=['biaya_tahunan'])
df_merged = df_merged[df_merged['biaya_tahunan'] > 100000]
df_merged = df_merged[df_merged['biaya_tahunan'] < 999999999]

df_merged['akreditasi'] = df_merged['akreditasi'].fillna('Belum Terakreditasi')
df_merged['akreditasi'] = df_merged['akreditasi'].replace({
    'nan': 'Belum Terakreditasi',
    'None': 'Belum Terakreditasi',
    '-': 'Belum Terakreditasi',
    '': 'Belum Terakreditasi',
    'TERAKREDITASI': 'C',
    'TIDAK TERAKREDITASI': 'Belum Terakreditasi'
})

akred_info = df_merged['akreditasi'].apply(kategori_akreditasi)
df_merged['kategori_akreditasi'] = akred_info.apply(lambda x: x[0])
df_merged['skor_akreditasi'] = akred_info.apply(lambda x: x[1])

terakreditasi = len(df_merged[df_merged['skor_akreditasi'] > 0])
belum = len(df_merged[df_merged['skor_akreditasi'] == 0])

harga_info = df_merged['biaya_tahunan'].apply(kategori_harga)
df_merged['kategori_harga'] = harga_info.apply(lambda x: x[0])
df_merged['tier_harga'] = harga_info.apply(lambda x: x[1])

lokasi_info = df_merged['provinsi'].apply(kategori_lokasi)
df_merged['provinsi_detail'] = lokasi_info.apply(lambda x: x[0])
df_merged['wilayah'] = lokasi_info.apply(lambda x: x[1])
df_merged['tipe_lokasi'] = lokasi_info.apply(lambda x: x[2])

df_merged['bobot_lokasi'] = df_merged['tipe_lokasi'].map({
    'Kota Besar': 1.0,
    'Kota Sedang': 0.5,
    'Daerah': 0.0
})

# CLUSTERING
df_belum = df_merged[df_merged['skor_akreditasi'] == 0].copy()
df_terakreditasi = df_merged[df_merged['skor_akreditasi'] > 0].copy()

# Clustering untuk TERAKREDITASI
if len(df_terakreditasi) > 0:
    features = df_terakreditasi[['biaya_tahunan', 'skor_akreditasi', 'bobot_lokasi']].copy()
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    dbscan = DBSCAN(eps=0.5, min_samples=30)
    df_terakreditasi['cluster'] = dbscan.fit_predict(features_scaled)
    
    noise_count = (df_terakreditasi['cluster'] == -1).sum()
    if noise_count > 0:
        for idx in df_terakreditasi[df_terakreditasi['cluster'] == -1].index:
            biaya = df_terakreditasi.loc[idx, 'biaya_tahunan']
            similar = df_terakreditasi[(df_terakreditasi['cluster'] != -1) & 
                               (abs(df_terakreditasi['biaya_tahunan'] - biaya) < 5000000)]
            if len(similar) > 0:
                df_terakreditasi.loc[idx, 'cluster'] = similar['cluster'].mode()[0]
            else:
                df_terakreditasi.loc[idx, 'cluster'] = 0
    
    max_cluster = df_terakreditasi['cluster'].max()
    print(f"   ✓ Terakreditasi: {max_cluster + 1} clusters (ID 0-{max_cluster})")
else:
    max_cluster = -1

if len(df_belum) > 0:
    # STRATEGY: Cluster berdasarkan harga + lokasi
    features_belum = df_belum[['biaya_tahunan', 'bobot_lokasi']].copy()
    
    if len(df_belum) >= 20:  # Turunkan threshold
        scaler_belum = StandardScaler()
        features_belum_scaled = scaler_belum.fit_transform(features_belum)
        
        # LOOSER clustering untuk belum terakreditasi
        dbscan_belum = DBSCAN(eps=0.7, min_samples=5)  # eps lebih besar, min_samples kecil
        sub_clusters = dbscan_belum.fit_predict(features_belum_scaled)
        
        # Offset cluster ID
        df_belum['cluster'] = max_cluster + 1 + sub_clusters
        
        # Fix noise: assign ke cluster terdekat atau buat cluster baru
        noise_belum = (df_belum['cluster'] < max_cluster + 1)
        if noise_belum.sum() > 0:
            df_belum.loc[noise_belum, 'cluster'] = max_cluster + 1
        
        n_belum_clusters = df_belum['cluster'].nunique()
    else:
        # Terlalu sedikit: buat 1 cluster khusus
        df_belum['cluster'] = max_cluster + 1
    
    # VERIFY: Pastikan SEMUA belum terakreditasi punya cluster >= max_cluster+1
    min_cluster_belum = df_belum['cluster'].min()
    max_cluster_belum = df_belum['cluster'].max()
    
    if min_cluster_belum <= max_cluster:
        df_belum['cluster'] = df_belum['cluster'] + (max_cluster + 1 - min_cluster_belum)

# Gabung kembali
df_merged = pd.concat([df_terakreditasi, df_belum], ignore_index=True)

n_clusters = df_merged['cluster'].nunique()

# VERIFY FINAL
belum_in_clusters = df_merged[df_merged['skor_akreditasi'] == 0]['cluster'].unique()

cluster_profiles = {}
for cluster_id in sorted(df_merged['cluster'].unique()):
    cluster_data = df_merged[df_merged['cluster'] == cluster_id]
    nama = buat_nama_cluster(cluster_data)
    
    # COUNT belum terakreditasi di cluster ini
    n_belum_in_cluster = (cluster_data['skor_akreditasi'] == 0).sum()
    
    cluster_profiles[cluster_id] = {
        'nama': nama,
        'jumlah': len(cluster_data),
        'jumlah_belum_terakreditasi': n_belum_in_cluster,
        'rata_biaya': int(cluster_data['biaya_tahunan'].mean()),
        'min_biaya': int(cluster_data['biaya_tahunan'].min()),
        'max_biaya': int(cluster_data['biaya_tahunan'].max())
    }
    
df_merged['nama_cluster'] = df_merged['cluster'].map(lambda x: cluster_profiles[x]['nama'])

output_main = df_merged[[
    'cluster', 'nama_cluster',
    'nama_kampus', 'kode_kampus',
    'nama_prodi', 'prodi_normalized', 'fakultas',
    'akreditasi', 'kategori_akreditasi', 'skor_akreditasi',
    'biaya_tahunan', 'kategori_harga',
    'provinsi_detail', 'wilayah', 'tipe_lokasi'
]].copy()

output_main = output_main.sort_values(['cluster', 'biaya_tahunan'])
output_main.to_csv(OUTPUT_PATH + 'hasil_clustering.csv', index=False, encoding='utf-8-sig')
print(f"✓ File 1: hasil_clustering.csv ({len(output_main):,} baris)")

summary_data = []
for cid, info in cluster_profiles.items():
    summary_data.append({
        'cluster_id': cid,
        'nama_cluster': info['nama'],
        'jumlah_prodi': info['jumlah'],
        'jumlah_belum_terakreditasi': info['jumlah_belum_terakreditasi'],
        'biaya_termurah': info['min_biaya'],
        'biaya_termahal': info['max_biaya'],
        'rata_rata_biaya': info['rata_biaya']
    })

df_summary = pd.DataFrame(summary_data)
df_summary.to_csv(OUTPUT_PATH + 'ringkasan_cluster.csv', index=False, encoding='utf-8-sig')

from itertools import product
all_akreditasi = ['Unggul', 'A', 'Baik Sekali', 'Baik', 'C', 'Belum Terakreditasi']
all_wilayah = df_merged['wilayah'].unique().tolist()

filter_akred = df_merged.groupby(['kategori_akreditasi', 'wilayah']).agg({
    'nama_prodi': 'count',
    'nama_kampus': 'nunique',
    'biaya_tahunan': 'mean'
}).reset_index()
filter_akred.columns = ['akreditasi', 'wilayah', 'jumlah_prodi', 'jumlah_kampus', 'rata_biaya']

all_combinations = pd.DataFrame(list(product(all_akreditasi, all_wilayah)), 
                                columns=['akreditasi', 'wilayah'])
filter_akred = all_combinations.merge(filter_akred, on=['akreditasi', 'wilayah'], how='left')
filter_akred = filter_akred.fillna({'jumlah_prodi': 0, 'jumlah_kampus': 0, 'rata_biaya': 0})
filter_akred['jumlah_prodi'] = filter_akred['jumlah_prodi'].astype(int)
filter_akred['jumlah_kampus'] = filter_akred['jumlah_kampus'].astype(int)
filter_akred = filter_akred.sort_values(['akreditasi', 'jumlah_prodi'], ascending=[True, False])
filter_akred.to_csv(OUTPUT_PATH + 'filter_akreditasi.csv', index=False, encoding='utf-8-sig')

filter_harga = df_merged.groupby(['kategori_harga', 'tier_harga']).agg({
    'nama_prodi': 'count'
}).reset_index()
filter_harga.columns = ['kategori_harga', 'tier', 'jumlah_prodi']
filter_harga = filter_harga.sort_values('tier')
filter_harga.to_csv(OUTPUT_PATH + 'filter_harga.csv', index=False, encoding='utf-8-sig')

filter_lokasi = df_merged.groupby(['wilayah', 'tipe_lokasi']).agg({
    'nama_prodi': 'count',
    'biaya_tahunan': 'mean'
}).reset_index()
filter_lokasi.columns = ['wilayah', 'tipe_lokasi', 'jumlah_prodi', 'rata_biaya']
filter_lokasi.to_csv(OUTPUT_PATH + 'filter_lokasi.csv', index=False, encoding='utf-8-sig')

filter_prodi = df_merged.groupby('prodi_normalized').agg({
    'nama_prodi': 'count',
    'nama_kampus': 'nunique',
    'biaya_tahunan': ['mean', 'min', 'max'],
    'skor_akreditasi': 'mean'
}).reset_index()
filter_prodi.columns = ['prodi', 'jumlah_kampus', 'total_kampus', 'rata_biaya', 'min_biaya', 'max_biaya', 'rata_akreditasi']
filter_prodi = filter_prodi[filter_prodi['jumlah_kampus'] >= 5]
filter_prodi = filter_prodi.sort_values('jumlah_kampus', ascending=False)
filter_prodi.to_csv(OUTPUT_PATH + 'filter_prodi.csv', index=False, encoding='utf-8-sig')

kampus_wilayah = df_merged.groupby(['wilayah', 'nama_kampus', 'kategori_akreditasi']).agg({
    'nama_prodi': 'count',
    'biaya_tahunan': 'first',
    'provinsi_detail': 'first'
}).reset_index()
kampus_wilayah.columns = ['wilayah', 'nama_kampus', 'akreditasi', 'jumlah_prodi', 'biaya_tahunan', 'provinsi']
kampus_wilayah = kampus_wilayah.sort_values(['wilayah', 'jumlah_prodi'], ascending=[True, False])
kampus_wilayah.to_csv(OUTPUT_PATH + 'kampus_per_wilayah.csv', index=False, encoding='utf-8-sig')

summary_dashboard = pd.DataFrame([{
    'total_kampus': df_merged['nama_kampus'].nunique(),
    'total_prodi': len(df_merged),
    'total_cluster': n_clusters,
    'biaya_termurah': int(df_merged['biaya_tahunan'].min()),
    'biaya_termahal': int(df_merged['biaya_tahunan'].max()),
    'rata_biaya': int(df_merged['biaya_tahunan'].mean()),
    'prodi_unggul': len(df_merged[df_merged['kategori_akreditasi'] == 'Unggul']),
    'prodi_a': len(df_merged[df_merged['kategori_akreditasi'] == 'A']),
    'prodi_baik_sekali': len(df_merged[df_merged['kategori_akreditasi'] == 'Baik Sekali']),
    'prodi_baik': len(df_merged[df_merged['kategori_akreditasi'] == 'Baik']),
    'prodi_c': len(df_merged[df_merged['kategori_akreditasi'] == 'C']),
    'prodi_belum_terakreditasi': len(df_merged[df_merged['kategori_akreditasi'] == 'Belum Terakreditasi']),
}])
summary_dashboard.to_csv(OUTPUT_PATH + 'summary_dashboard.csv', index=False, encoding='utf-8-sig')

akred_order = ['Unggul', 'A', 'Baik Sekali', 'Baik', 'C', 'Belum Terakreditasi']
for akred in akred_order:
    count = len(df_merged[df_merged['kategori_akreditasi'] == akred])
    pct = count/len(df_merged)*100 if len(df_merged) > 0 else 0
    status = "✅" if akred != 'Belum Terakreditasi' else "🔴"
    print(f"   {status} {akred}: {count} prodi ({pct:.1f}%)")