import pandas as pd
import numpy as np
import os
from datetime import datetime
from itertools import product
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    BASE_PATH = os.path.join(parent_dir, "csv_result") + os.sep
    OUTPUT_PATH = os.path.join(parent_dir, "csv_marybaru") + os.sep
except NameError:
    BASE_PATH = "csv_result/"
    OUTPUT_PATH = "csv_marybaru/"

os.makedirs(OUTPUT_PATH, exist_ok=True)

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
    """Kategorikan lokasi kampus - SEMUA PROVINSI"""
    prov = str(provinsi).upper().strip()
    prov = prov.replace('.', '').replace('  ', ' ')
    
    # JABODETABEK
    if 'JAKARTA' in prov or 'DKI' in prov:
        return 'DKI Jakarta', 'Jabodetabek', 'Kota Besar', 1
    elif 'BEKASI' in prov:
        return 'Bekasi', 'Jabodetabek', 'Kota Besar', 2
    elif 'DEPOK' in prov:
        return 'Depok', 'Jabodetabek', 'Kota Besar', 3
    elif 'TANGERANG' in prov:
        return 'Tangerang', 'Jabodetabek', 'Kota Besar', 4
    elif 'BOGOR' in prov:
        return 'Bogor', 'Jabodetabek', 'Kota Besar', 5
    
    # JAWA
    elif 'JAWA BARAT' in prov or 'JABAR' in prov:
        return 'Jawa Barat', 'Jawa', 'Kota Besar', 10
    elif 'JAWA TIMUR' in prov or 'JATIM' in prov:
        return 'Jawa Timur', 'Jawa', 'Kota Besar', 11
    elif 'JAWA TENGAH' in prov or 'JATENG' in prov:
        return 'Jawa Tengah', 'Jawa', 'Kota Besar', 12
    elif 'YOGYA' in prov or 'YOGYAKARTA' in prov or 'DI YOGYA' in prov or 'DAERAH ISTIMEWA' in prov or 'DIY' in prov:
        return 'D.I. Yogyakarta', 'Jawa', 'Kota Besar', 13
    elif 'BANTEN' in prov:
        return 'Banten', 'Jawa', 'Kota Sedang', 14
    
    # SUMATERA
    elif 'SUMATERA UTARA' in prov or 'SUMATRA UTARA' in prov or 'SUMUT' in prov:
        return 'Sumatera Utara', 'Sumatera', 'Kota Sedang', 20
    elif 'SUMATERA BARAT' in prov or 'SUMATRA BARAT' in prov or 'SUMBAR' in prov:
        return 'Sumatera Barat', 'Sumatera', 'Kota Sedang', 21
    elif 'SUMATERA SELATAN' in prov or 'SUMATRA SELATAN' in prov or 'SUMSEL' in prov:
        return 'Sumatera Selatan', 'Sumatera', 'Kota Sedang', 22
    elif 'RIAU' in prov and 'KEPULAUAN' not in prov:
        return 'Riau', 'Sumatera', 'Daerah', 23
    elif 'KEPULAUAN RIAU' in prov or 'KEP RIAU' in prov or 'KEPRI' in prov:
        return 'Kepulauan Riau', 'Sumatera', 'Daerah', 24
    elif 'JAMBI' in prov:
        return 'Jambi', 'Sumatera', 'Daerah', 25
    elif 'ACEH' in prov or 'NAD' in prov:
        return 'Aceh', 'Sumatera', 'Daerah', 26
    elif 'LAMPUNG' in prov:
        return 'Lampung', 'Sumatera', 'Daerah', 27
    elif 'BENGKULU' in prov:
        return 'Bengkulu', 'Sumatera', 'Daerah', 28
    elif 'BANGKA' in prov or 'BELITUNG' in prov or 'BABEL' in prov:
        return 'Bangka Belitung', 'Sumatera', 'Daerah', 29
    
    # KALIMANTAN
    elif 'KALIMANTAN TIMUR' in prov or 'KALTIM' in prov:
        return 'Kalimantan Timur', 'Kalimantan', 'Daerah', 30
    elif 'KALIMANTAN SELATAN' in prov or 'KALSEL' in prov:
        return 'Kalimantan Selatan', 'Kalimantan', 'Daerah', 31
    elif 'KALIMANTAN TENGAH' in prov or 'KALTENG' in prov:
        return 'Kalimantan Tengah', 'Kalimantan', 'Daerah', 32
    elif 'KALIMANTAN BARAT' in prov or 'KALBAR' in prov:
        return 'Kalimantan Barat', 'Kalimantan', 'Daerah', 33
    elif 'KALIMANTAN UTARA' in prov or 'KALTARA' in prov:
        return 'Kalimantan Utara', 'Kalimantan', 'Daerah', 34
    elif 'KALIMANTAN' in prov:
        return 'Kalimantan', 'Kalimantan', 'Daerah', 35
    
    # SULAWESI
    elif 'SULAWESI SELATAN' in prov or 'SULSEL' in prov:
        return 'Sulawesi Selatan', 'Sulawesi', 'Kota Sedang', 40
    elif 'SULAWESI UTARA' in prov or 'SULUT' in prov:
        return 'Sulawesi Utara', 'Sulawesi', 'Daerah', 41
    elif 'SULAWESI TENGAH' in prov or 'SULTENG' in prov:
        return 'Sulawesi Tengah', 'Sulawesi', 'Daerah', 42
    elif 'SULAWESI TENGGARA' in prov or 'SULTRA' in prov:
        return 'Sulawesi Tenggara', 'Sulawesi', 'Daerah', 43
    elif 'SULAWESI BARAT' in prov or 'SULBAR' in prov:
        return 'Sulawesi Barat', 'Sulawesi', 'Daerah', 44
    elif 'GORONTALO' in prov:
        return 'Gorontalo', 'Sulawesi', 'Daerah', 45
    
    # BALI & NUSA TENGGARA
    elif 'BALI' in prov:
        return 'Bali', 'Bali-Nusa Tenggara', 'Kota Sedang', 50
    elif 'NUSA TENGGARA BARAT' in prov or 'NTB' in prov:
        return 'Nusa Tenggara Barat', 'Bali-Nusa Tenggara', 'Daerah', 51
    elif 'NUSA TENGGARA TIMUR' in prov or 'NTT' in prov:
        return 'Nusa Tenggara Timur', 'Bali-Nusa Tenggara', 'Daerah', 52
    
    # MALUKU
    elif 'MALUKU UTARA' in prov or 'MALUT' in prov:
        return 'Maluku Utara', 'Maluku-Papua', 'Daerah', 60
    elif 'MALUKU' in prov:
        return 'Maluku', 'Maluku-Papua', 'Daerah', 61
    
    # PAPUA
    elif 'PAPUA BARAT' in prov or 'PAPBAR' in prov:
        return 'Papua Barat', 'Maluku-Papua', 'Daerah', 70
    elif 'PAPUA TENGAH' in prov:
        return 'Papua Tengah', 'Maluku-Papua', 'Daerah', 71
    elif 'PAPUA PEGUNUNGAN' in prov:
        return 'Papua Pegunungan', 'Maluku-Papua', 'Daerah', 72
    elif 'PAPUA SELATAN' in prov:
        return 'Papua Selatan', 'Maluku-Papua', 'Daerah', 73
    elif 'PAPUA' in prov:
        return 'Papua', 'Maluku-Papua', 'Daerah', 74
    
    else:
        return prov, 'Lainnya', 'Daerah', 99

def buat_nama_cluster(lokasi_name, sub_cluster_data):
    """Buat nama cluster untuk sub-cluster"""
    avg_akred = sub_cluster_data['skor_akreditasi'].mean()
    avg_biaya = sub_cluster_data['biaya_tahunan'].mean()
    
    has_belum = (sub_cluster_data['skor_akreditasi'] == 0).any()
    pct_belum = (sub_cluster_data['skor_akreditasi'] == 0).sum() / len(sub_cluster_data) * 100
    
    if has_belum and avg_akred < 0.3:
        kualitas = 'Belum Terakreditasi'
    elif pct_belum >= 40:
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
    
    if avg_biaya < 5000000:
        harga = 'Murah'
    elif avg_biaya < 10000000:
        harga = 'Terjangkau'
    elif avg_biaya < 20000000:
        harga = 'Menengah'
    else:
        harga = 'Mahal'
    
    return f"{lokasi_name} | {kualitas} - {harga}"

# ============================================
# LOAD DATA
# ============================================

try:
    df_institutions = pd.read_csv(BASE_PATH + "merged_institutions.csv")
    df_prodi = pd.read_csv(BASE_PATH + "merged_prodi.csv")
    print(f"✓ Loaded {len(df_institutions):,} kampus")
    print(f"✓ Loaded {len(df_prodi):,} program studi")
except FileNotFoundError as e:
    print(f"❌ Error: File tidak ditemukan - {e.filename}")
    exit(1)

# Create mapping dict
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

# ============================================
# CLEAN & PROCESS DATA
# ============================================
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

# Kategorikan
akred_info = df_merged['akreditasi'].apply(kategori_akreditasi)
df_merged['kategori_akreditasi'] = akred_info.apply(lambda x: x[0])
df_merged['skor_akreditasi'] = akred_info.apply(lambda x: x[1])

harga_info = df_merged['biaya_tahunan'].apply(kategori_harga)
df_merged['kategori_harga'] = harga_info.apply(lambda x: x[0])
df_merged['tier_harga'] = harga_info.apply(lambda x: x[1])

lokasi_info = df_merged['provinsi'].apply(kategori_lokasi)
df_merged['provinsi_detail'] = lokasi_info.apply(lambda x: x[0])
df_merged['wilayah'] = lokasi_info.apply(lambda x: x[1])
df_merged['tipe_lokasi'] = lokasi_info.apply(lambda x: x[2])
df_merged['kode_lokasi'] = lokasi_info.apply(lambda x: x[3])

# ============================================
# LEVEL 1: GUNAKAN PROVINSI SEBAGAI CLUSTER
# ============================================

provinsi_to_cluster = {prov: idx for idx, prov in enumerate(sorted(df_merged['provinsi_detail'].unique()))}
df_merged['cluster_lokasi'] = df_merged['provinsi_detail'].map(provinsi_to_cluster)

n_lokasi_clusters = df_merged['cluster_lokasi'].nunique()

# Tampilkan semua provinsi
for cluster_id in sorted(df_merged['cluster_lokasi'].unique()):
    cluster_data = df_merged[df_merged['cluster_lokasi'] == cluster_id]
    lokasi_name = cluster_data['provinsi_detail'].iloc[0]
    print(f"   {cluster_id:2d}. {lokasi_name:30s} ({len(cluster_data):,} prodi)")

# ============================================
# LEVEL 2: SUB-CLUSTERING AKREDITASI & BIAYA
# ============================================

df_merged['cluster_final'] = -1
cluster_counter = 0
cluster_profiles = {}

for lokasi_id in sorted(df_merged['cluster_lokasi'].unique()):
    lokasi_data = df_merged[df_merged['cluster_lokasi'] == lokasi_id].copy()
    lokasi_name = lokasi_data['provinsi_detail'].iloc[0]
    
    if len(lokasi_data) < 10:
        df_merged.loc[lokasi_data.index, 'cluster_final'] = cluster_counter
        cluster_profiles[cluster_counter] = {
            'cluster_lokasi': lokasi_id,
            'lokasi_name': lokasi_name,
            'sub_cluster': 0,
            'nama': buat_nama_cluster(lokasi_name, lokasi_data),
            'jumlah': len(lokasi_data),
            'rata_biaya': int(lokasi_data['biaya_tahunan'].mean()),
            'min_biaya': int(lokasi_data['biaya_tahunan'].min()),
            'max_biaya': int(lokasi_data['biaya_tahunan'].max()),
            'rata_akreditasi': lokasi_data['skor_akreditasi'].mean()
        }
        cluster_counter += 1
        continue
    
    # Sub-clustering
    features_sub = lokasi_data[['skor_akreditasi', 'biaya_tahunan']].copy()
    scaler_sub = StandardScaler()
    features_sub_scaled = scaler_sub.fit_transform(features_sub)
    
    dbscan_sub = DBSCAN(eps=0.5, min_samples=5)
    sub_clusters = dbscan_sub.fit_predict(features_sub_scaled)
    
    # Handle noise
    if (sub_clusters == -1).sum() > 0:
        for idx_pos, idx in enumerate(lokasi_data.index):
            if sub_clusters[idx_pos] == -1:
                valid_mask = sub_clusters != -1
                if valid_mask.sum() > 0:
                    distances = np.abs(features_sub_scaled[valid_mask] - features_sub_scaled[idx_pos]).sum(axis=1)
                    nearest = np.argmin(distances)
                    sub_clusters[idx_pos] = sub_clusters[valid_mask][nearest]
                else:
                    sub_clusters[idx_pos] = 0
    
    unique_sub = np.unique(sub_clusters)
    
    for sub_id in unique_sub:
        sub_mask = sub_clusters == sub_id
        sub_data = lokasi_data[sub_mask]
        
        df_merged.loc[sub_data.index, 'cluster_final'] = cluster_counter
        
        cluster_profiles[cluster_counter] = {
            'cluster_lokasi': lokasi_id,
            'lokasi_name': lokasi_name,
            'sub_cluster': int(sub_id),
            'nama': buat_nama_cluster(lokasi_name, sub_data),
            'jumlah': len(sub_data),
            'rata_biaya': int(sub_data['biaya_tahunan'].mean()),
            'min_biaya': int(sub_data['biaya_tahunan'].min()),
            'max_biaya': int(sub_data['biaya_tahunan'].max()),
            'rata_akreditasi': sub_data['skor_akreditasi'].mean()
        }
        cluster_counter += 1

n_final_clusters = df_merged['cluster_final'].nunique()

df_merged['nama_cluster'] = df_merged['cluster_final'].map(lambda x: cluster_profiles[x]['nama'])

# ============================================
# POST-PROCESSING: Merge Cluster Duplikat
# ============================================

# Cari cluster dengan nama yang sama
nama_to_clusters = {}
for cluster_id, profile in cluster_profiles.items():
    nama = profile['nama']
    if nama not in nama_to_clusters:
        nama_to_clusters[nama] = []
    nama_to_clusters[nama].append(cluster_id)

# Merge cluster duplikat
duplicates_found = 0
cluster_mapping = {}  # old_id -> new_id

for nama, cluster_ids in nama_to_clusters.items():
    if len(cluster_ids) > 1:
        # Ada duplikat, merge ke cluster pertama
        master_id = min(cluster_ids)
        duplicates_found += len(cluster_ids) - 1
        
        print(f"   !  Duplikat: '{nama}'")
        print(f"      • Master cluster: {master_id}")
        print(f"      • Merging: {cluster_ids[1:]}")
        
        for dup_id in cluster_ids[1:]:
            cluster_mapping[dup_id] = master_id

if duplicates_found > 0:
    # Apply mapping
    df_merged['cluster_final'] = df_merged['cluster_final'].replace(cluster_mapping)
    
    # Re-number clusters agar berurutan (0, 1, 2, ...)
    unique_clusters = sorted(df_merged['cluster_final'].unique())
    renumber_map = {old: new for new, old in enumerate(unique_clusters)}
    df_merged['cluster_final'] = df_merged['cluster_final'].map(renumber_map)
    
    # Rebuild cluster profiles
    new_cluster_profiles = {}
    for new_id, old_id in enumerate(unique_clusters):
        cluster_data = df_merged[df_merged['cluster_final'] == new_id]
        lokasi_name = cluster_data['provinsi_detail'].iloc[0]
        
        new_cluster_profiles[new_id] = {
            'cluster_lokasi': cluster_data['cluster_lokasi'].mode()[0],
            'lokasi_name': lokasi_name,
            'nama': buat_nama_cluster(lokasi_name, cluster_data),
            'jumlah': len(cluster_data),
            'rata_biaya': int(cluster_data['biaya_tahunan'].mean()),
            'min_biaya': int(cluster_data['biaya_tahunan'].min()),
            'max_biaya': int(cluster_data['biaya_tahunan'].max()),
            'rata_akreditasi': cluster_data['skor_akreditasi'].mean()
        }
    
    cluster_profiles = new_cluster_profiles
    df_merged['nama_cluster'] = df_merged['cluster_final'].map(lambda x: cluster_profiles[x]['nama'])
    
    n_final_clusters = df_merged['cluster_final'].nunique()
    print(f"\n   Merged {duplicates_found} duplicate clusters")
    print(f"   Final unique clusters: {n_final_clusters}")
else:
    print(f"   No duplicates found")

# ============================================
# EXPORT HASIL
# ============================================

output_main = df_merged[[
    'cluster_lokasi', 'cluster_final', 'nama_cluster',
    'nama_kampus', 'kode_kampus',
    'nama_prodi', 'prodi_normalized', 'fakultas',
    'akreditasi', 'kategori_akreditasi', 'skor_akreditasi',
    'biaya_tahunan', 'kategori_harga',
    'provinsi_detail', 'wilayah', 'tipe_lokasi'
]].copy()

output_main = output_main.sort_values(['cluster_lokasi', 'cluster_final', 'biaya_tahunan'])
output_main.to_csv(OUTPUT_PATH + 'hasil_clustering.csv', index=False, encoding='utf-8-sig')

summary_data = []
for cid, info in cluster_profiles.items():
    summary_data.append({
        'cluster_final': cid,
        'cluster_lokasi': info['cluster_lokasi'],
        'lokasi': info['lokasi_name'],
        'nama_cluster': info['nama'],
        'jumlah_prodi': info['jumlah'],
        'biaya_termurah': info['min_biaya'],
        'biaya_termahal': info['max_biaya'],
        'rata_rata_biaya': info['rata_biaya'],
        'rata_akreditasi': round(info['rata_akreditasi'], 2)
    })

df_summary = pd.DataFrame(summary_data)
df_summary = df_summary.sort_values(['cluster_lokasi', 'rata_rata_biaya'])
df_summary.to_csv(OUTPUT_PATH + 'ringkasan_cluster.csv', index=False, encoding='utf-8-sig')

# Define all possible values for combinations
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
    'total_cluster': n_final_clusters,
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
    status = "V" if akred != 'Belum Terakreditasi' else "X"
    print(f"   {status} {akred}: {count} prodi ({pct:.1f}%)")