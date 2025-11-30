import pandas as pd
import numpy as np
from kmodes.kmodes import KModes
from kmodes.kprototypes import KPrototypes
from sklearn.preprocessing import MinMaxScaler

# --- 1. LOAD & PREPARATION ---
try:
    df_institusi = pd.read_csv('merged_institutions.csv')
    df_prodi = pd.read_csv('merged_prodi_final.csv')
except FileNotFoundError:
    print("Error: File tidak ditemukan.")
    exit()

# Gabungkan data
df_full = pd.merge(df_prodi, df_institusi, on='institution_code', how='left')

# ambil kolom yang relevan untuk clustering ini
# Level 1: province
# Level 2: average_yearly_fee, campus_accreditation
# Info tambahan: institution_name, prodi

cols_req = ['institution_name', 'prodi', 'province', 'average_yearly_fee', 'campus_accreditation']
df_cluster = df_full[cols_req].copy()

# --- 2. CLEANING ---
# Bersihkan data NaN atau nilai -1.0
df_cluster['average_yearly_fee'] = df_cluster['average_yearly_fee'].replace(-1.0, np.nan)
df_cluster = df_cluster.dropna(subset=['province', 'average_yearly_fee', 'campus_accreditation'])
df_cluster = df_cluster[df_cluster['province'] != '-']
df_cluster = df_cluster[df_cluster['campus_accreditation'] != '-']

# Reset index agar rapi
df_cluster.reset_index(drop=True, inplace=True)

print(f"Data siap: {len(df_cluster)} baris")


#  1. CLUSTERING BERDASARKAN LOKASI
print("\n Clustering Lokasi (Provinsi)")

k_lokasi = 10

data_lokasi = df_cluster[['province']]

km_lokasi = KModes(n_clusters=k_lokasi, init='Huang', n_init=5, verbose=0, random_state=42)
cluster_lokasi_labels = km_lokasi.fit_predict(data_lokasi)

# Simpan hasil tahap 1
df_cluster['cluster_lokasi'] = cluster_lokasi_labels

# Cek hasil Level 1
print("Hasil Cluster Lokasi:")
print(df_cluster.groupby('cluster_lokasi')['province'].unique())



#  2. Clustering ke 2, biaya per tahun dan akred
df_cluster['cluster_final'] = -1
df_cluster['segment_name'] = ""


for i in range(k_lokasi):
    subset_data = df_cluster[df_cluster['cluster_lokasi'] == i].copy()
    
    if len(subset_data) < 5:
        print(f"Cluster Lokasi {i} data terlalu sedikit ({len(subset_data)}), skip clustering tahap 2.")
        df_cluster.loc[subset_data.index, 'cluster_final'] = 0
        df_cluster.loc[subset_data.index, 'segment_name'] = f"Loc_{i}-Group_0"
        continue

    print(f"\nProcessing Lokasi Group {i} (Jumlah data: {len(subset_data)})...")

    scaler = MinMaxScaler()
    fitur_biaya= scaler.fit_transform(subset_data[['average_yearly_fee']])
    fitur_akred= subset_data[['campus_accreditation']].values

    # Gabung jadi matriks
    matrix_subset = np.hstack((fitur_biaya, fitur_akred))

    # Posisi kolom kategorikal (akreditasi ada di index 1, karena index 0 adalah biaya)
    categorical_pos = [1] 

    # 3. Jalankan K-Prototypes
    # Misal bagi jadi 3 kategori (Murah, Sedang, Mahal / Bagus, Biasa)
    k_sub = 3 
    
    try:
        kproto_sub = KPrototypes(n_clusters=k_sub, init='Huang', n_init=5, verbose=0, random_state=42)
        sub_labels = kproto_sub.fit_predict(matrix_subset, categorical=categorical_pos)
        
        # 4. Simpan kembali ke DataFrame utama
        df_cluster.loc[subset_data.index, 'cluster_final'] = sub_labels
        
        # Buat nama segmen biar mudah dibaca
        df_cluster.loc[subset_data.index, 'segment_name'] =  \
            df_cluster.loc[subset_data.index].apply(lambda x: f"Loc_{x['cluster_lokasi']}-Type_{x['cluster_final']}", axis=1)

        # Analisis per Sub-Cluster
        print(f"  > Hasil Group {i}:")
        print(subset_data.assign(sub_cluster=sub_labels).groupby('sub_cluster')[['average_yearly_fee']].mean())
        
    except Exception as e:
        print(f"  > Gagal clustering di group {i} (mungkin data terlalu homogen): {e}")


# Hasil
print("\n HASIL")
result_view = df_cluster[['institution_name', 'province', 'average_yearly_fee', 'campus_accreditation', 'segment_name']]
print(result_view.head(10))

# Simpan
result_view.to_csv('hasil_clustering_terbaru.csv', index=False)

# Grouping berdasarkan nama segmen 
analisis_harga = df_cluster.groupby('segment_name')['average_yearly_fee'].agg(
    Min_Biaya='min',
    Max_Biaya='max',
    Rata_Rata='mean',
    Jumlah_Kampus='count'
).sort_values(by='Rata_Rata', ascending=False)

# Format angka supaya enak dibaca (Ribuan separator)
pd.options.display.float_format = '{:,.0f}'.format

print("\n--- DETAIL RENTANG BIAYA PER CLUSTER ---")
print(analisis_harga)

# Simpan ke CSV 
analisis_harga.to_csv('detail_range_harga_cluster.csv')
print("\nFile detail harga disimpan: detail_range_harga_cluster.csv")
