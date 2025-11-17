import pandas as pd
import numpy as np
import os

# Setup path
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    BASE_PATH = os.path.join(parent_dir, "csv_result") + os.sep
except NameError:
    BASE_PATH = "csv_result/"


def normalize_features(features):
    mean = np.mean(features, axis=0)
    std = np.std(features, axis=0)
    std[std == 0] = 1
    return (features - mean) / std


def euclidean_distance(p1, p2):
    return np.sqrt(np.sum((p1 - p2) ** 2))


def region_query(data, point_idx, eps):
    neighbors = []
    point = data[point_idx]
    for i in range(len(data)):
        if euclidean_distance(point, data[i]) <= eps:
            neighbors.append(i)
    return neighbors


def expand_cluster(data, labels, point_idx, neighbors, cluster_id, eps, min_pts):
    labels[point_idx] = cluster_id
    i = 0
    while i < len(neighbors):
        neighbor_idx = neighbors[i]
        if labels[neighbor_idx] == -1:
            labels[neighbor_idx] = cluster_id
        if labels[neighbor_idx] == 0:
            labels[neighbor_idx] = cluster_id
            neighbor_neighbors = region_query(data, neighbor_idx, eps)
            if len(neighbor_neighbors) >= min_pts:
                neighbors.extend(neighbor_neighbors)
        i += 1


def dbscan(data, eps, min_pts):
    n = len(data)
    labels = np.zeros(n, dtype=int)
    cluster_id = 0
    
    print(f"Processing {n} points...")
    for point_idx in range(n):
        if point_idx % 50 == 0:
            print(f"Progress: {point_idx}/{n} ({point_idx/n*100:.1f}%)", end='\r')
        
        if labels[point_idx] != 0:
            continue
        
        neighbors = region_query(data, point_idx, eps)
        
        if len(neighbors) < min_pts:
            labels[point_idx] = -1
        else:
            cluster_id += 1
            expand_cluster(data, labels, point_idx, neighbors, cluster_id, eps, min_pts)
    
    print(f"Progress: {n}/{n} (100.0%)    ")
    labels = np.where(labels <= 0, -1, labels - 1)
    return labels


print("=" * 80)
print("CLUSTERING PTS - DATA MINING TECHNIQUE")
print("Mengelompokkan PTS berdasarkan kemiripan karakteristik:")
print("Akreditasi, Lokasi, Program Studi, dan Biaya")
print("=" * 80)

# Load data
print("\nLoading data...")
df_prodi = pd.read_csv(BASE_PATH + "merged_prodi_final.csv")
df_inst = pd.read_csv(BASE_PATH + "merged_institutions.csv")

print(f"Total prodi: {len(df_prodi)}")
print(f"Total institutions: {len(df_inst)}")

# Merge untuk mendapatkan info lengkap
print("\nMerging data...")
df_merged = df_prodi.merge(
    df_inst,
    on='institution_code',
    how='inner'
)

print(f"Merged data: {len(df_merged)} rows")

# Filter data yang lengkap
required_cols = ['prodi', 'faculty', 'province', 'average_yearly_fee', 
                 'campus_accreditation', 'body_type']
df_merged = df_merged.dropna(subset=required_cols)
print(f"After cleaning: {len(df_merged)} rows")

# Sampling untuk demo (hapus jika mau proses semua)
SAMPLE_SIZE = 2000
if len(df_merged) > SAMPLE_SIZE:
    print(f"Sampling {SAMPLE_SIZE} rows for demo...")
    df_merged = df_merged.sample(n=SAMPLE_SIZE, random_state=42)

print("\n" + "=" * 80)
print("FITUR CLUSTERING:")
print("=" * 80)
print("1. Akreditasi Institusi")
print("2. Akreditasi Program Studi")
print("3. Lokasi/Provinsi")
print("4. Biaya Kuliah per Tahun")
print("5. Jenis Institusi (NEGERI/SWASTA)")
print("6. Fakultas/Bidang Studi")
print("7. Jenjang Pendidikan")

# Encode categorical features
print("\nEncoding features...")
df_merged['province_encoded'] = pd.Categorical(df_merged['province']).codes
df_merged['body_type_encoded'] = pd.Categorical(df_merged['body_type'].fillna('SWASTA')).codes
df_merged['campus_acc_encoded'] = pd.Categorical(df_merged['campus_accreditation'].fillna('-').str.upper().str.strip()).codes
df_merged['prodi_acc_encoded'] = pd.Categorical(df_merged['accreditation'].fillna('-').str.upper().str.strip()).codes
df_merged['edu_level_encoded'] = pd.Categorical(df_merged['edu_level'].fillna('S1')).codes
df_merged['faculty_encoded'] = pd.Categorical(df_merged['faculty']).codes

# Normalize biaya
df_merged['biaya_normalized'] = np.log1p(df_merged['average_yearly_fee'])

# Prepare features
feature_cols = [
    'campus_acc_encoded',
    'prodi_acc_encoded',
    'province_encoded',
    'biaya_normalized',
    'body_type_encoded',
    'faculty_encoded',
    'edu_level_encoded'
]

features = df_merged[feature_cols].values.astype(float)
features_scaled = normalize_features(features)

# Run DBSCAN
print("\nRunning DBSCAN clustering...")
print("Parameters: eps=0.8, min_samples=3")
clusters = dbscan(features_scaled, eps=0.8, min_pts=3)
df_merged['cluster'] = clusters

# Remove noise
df_clustered = df_merged[df_merged['cluster'] != -1].copy()
df_noise = df_merged[df_merged['cluster'] == -1].copy()

# Statistics
n_clusters = len(set(clusters)) - (1 if -1 in clusters else 0)
n_noise = list(clusters).count(-1)

print("\n" + "=" * 80)
print("CLUSTERING RESULTS")
print("=" * 80)
print(f"Total data points: {len(df_merged)}")
print(f"Number of clusters: {n_clusters}")
print(f"Noise/outliers: {n_noise} ({n_noise/len(df_merged)*100:.1f}%)")
print(f"Successfully clustered: {len(df_clustered)} ({len(df_clustered)/len(df_merged)*100:.1f}%)")

# Analisis karakteristik per cluster
print("\n" + "=" * 80)
print("KARAKTERISTIK SETIAP CLUSTER")
print("=" * 80)

cluster_profiles = []
for cluster_id in sorted(df_clustered['cluster'].unique()):
    cluster_data = df_clustered[df_clustered['cluster'] == cluster_id]
    
    # Hitung karakteristik
    akred_institusi = cluster_data['campus_accreditation'].mode()[0] if len(cluster_data) > 0 else '-'
    akred_prodi = cluster_data['accreditation'].mode()[0] if len(cluster_data) > 0 else '-'
    provinsi = cluster_data['province'].mode()[0] if len(cluster_data) > 0 else '-'
    jenis = cluster_data['body_type'].mode()[0] if len(cluster_data) > 0 else '-'
    fakultas = cluster_data['faculty'].mode()[0] if len(cluster_data) > 0 else '-'
    
    # Statistik biaya
    biaya_min = cluster_data['average_yearly_fee'].min()
    biaya_max = cluster_data['average_yearly_fee'].max()
    biaya_avg = cluster_data['average_yearly_fee'].mean()
    
    profile = {
        'Cluster': f'Cluster {cluster_id + 1}',
        'Jumlah_PTS': cluster_data['institution_name'].nunique(),
        'Jumlah_Prodi': len(cluster_data),
        'Akreditasi_Institusi': akred_institusi,
        'Akreditasi_Prodi': akred_prodi,
        'Provinsi_Dominan': provinsi,
        'Jenis': jenis,
        'Fakultas_Dominan': fakultas,
        'Biaya_Min': biaya_min,
        'Biaya_Max': biaya_max,
        'Biaya_Rata': biaya_avg,
    }
    cluster_profiles.append(profile)
    
    print(f"\n{profile['Cluster']}:")
    print(f"  Profil: {jenis} | Akreditasi {akred_institusi} | {provinsi}")
    print(f"  Jumlah: {profile['Jumlah_PTS']} institusi, {profile['Jumlah_Prodi']} program studi")
    print(f"  Biaya: Rp {biaya_min:,.0f} - Rp {biaya_max:,.0f} (rata-rata: Rp {biaya_avg:,.0f})")
    print(f"  Fakultas dominan: {fakultas}")

# Save cluster profiles summary
df_profiles = pd.DataFrame(cluster_profiles)
profile_file = BASE_PATH + 'cluster_profiles.csv'
df_profiles.to_csv(profile_file, index=False, encoding='utf-8-sig')
print(f"\nCluster profiles saved to: {profile_file}")

# Save main clustering result (1 file saja)
print("\n" + "=" * 80)
print("SAVING RESULTS")
print("=" * 80)

output_cols = [
    'cluster',
    'institution_code',
    'institution_name',
    'province',
    'body_type',
    'campus_accreditation',
    'faculty',
    'prodi',
    'edu_level',
    'accreditation',
    'average_yearly_fee',
    'average_semester_fee'
]

df_output = df_clustered[output_cols].copy()
df_output['cluster'] = df_output['cluster'] + 1  # Start from 1
df_output = df_output.sort_values(['cluster', 'average_yearly_fee'])

# Save to single file
output_file = BASE_PATH + 'pts_clustering_result.csv'
df_output.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"✓ Clustering result: {output_file} ({len(df_output)} records)")

# Save outliers
noise_file = BASE_PATH + 'outliers.csv'
df_noise[output_cols[1:]].to_csv(noise_file, index=False, encoding='utf-8-sig')
print(f"✓ Outliers: {noise_file} ({len(df_noise)} records)")

# Contoh analisis untuk laporan
print("\n" + "=" * 80)
print("ANALISIS UNTUK LAPORAN")
print("=" * 80)

print("\n1. Distribusi Cluster berdasarkan Jenis Institusi:")
cluster_by_type = df_output.groupby(['cluster', 'body_type']).size().reset_index(name='count')
print(cluster_by_type.to_string(index=False))

print("\n2. Distribusi Cluster berdasarkan Akreditasi:")
cluster_by_acc = df_output.groupby(['cluster', 'campus_accreditation']).size().reset_index(name='count')
print(cluster_by_acc.to_string(index=False))

print("\n3. Range Biaya per Cluster:")
biaya_summary = df_output.groupby('cluster')['average_yearly_fee'].agg(['min', 'max', 'mean']).reset_index()
biaya_summary.columns = ['Cluster', 'Biaya_Min', 'Biaya_Max', 'Biaya_Rata']
print(biaya_summary.to_string(index=False))

# Save analysis summary
analysis_file = BASE_PATH + 'cluster_analysis_summary.csv'
with open(analysis_file, 'w', encoding='utf-8-sig') as f:
    f.write("CLUSTER ANALYSIS SUMMARY\n\n")
    f.write("1. Distribusi berdasarkan Jenis Institusi:\n")
    cluster_by_type.to_csv(f, index=False)
    f.write("\n2. Distribusi berdasarkan Akreditasi:\n")
    cluster_by_acc.to_csv(f, index=False)
    f.write("\n3. Range Biaya per Cluster:\n")
    biaya_summary.to_csv(f, index=False)

print(f"\n✓ Analysis summary: {analysis_file}")

print("\n" + "=" * 80)
print("CLUSTERING COMPLETE!")
print("=" * 80)
print("\nManfaat Clustering:")
print("✓ Mengelompokkan PTS dengan profil serupa")
print("✓ Memudahkan perbandingan dan identifikasi PTS sesuai kriteria")
print("✓ Tidak ada ranking/peringkat, hanya pengelompokan berdasarkan kemiripan")
print("\nOutput Files:")
print(f"1. {profile_file} - Profil karakteristik setiap cluster")
print(f"2. {output_file} - Data lengkap dengan label cluster")
print(f"3. {noise_file} - Data outlier (tidak masuk cluster)")
print(f"4. {analysis_file} - Ringkasan analisis untuk laporan")