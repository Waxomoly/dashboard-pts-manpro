import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
from sklearn.preprocessing import StandardScaler
import os

# ===========================
# 1. LOAD DATA
# ===========================

csv_filename = "merged_prodi_final.csv"     
csv_path = os.path.join("..", "csv_result", csv_filename)

if not os.path.exists(csv_path):
    raise FileNotFoundError(f"File tidak ditemukan: {csv_path}")

df = pd.read_csv(csv_path)
print(f"Berhasil load file: {csv_filename}")
print(df.head())

# ===========================
# 2. PILIH KOLOM NUMERIK
# ===========================

numeric_df = df.select_dtypes(include=[np.number]).dropna()

if numeric_df.empty:
    raise ValueError("Tidak ada kolom numerik untuk clustering!")

# ===========================
# 3. NORMALISASI
# ===========================

scaler = StandardScaler()
scaled_data = scaler.fit_transform(numeric_df)

# ===========================
# 4. HIERARCHICAL CLUSTERING
# ===========================

Z = linkage(scaled_data, method="ward")

# ===========================
# 5. PLOT DENDROGRAM
# ===========================

plt.figure(figsize=(12, 6))
dendrogram(Z)
plt.title("Hierarchical Clustering Dendrogram")
plt.xlabel("Sample Index")
plt.ylabel("Distance")
plt.tight_layout()
plt.show()

# ===========================
# 6. TENTUKAN CLUSTER
# ===========================

num_clusters = 4
clusters = fcluster(Z, num_clusters, criterion='maxclust')
df["cluster"] = clusters

# ===========================
# 6B. TAMBAHKAN NAMA CLUSTER
# ===========================

cluster_names = {
    1: "Prodi Unggulan & Kompetitif",
    2: "Prodi Populer di Kampus Besar",
    3: "Prodi Terjangkau & Aksesibel",
    4: "Prodi Spesialis / Fokus Tertentu"
}

df["cluster_name"] = df["cluster"].map(cluster_names)

# ===========================
# 7. RAPIKAN OUTPUT CSV
# ===========================

df = df.sort_values("cluster")
df = df.round(2)

cols = ["cluster", "cluster_name"] + [c for c in df.columns if c not in ["cluster", "cluster_name"]]
df = df[cols]

# ===========================
# 8. SIMPAN HASIL
# ===========================

output_file = f"hasil_clustering_rapi_{csv_filename}"
df.to_csv(output_file, index=False)

print("Clustering selesai!")
print(f"Hasil rapi disimpan sebagai: {output_file}")
