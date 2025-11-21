import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
from sklearn.preprocessing import StandardScaler
import os

# ============================
# 1. LOAD FILE CSV
# ============================

csv_filename = "merged_institutions.csv"
csv_path = os.path.join("..", "csv_result", csv_filename)

if not os.path.exists(csv_path):
    raise FileNotFoundError(f"File tidak ditemukan: {csv_path}")

df = pd.read_csv(csv_path)
print(f"Berhasil load file: {csv_filename}")
print(df.head())

# ============================
# 2. PILIH KOLOM NUMERIK
# ============================

numeric_df = df.select_dtypes(include=[np.number])
numeric_df = numeric_df.fillna(numeric_df.median())

if numeric_df.empty:
    raise ValueError("Tidak ada kolom numerik untuk clustering!")

# ============================
# 3. NORMALISASI DATA
# ============================

scaler = StandardScaler()
scaled_data = scaler.fit_transform(numeric_df)

# ============================
# 4. HIERARCHICAL CLUSTERING
# ============================

Z = linkage(scaled_data, method="ward")

# ============================
# 5. DENDROGRAM
# ============================

plt.figure(figsize=(12, 6))
dendrogram(Z)
plt.title("Hierarchical Clustering Dendrogram")
plt.xlabel("Sample Index")
plt.ylabel("Distance")
plt.tight_layout()
plt.show()

# ============================
# 6. BENTUK CLUSTER
# ============================

num_clusters = 4
clusters = fcluster(Z, num_clusters, criterion='maxclust')
df["cluster"] = clusters

# ============================
# 6B. TAMBAHKAN NAMA CLUSTER
# ============================

cluster_names = {
    1: "Perguruan Tinggi Murah di Kota Besar",
    2: "Perguruan Tinggi Unggulan Akreditasi Tinggi",
    3: "Perguruan Tinggi Menengah di Daerah",
    4: "Perguruan Tinggi Premium & Prestisius"
}

df["cluster_name"] = df["cluster"].map(cluster_names)

# ============================
# 7. RAPIKAN CSV
# ============================

ordered_columns = (
    ["cluster", "cluster_name", "institution_name", "institution_code", "body_type",
     "link", "province", "campus_accreditation", "banpt_code", "rank"]
    + list(numeric_df.columns)
)

ordered_columns = [c for c in ordered_columns if c in df.columns]
df_clean = df[ordered_columns]

# ============================
# 8. SIMPAN CSV
# ============================

output_file = f"hasil_clustering_rapi_{csv_filename}"
df_clean.to_csv(output_file, index=False)

print("Clustering selesai!")
print(f"Hasil CSV rapi disimpan sebagai: {output_file}")
