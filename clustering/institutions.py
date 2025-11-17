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

# Jika ada missing values → isi dengan median agar rapi
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
# 7. RAPIKAN CSV
# ============================

# Tentukan urutan kolom agar tidak acak-acakan
ordered_columns = (
    ["institution_name", "institution_code", "body_type", "link",
     "province", "campus_accreditation", "banpt_code", "rank"]
    + list(numeric_df.columns)  # kolom numerik
    + ["cluster"]
)

# Hanya ambil kolom yang benar-benar ada
ordered_columns = [c for c in ordered_columns if c in df.columns]

df_clean = df[ordered_columns]

# Simpan CSV rapi tanpa index
output_file = f"hasil_clustering_rapi_{csv_filename}"
df_clean.to_csv(output_file, index=False)

print("Clustering selesai!")
print(f"Hasil CSV rapi disimpan sebagai: {output_file}")
