import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
from sklearn.preprocessing import StandardScaler
import os


# 1. LOAD DATA DARI FOLDER csv_result/


# Pilih salah satu file:
csv_filename = "merged_prodi_final.csv"     

csv_path = os.path.join("..", "csv_result", csv_filename)

if not os.path.exists(csv_path):
    raise FileNotFoundError(f"File tidak ditemukan: {csv_path}")

df = pd.read_csv(csv_path)
print(f"Berhasil load file: {csv_filename}")
print(df.head())

# 2. PILIH KOLOM NUMERIK SAJA

numeric_df = df.select_dtypes(include=[np.number]).dropna()

if numeric_df.empty:
    raise ValueError("Tidak ada kolom numerik untuk clustering!")


# 3. NORMALISASI

scaler = StandardScaler()
scaled_data = scaler.fit_transform(numeric_df)

# 4. HIERARCHICAL CLUSTERING

Z = linkage(scaled_data, method="ward")

# 5. PLOT DENDROGRAM

plt.figure(figsize=(12, 6))
dendrogram(Z)
plt.title("Hierarchical Clustering Dendrogram")
plt.xlabel("Sample Index")
plt.ylabel("Distance")
plt.tight_layout()
plt.show()

# 6. BENTUK CLUSTER (misal 4)

num_clusters = 4
clusters = fcluster(Z, num_clusters, criterion='maxclust')

df["cluster"] = clusters

# 7. SIMPAN HASIL KE CSV

output_file = f"hasil_clustering_{csv_filename}"
df.to_csv(output_file, index=False)

print("Clustering selesai!")
print(f"Hasil disimpan sebagai: {output_file}")
