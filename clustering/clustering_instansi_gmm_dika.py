import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.mixture import GaussianMixture

# ==========================================
# 1. LOAD DATA (Auto-detect path)
# ==========================================
print("Loading data instansi...")
path_root = 'csv_result/merged_institutions.csv'
path_inside = '../csv_result/merged_institutions.csv'

if os.path.exists(path_root):
    file_path = path_root
elif os.path.exists(path_inside):
    file_path = path_inside
else:
    # Fallback terakhir
    file_path = 'csv_result/merged_institutions.csv'

df = pd.read_csv(file_path)

# ==========================================
# 2. PREPROCESSING PIPELINE
# ==========================================
print("Melakukan Preprocessing...")

# Definisi Fitur
numeric_features = [
    'student_amount', 'lecturer_amount', 
    'average_semester_fee', 'average_yearly_fee'
]
categorical_features = [
    'body_type', 'province', 'campus_accreditation'
]

# Cleaning: Ubah -1.0 jadi NaN untuk fitur numerik
for col in numeric_features:
    df[col] = df[col].replace(-1.0, np.nan)

# Pipeline Numerik (Isi NaN dengan Median -> Scaling)
numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())
])

# Pipeline Kategorikal (Isi NaN -> One Hot Encoding)
categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='constant', fill_value='UNKNOWN')),
    ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
])

# Gabung keduanya
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ])

# Terapkan ke data
X_processed = preprocessor.fit_transform(df[numeric_features + categorical_features])

# ==========================================
# 3. EKSEKUSI GMM (5 CLUSTER)
# ==========================================
# Kita pakai 5 Cluster berdasarkan analisis grafik AIC/BIC sebelumnya
print("Menjalankan GMM dengan 5 Cluster...")

gmm = GaussianMixture(n_components=5, random_state=42)
gmm_labels = gmm.fit_predict(X_processed)

# Ambil Confidence Score (Probabilitas tertinggi)
probs = gmm.predict_proba(X_processed)
certainty = probs.max(axis=1)

# Masukkan ke DataFrame sementara
df['cluster_gmm'] = gmm_labels
df['confidence_score'] = certainty

# ==========================================
# 4. MAPPING NAMA (LABELING)
# ==========================================
# Berdasarkan analisis profiling sebelumnya:
# Cluster 4: Mhs 44rb, Biaya 9jt, Akred A -> Besar & Favorit
# Cluster 3: Mhs 7rb, Biaya 8.5jt, Jatim -> Premium 
# Cluster 2: Mhs 2rb, Biaya 8.3jt, Jabar -> Menengah
# Cluster 0: Mhs 3.5rb, Biaya 6.8jt, Baik Sekali -> Menengah Berkualitas
# Cluster 1: Mhs 4.8rb, Biaya 4.5jt -> Terjangkau

mapping_nama_instansi = {
    4: "Kampus Besar & Favorit",
    3: "Kampus Premium",
    2: "Kampus Menengah",
    0: "Kampus Menengah Berkualitas",
    1: "Kampus Terjangkau"
}

df['cluster_category'] = df['cluster_gmm'].map(mapping_nama_instansi)

# ==========================================
# 5. SIMPAN HASIL FINAL
# ==========================================
if os.path.exists('csv_result'):
    output_path = 'csv_result/instansi_clustered_gmm.csv'
else:
    output_path = '../csv_result/instansi_clustered_gmm.csv'

df.to_csv(output_path, index=False)

print("\n" + "="*50)
print(f"SUKSES! Data Instansi berhasil dikelompokkan & diberi nama.")
print(f"File tersimpan di: {output_path}")
print("="*50)

# Tampilkan contoh hasil
print(df[['institution_name', 'cluster_category', 'average_semester_fee']].head(10))