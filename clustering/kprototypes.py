import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from kmodes.kprototypes import KPrototypes
from sklearn.preprocessing import MinMaxScaler


try:
    df_institusi = pd.read_csv('merged_institutions.csv')
    df_prodi = pd.read_csv('merged_prodi_final.csv')
except FileNotFoundError:
    print("Error: Pastikan file 'merged_institutions.csv' dan 'merged_prodi_final.csv' ada di direktori yang sama.")
    exit()

print("Data berhasil dimuat.")

# Menggabungkan data prodi dengan data institusinya
# Menggunakan 'left' join agar semua prodi tetap ada
df_full = pd.merge(df_prodi, df_institusi, on='institution_code', how='left')

print(f"Data berhasil digabung. Total baris: {len(df_full)}")

# Kolom numerik yang digunakan
kolom_numerik = [
    'average_semester_fee', # Biaya semester rata-rata
    'student_amount',       # Jumlah mahasiswa di institusi
    'lecturer_amount'       # Jumlah dosen di institusi
]

# Kolom kategorikal yang digunakan
kolom_kategorikal = [
    'institution_name',
    'faculty',              # Fakultas prodi
    'edu_level',            # Jenjang pendidikan (S1, D3, dll)
    'accreditation',        # Akreditasi prodi
    'body_type',            # Tipe institusi (NEGERI, SWASTA)
    'province',             # Provinsi institusi
    'campus_accreditation'  # Akreditasi institusi
]

kolom_untuk_cluster = kolom_numerik + kolom_kategorikal
df_cluster = df_full[kolom_untuk_cluster].copy()

# Data cleaning
print(f"Data awal sebelum dibersihkan: {len(df_cluster)} baris")
df_cluster[kolom_numerik] = df_cluster[kolom_numerik].replace(-1.0, np.nan)

# Ganti nilai '-' di kolom kategorikal menjadi NaN (data hilang)
df_cluster[kolom_kategorikal] = df_cluster[kolom_kategorikal].replace('-', np.nan)

initial_rows = len(df_cluster)
df_cluster = df_cluster.dropna()
cleaned_rows = len(df_cluster)
print(f"Membersihkan data: {initial_rows - cleaned_rows} baris dihapus karena data kosong.")
print(f"Jumlah baris siap cluster: {cleaned_rows}")

# Konversi tipe data
for col in kolom_kategorikal:
    df_cluster[col] = df_cluster[col].astype(str)

# Penskalaan (Scaling) Data Numerik 
scaler = MinMaxScaler()
df_scaled = df_cluster.copy()
df_scaled[kolom_numerik] = scaler.fit_transform(df_scaled[kolom_numerik])


# Pisahkan data numerik dan kategorikal untuk input model
df_numerik_scaled = df_scaled[kolom_numerik]
df_kategorikal = df_scaled[kolom_kategorikal]

# Gabungkan kembali sebagai array numpy (matriks)
data_matrix = np.hstack((df_numerik_scaled.values, df_kategorikal.values))

# Dapatkan posisi (indeks) kolom kategorikal
posisi_kolom_kategorikal = list(range(len(kolom_numerik), data_matrix.shape[1]))

print(f"\nData siap untuk clustering.")
print(f"Jumlah kolom numerik: {len(kolom_numerik)}")
print(f"Jumlah kolom kategorikal: {len(kolom_kategorikal)}")
print(f"Posisi kolom kategorikal (indeks): {posisi_kolom_kategorikal}")


# Menentukan Jumlah Klaster (k) dengan Elbow Method 
print("\nMencari jumlah k optimal dengan Elbow Method...")
costs = []
K_range = range(2, 11) # Mencoba k dari 2 sampai 10

for k in K_range:
    print(f"Mencoba k={k}...")
    kproto = KPrototypes(n_clusters=k, init='Huang', n_init=10, random_state=42, n_jobs=-1, verbose=0)
    kproto.fit(data_matrix, categorical=posisi_kolom_kategorikal)
    costs.append(kproto.cost_)

# Plot Elbow Method
plt.figure()
plt.plot(K_range, costs, 'bx-')
plt.xlabel('Jumlah Klaster (k)')
plt.ylabel('Cost (Inertia)')
plt.title('Elbow Method untuk K-Prototypes')
plt.savefig('elbow_plot.png')
print("Grafik Elbow Method disimpan sebagai 'elbow_plot.png'")


# Menjalankan Model K-Prototypes Final 
K_OPTIMAL = 5 

print(f"\nMenjalankan clustering final dengan k={K_OPTIMAL}...")
kproto_final = KPrototypes(n_clusters=K_OPTIMAL, init='Huang', n_init=10, random_state=42, n_jobs=-1, verbose=0)
clusters = kproto_final.fit_predict(data_matrix, categorical=posisi_kolom_kategorikal)

# Analisis Hasil 
df_cluster['klaster'] = clusters

print("\n--- Analisis Hasil Clustering ---")

# Analisis fitur numerik (rata-rata per klaster)
print("\nRata-rata Fitur Numerik per Klaster:")
analisis_numerik = df_cluster.groupby('klaster')[kolom_numerik].mean()
print(analisis_numerik)

# Analisis fitur kategorikal (nilai paling umum per klaster)
print("\nModus (Nilai Paling Umum) Fitur Kategorikal per Klaster:")
for col in kolom_kategorikal:
    print(f"\n--- Analisis '{col}' ---")
    # Menggunakan describe() untuk mendapatkan 'top' (modus) dan 'freq' (frekuensi modus)
    print(df_cluster.groupby('klaster')[col].describe())

# Menyimpan Hasil 
df_cluster.to_csv('hasil_clustering.csv', index=False)
print(f"\nAnalisis selesai. Hasil di 'hasil_clustering.csv'")

# Menampilkan 5 baris pertama dari hasil
print("\nContoh 5 baris pertama dari hasil:")
print(df_cluster.head())