import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import QuantileTransformer

# ================= KONFIGURASI =================
KOLOM_BIAYA = 'average_semester_fee' 
# ===============================================
import pandas as pd

def check_unique_values():
    print("🔍 INSPEKSI NILAI UNIK AKREDITASI")
    print("=================================")
    
    path = r"C:\laragon\www\dashboard-pts-manpro\csv_result"
    
    try:
        # Load kedua file
        df_inst = pd.read_csv(path + r"\merged_institutions.csv", engine='python', on_bad_lines='skip')
        df_prodi = pd.read_csv(path + r"\merged_prodi_final.csv", engine='python', on_bad_lines='skip')
    except Exception as e:
        print(f"❌ Gagal load file: {e}")
        return

    # --- 1. CEK INSTITUSI ---
    col_kampus = 'campus_accreditation'
    
    # Cari kolom jika namanya beda dikit
    if col_kampus not in df_inst.columns:
        for c in df_inst.columns:
            if 'accreditation' in c: col_kampus = c; break
    
    if col_kampus in df_inst.columns:
        # Kita ambil uniknya, ubah jadi string uppercase agar ketahuan variasi tulisannya
        unik_kampus = df_inst[col_kampus].dropna().astype(str).str.strip().str.upper().unique()
        print(f"\n1. Akreditasi KAMPUS (Kolom: {col_kampus})")
        print(f"   Total Variasi: {len(unik_kampus)}")
        print(f"   Isi: {sorted(unik_kampus)}")
    else:
        print("\n⚠️ Kolom akreditasi kampus tidak ditemukan!")

    # --- 2. CEK PRODI ---
    col_prodi = 'accreditation' # Nama default di file prodi
    
    # Cari kolom jika namanya beda dikit
    if col_prodi not in df_prodi.columns:
        for c in df_prodi.columns:
            if 'accreditation' in c: col_prodi = c; break
            
    if col_prodi in df_prodi.columns:
        unik_prodi = df_prodi[col_prodi].dropna().astype(str).str.strip().str.upper().unique()
        print(f"\n2. Akreditasi PRODI (Kolom: {col_prodi})")
        print(f"   Total Variasi: {len(unik_prodi)}")
        print(f"   Isi: {sorted(unik_prodi)}")
    else:
        print("\n⚠️ Kolom akreditasi prodi tidak ditemukan!")
    

def run_complete_clustering_with_location():
    print("🚀 Memulai Clustering Lengkap (Kampus + Prodi + Lokasi)...")
    
    path = r"C:\laragon\www\dashboard-pts-manpro\csv_result"
    
    # --- 1. LOAD DATA ---
    try:
        df_inst = pd.read_csv(path + r"\merged_institutions.csv", engine='python', on_bad_lines='skip')
        df_prodi = pd.read_csv(path + r"\merged_prodi_final.csv", engine='python', on_bad_lines='skip')
        print("✓ Data berhasil dimuat.")
    except Exception as e:
        print(f"❌ Gagal load: {e}")
        return

    # --- 2. MAPPING AKREDITASI (Institusi & Prodi) ---
    map_accred_fix = {
        'UNGGUL': 5, 'TERAKREDITASI UNGGUL': 5, 'A': 5,
        'BAIK SEKALI': 4, 'B': 4,
        'BAIK': 3,
        'C': 2, 'TERAKREDITASI': 2, 'TERAKREDITASI PERTAMA': 2, 'TERAKREDITASI SEMENTARA': 2,
        'TIDAK TERAKREDITASI': 1, '-': 1
    }

    # --- 3. HITUNG SKOR PRODI ---
    print("⚙️ Menghitung skor akademik...")
    # Cari kolom akreditasi prodi
    col_acc_prodi = next((c for c in df_prodi.columns if 'accreditation' in c), 'accreditation')
    
    # Convert huruf ke angka
    df_prodi['skor_prodi'] = df_prodi[col_acc_prodi].astype(str).str.strip().str.upper().map(map_accred_fix).fillna(1)
    
    # Cari kolom ID Institusi
    id_col = next((c for c in df_prodi.columns if 'code' in c and 'institution' in c), 'institution_code')
    
    # Rata-rata skor prodi per kampus
    prodi_agg = df_prodi.groupby(id_col)['skor_prodi'].mean().reset_index()
    prodi_agg.columns = ['institution_code', 'Rata_Mutu_Prodi']

    # --- 4. MERGE DATA & CEK LOKASI ---
    df_inst['institution_code'] = pd.to_numeric(df_inst['institution_code'], errors='coerce')
    prodi_agg['institution_code'] = pd.to_numeric(prodi_agg['institution_code'], errors='coerce')
    
    df_merged = df_inst.merge(prodi_agg, on='institution_code', how='left')
    df_merged['Rata_Mutu_Prodi'] = df_merged['Rata_Mutu_Prodi'].fillna(1)

    # Bersihkan Biaya
    df_merged[KOLOM_BIAYA] = pd.to_numeric(df_merged[KOLOM_BIAYA], errors='coerce')
    
    # Filter Data Valid (Hapus error triliunan)
    df_valid = df_merged[
        (df_merged[KOLOM_BIAYA].notna()) & 
        (df_merged[KOLOM_BIAYA] < 500000000)
    ].copy()

    # --- 5. PERSIAPAN FITUR CLUSTERING ---
    # A. Biaya (Log Transform)
    df_valid['log_fee'] = np.log1p(df_valid[KOLOM_BIAYA])
    
    # B. Akreditasi Institusi (PENTING: Ini yang Anda minta)
    col_acc_kampus = next((c for c in df_valid.columns if 'accreditation' in c), 'campus_accreditation')
    df_valid['Skor_Kampus'] = df_valid[col_acc_kampus].astype(str).str.strip().str.upper().map(map_accred_fix).fillna(1)
    
    # C. Lokasi (Kita pastikan kolom ini ada untuk output)
    col_lokasi = next((c for c in df_valid.columns if 'province' in c or 'provinsi' in c), 'province')
    if col_lokasi not in df_valid.columns:
        df_valid['province'] = 'Unknown' # Default jika tidak ada
        col_lokasi = 'province'

    # --- 6. EKSEKUSI CLUSTERING (k=7) ---
    print("🤖 Menghitung Cluster berdasarkan Biaya, Mutu Kampus, & Mutu Prodi...")
    
    # Kita hanya clustering berdasarkan KUALITAS & HARGA.
    # Lokasi TIDAK dimasukkan ke rumus X agar cluster tidak pecah berdasarkan wilayah.
    X = df_valid[['log_fee', 'Skor_Kampus', 'Rata_Mutu_Prodi']].copy()
    
    scaler = QuantileTransformer(output_distribution='normal', random_state=42)
    X_scaled = scaler.fit_transform(X)
    
    kmeans = KMeans(n_clusters=7, random_state=42, n_init=10)
    df_valid['cluster'] = kmeans.fit_predict(X_scaled)

    # --- 7. LABELING CERDAS ---
    summary = df_valid.groupby('cluster').agg(
        Biaya_Rata=(KOLOM_BIAYA, 'mean'),
        Mutu_Kampus=('Skor_Kampus', 'mean'),
        Mutu_Prodi=('Rata_Mutu_Prodi', 'mean')
    )

    def generate_label(row):
        # Label Harga
        biaya = row['Biaya_Rata']
        if biaya < 2500000: tag_h = "Sangat Hemat"
        elif biaya < 5000000: tag_h = "Hemat"
        elif biaya < 8500000: tag_h = "Terjangkau"
        elif biaya < 13000000: tag_h = "Menengah"
        elif biaya < 20000000: tag_h = "Premium"
        else: tag_h = "Eksklusif"
        
        # Label Mutu (Institusi + Prodi)
        m_inst = row['Mutu_Kampus']
        m_prodi = row['Mutu_Prodi']
        
        # Deteksi data prodi kosong/jelek
        if m_prodi <= 1.5:
            return "Perlu Cek Data Prodi"
            
        # Rata-rata mutu
        score = (m_inst + m_prodi) / 2
        
        if score >= 4.2: tag_m = "Elite (A)"
        elif score >= 3.8: tag_m = "Unggul (A-)"
        elif score >= 3.0: tag_m = "Sangat Baik (B)"
        elif score >= 2.0: tag_m = "Baik (C)"
        else: tag_m = "Standar"
        
        # Logika Hidden Gem (Murah tapi Bagus)
        if biaya < 6000000 and score >= 3.5:
            return f"⭐ Best Value ({tag_m})"
            
        return f"{tag_h} - {tag_m}"

    summary['Label'] = summary.apply(generate_label, axis=1)
    label_map = summary['Label'].to_dict()
    df_valid['Kategori'] = df_valid['cluster'].map(label_map)

    # --- 8. REPORTING & SAVING ---
    # Kita tampilkan juga Provinsi terbanyak di setiap cluster
    def top_province(x): return x.mode()[0] if not x.mode().empty else '-'
    def q90(x): return x.quantile(0.90)

    final_report = df_valid.groupby(['cluster', 'Kategori']).agg(
        Jumlah=('institution_name', 'count'),
        Biaya_Rata=(KOLOM_BIAYA, 'mean'),
        Mutu_Kampus=('Skor_Kampus', 'mean'),   # Ini bukti Institusi masuk hitungan
        Mutu_Prodi=('Rata_Mutu_Prodi', 'mean'), # Ini bukti Prodi masuk hitungan
        Lokasi_Dominan=(col_lokasi, top_province) # Info lokasi
    ).sort_values('Biaya_Rata')
    
    print("\n=== HASIL FINAL (LENGKAP DENGAN LOKASI) ===")
    pd.set_option('display.float_format', '{:,.0f}'.format)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', 1000)
    print(final_report)
    
    # Save CSV dengan Kolom Lokasi & Akreditasi Institusi
    cols_export = ['institution_name', 'Kategori', 'province', KOLOM_BIAYA, col_acc_kampus, 'Rata_Mutu_Prodi']
    df_valid[cols_export].to_csv('hasil_rekomendasi_final_lengkap.csv', index=False)
    print("\n✓ Hasil tersimpan: 'hasil_rekomendasi_final_lengkap.csv'")
    print("  (Kolom 'province' dan 'campus_accreditation' sudah disertakan untuk filter di Dashboard)")

    # Visualisasi (Opsional)
    plt.figure(figsize=(12, 7))
    sns.scatterplot(data=df_valid, x=KOLOM_BIAYA, y='Rata_Mutu_Prodi', hue='Kategori', style='Skor_Kampus', palette='tab10', s=80)
    plt.xscale('log')
    plt.title('Peta Cluster: Biaya vs Mutu (Simbol = Akreditasi Institusi)')
    plt.xlabel('Biaya Semester (Log Scale)')
    plt.ylabel('Rata-rata Akreditasi Prodi')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig('peta_cluster_lengkap.png')

if __name__ == "__main__":
    run_complete_clustering_with_location()