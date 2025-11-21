import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import QuantileTransformer

# ================= KONFIGURASI =================
KOLOM_BIAYA = 'average_semester_fee' 
# ===============================================

def run_smart_clustering():
    print("🚀 Memulai Clustering Cerdas (Harga + Mutu)...")
    
    # 1. LOAD DATA
    path = r"C:\laragon\www\dashboard-pts-manpro\csv_result"
    try:
        df = pd.read_csv(path + r"\merged_institutions.csv", engine='python', on_bad_lines='skip')
    except Exception as e:
        print(f"❌ Gagal load: {e}")
        return

    # 2. CLEANING & FILTERING
    df[KOLOM_BIAYA] = pd.to_numeric(df[KOLOM_BIAYA], errors='coerce')
    
    # PENTING: Kita buang 19 Triliun agar rata-rata harga tidak rusak
    BATAS_ATAS = 500000000 # 500 Juta
    
    df_valid = df[
        (df[KOLOM_BIAYA].notna()) & 
        (df[KOLOM_BIAYA] < BATAS_ATAS) # Filter Error Triliunan
    ].copy()
    
    print(f"✓ Data Valid: {len(df_valid)} kampus")

    # 3. PREPARE DATA (AKREDITASI)
    col_accred = 'campus_accreditation'
    if col_accred not in df_valid.columns:
        for c in df_valid.columns:
            if 'accreditation' in c: col_accred = c; break
            
    map_accred = {'UNGGUL': 5, 'A': 5, 'BAIK SEKALI': 4, 'B': 4, 'BAIK': 3, 'C': 2}
    df_valid['score_kampus'] = df_valid[col_accred].astype(str).str.upper().str.strip().map(map_accred).fillna(1)

    # 4. CLUSTERING
    df_valid['log_fee'] = np.log1p(df_valid[KOLOM_BIAYA])
    X = df_valid[['log_fee', 'score_kampus']].copy()
    
    # QuantileTransformer untuk sebaran normal
    scaler = QuantileTransformer(output_distribution='normal', random_state=42)
    X_scaled = scaler.fit_transform(X)
    
    # Kita pakai 5 Cluster
    kmeans = KMeans(n_clusters=5, random_state=42, n_init=10)
    df_valid['cluster'] = kmeans.fit_predict(X_scaled)
    
    # 5. ANALISIS CLUSTER (SUMMARY)
    summary = df_valid.groupby('cluster').agg(
        Jumlah=('score_kampus', 'count'),
        Biaya_Rata=(KOLOM_BIAYA, 'mean'),
        Mutu_Rata=('score_kampus', 'mean')
    )

    # === 6. SMART LABELING (LOGIKA PEMBERIAN NAMA) ===
    # Fungsi ini memberi nama berdasarkan Fakta Data, bukan urutan manual
    def generate_label(row):
        # A. Tentukan Level Harga
        if row['Biaya_Rata'] < 3000000:
            price_tag = "Ekonomis"
        elif row['Biaya_Rata'] < 7000000:
            price_tag = "Terjangkau"
        elif row['Biaya_Rata'] < 15000000:
            price_tag = "Menengah"
        else:
            price_tag = "Premium"
            
        # B. Tentukan Level Mutu (Rata-rata skor di cluster itu)
        # Ingat: 5=Unggul, 4=Baik Sekali, 3=Baik, 2=C
        if row['Mutu_Rata'] >= 4.5:
            quality_tag = "Unggul (A)"
        elif row['Mutu_Rata'] >= 3.5:
            quality_tag = "Sangat Baik (B)"
        elif row['Mutu_Rata'] >= 2.5:
            quality_tag = "Baik (C)"
        else:
            quality_tag = "Perlu Dicek"
            
        # C. Gabungkan
        return f"{price_tag} - {quality_tag}"

    # Terapkan fungsi penamaan ke setiap cluster
    summary['Label_Otomatis'] = summary.apply(generate_label, axis=1)
    
    # Mapping label ke dataframe utama
    label_map = summary['Label_Otomatis'].to_dict()
    df_valid['Kategori'] = df_valid['cluster'].map(label_map)
    
    # 7. HASIL FINAL
    # Kita group berdasarkan Label Baru agar rapi
    final_summary = df_valid.groupby('Kategori').agg(
        Jumlah=('score_kampus', 'count'),
        Biaya_Min=(KOLOM_BIAYA, 'min'),
        Biaya_Rata=(KOLOM_BIAYA, 'mean'),
        Biaya_Max=(KOLOM_BIAYA, 'max'),
        Mutu_Rata=('score_kampus', 'mean')
    ).sort_values('Biaya_Rata')
    
    print("\n=== HASIL FINAL (LABEL DINAMIS) ===")
    pd.set_option('display.float_format', '{:,.0f}'.format)
    # Lebarkan kolom agar nama label terlihat
    pd.set_option('display.max_colwidth', None) 
    print(final_summary)
    
    # Save
    df_valid[['institution_name', 'Kategori', KOLOM_BIAYA, col_accred]].to_csv('hasil_smart_clustering.csv', index=False)
    print("\n✓ Tersimpan di 'hasil_smart_clustering.csv'")
    
    # Visualisasi
    plt.figure(figsize=(11, 7))
    sns.scatterplot(data=df_valid, x=KOLOM_BIAYA, y='score_kampus', hue='Kategori', palette='Set1', s=60)
    plt.xscale('log')
    plt.title('Peta Cluster Universitas (Label Harga & Mutu)')
    plt.xlabel('Biaya Semester (Log Scale)')
    plt.ylabel('Skor Akreditasi')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left') # Legenda di luar agar rapi
    plt.tight_layout()
    plt.savefig('peta_smart_cluster.png')

if __name__ == "__main__":
    run_smart_clustering()