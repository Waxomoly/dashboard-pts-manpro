import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
from sklearn.metrics import silhouette_score
import os
import warnings
import sys
warnings.filterwarnings('ignore')

BASE_PATH = "csv_result/"
OUTPUT_PATH = "csv_manual/"
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

MIN_DATA_FOR_CLUSTERING = 5 # Minimum data buat clustering set 5
AKRED_WEIGHT = 2.0  # Bobot akreditasi untuk clustering

# buat bantu kategori prodi yang lebih luas
def get_broad_category(prodi_norm):
    p = str(prodi_norm).upper().strip()

    if any(k in p for k in ['PENDIDIKAN', 'KEGURUAN', 'PGSD', 'PAUD', 'GURU', 'BIMBINGAN', 'PENJASKES', 'TARBIYAH', 'JASMANI', 'OLAHRAGA']): 
        return 'Pendidikan'
    if any(k in p for k in ['SASTRA', 'BAHASA', 'SENI', 'DESAIN', 'DKV', 'PARIWISATA', 'PERHOTELAN', 'MUSIK', 'FASHION', 'TEATER', 'FILM', 'ANIMASI']): 
        return 'Seni & Pariwisata'
    if any(k in p for k in ['KEDOKTERAN', 'DOKTER', 'KEPERAWATAN', 'NERS', 'BIDAN', 'FARMASI', 'APOTEKER', 'GIZI', 'FISIOTERAPI', 'RADIOLOGI', 'BIOMEDIS', 'KESEHATAN', 'MEDIS', 'K3']): 
        return 'Kesehatan'
    if any(k in p for k in ['INFORMATIKA', 'KOMPUTER', 'SISTEM INFORMASI', 'TEKNOLOGI INFORMASI', 'DATA', 'SOFTWARE', 'JARINGAN', 'SIBER', 'GAME', 'MULTIMEDIA', 'ROBOTIKA', 'REKAYASA PERANGKAT LUNAK', 'KECERDASAN BUATAN']): 
        return 'Komputer & Teknologi'
    if any(k in p for k in ['TEKNIK', 'SIPIL', 'ARSITEKTUR', 'MESIN', 'ELEKTRO', 'INDUSTRI', 'LINGKUNGAN', 'PLANOLOGI', 'PERTAMBANGAN', 'PERMINYAKAN', 'GEODESI', 'PERKAPALAN', 'DIRGANTARA', 'OTOMOTIF', 'ENERGI', 'MANUFAKTUR', 'METALURGI', 'KELAUTAN', 'REKAYASA', 'TRANSPORTASI', 'PENERBANGAN']): 
        return 'Teknik & Arsitektur'
    if any(k in p for k in ['PERTANIAN', 'AGRO', 'PETERNAKAN', 'KEHUTANAN', 'PERIKANAN', 'BIOLOGI', 'KIMIA', 'FISIKA', 'MATEMATIKA', 'STATISTIK', 'MIPA', 'GEOGRAFI', 'GEOLOGI', 'IPA', 'ALAM', 'HAYATI', 'HEWAN', 'AKUAKULTUR', 'BIOKIMIA']): 
        return 'Sains & Agro'
    if any(k in p for k in ['HUKUM', 'KOMUNIKASI', 'HUBUNGAN', 'SOSIOLOGI', 'POLITIK', 'PEMERINTAHAN', 'PSIKOLOGI', 'KRIMINOLOGI', 'SOSIAL', 'NEGARA', 'PUBLIK', 'SEJARAH', 'ANTROPOLOGI', 'JURNALISTIK', 'KEWARGANEGARAAN', 'PANCASILA', 'HUMANITAS']): 
        return 'Sosial & Hukum'
    if any(k in p for k in ['EKONOMI', 'AKUNTANSI', 'MANAJEMEN', 'BISNIS', 'KEUANGAN', 'PAJAK', 'PERBANKAN', 'PEMASARAN', 'LOGISTIK', 'ADMINISTRASI', 'KEWIRAUSAHAAN', 'SEKRETARIAT', 'PERDAGANGAN', 'AKTUARIA', 'MARKETING']): 
        return 'Ekonomi & Bisnis'    
    if any(k in p for k in ['FILSAFAT', 'TEOLOGI']): 
        return 'Agama & Filsafat'
    return 'Lainnya'

def get_price_segment(fee):
    if fee >= 50_000_000: return "SEGMENT_E"
    elif fee >= 20_000_000: return "SEGMENT_D"
    elif fee >= 10_000_000: return "SEGMENT_C"
    elif fee >= 5_000_000: return "SEGMENT_B"
    else: return "SEGMENT_A"

def get_price_tag(fee):
    if fee >= 50_000_000: return "Sangat Mahal (>50jt)"
    elif fee >= 20_000_000: return "Mahal (20-50jt)"
    elif fee >= 10_000_000: return "Menengah (10-20jt)"
    elif fee >= 5_000_000: return "Terjangkau (5-10jt)"
    else: return "Hemat (<5jt)"

def get_acc_score_detailed(akred_text): # Ubah teks akredutasi jadi skor
    akred = str(akred_text).upper().strip()
    if 'UNGGUL' in akred: return 5
    elif 'A' in akred and 'BAIK' not in akred: return 4
    elif 'BAIK SEKALI' in akred or 'B' in akred: return 3
    elif 'BAIK' in akred: return 2
    elif 'C' in akred: return 1
    else: return 0

# Ubah score akred menjadi label lagi untuk dashboard
def get_acc_dashboard_label(score):
    if score >= 4: return "Unggul / A"        # Skor 4 & 5
    elif score >= 3: return "Baik Sekali / B" # Skor 3
    elif score >= 1: return "Baik / C"        # Skor 1 & 2
    else: return "Belum Terakreditasi"


df_inst = pd.read_csv(os.path.join(BASE_PATH, "merged_institutions.csv"), on_bad_lines='skip')
df_prodi = pd.read_csv(os.path.join(BASE_PATH, "merged_prodi_final.csv"), on_bad_lines='skip')

df_inst.columns = df_inst.columns.str.strip()
df_prodi.columns = df_prodi.columns.str.strip()

# Normalisasi provinsi untuk dki dan jogja
df_inst['province'] = df_inst['province'].astype(str).str.upper().str.replace('.', '', regex=False).str.strip()
norm_prov = {'DIY': 'DI YOGYAKARTA', 'DAERAH ISTIMEWA YOGYAKARTA': 'DI YOGYAKARTA', 'YOGYAKARTA': 'DI YOGYAKARTA', 
             'DKI': 'DKI JAKARTA', 'JAKARTA': 'DKI JAKARTA'}
df_inst['province'] = df_inst['province'].replace(norm_prov)

# Merge
df_merged = df_prodi.merge(
    df_inst[['institution_code', 'institution_name', 'province', 'contact', 'average_yearly_fee', 'campus_accreditation', 'link']],
    on='institution_code', how='inner'
)

# Filter untuk 5 provinsi yang besar
target_locations = ['DKI JAKARTA', 'DI YOGYAKARTA', 'JAWA BARAT', 'JAWA TENGAH', 'JAWA TIMUR']
df_merged = df_merged[df_merged['province'].isin(target_locations)]
target_col = 'prodi_normalized' if 'prodi_normalized' in df_merged.columns else 'prodi'
df_merged['broad_category'] = df_merged[target_col].apply(get_broad_category)

df_merged['campus_accreditation'] = np.where(
    (df_merged['accreditation'].notna()) & (df_merged['accreditation'] != '-'), 
    df_merged['accreditation'], 
    df_merged['campus_accreditation']
)
df_merged['final_acc_score'] = df_merged['campus_accreditation'].apply(get_acc_score_detailed)
df_merged['acc_tier'] = df_merged['final_acc_score'].apply(get_acc_dashboard_label)

df_merged['average_yearly_fee'] = pd.to_numeric(df_merged['average_yearly_fee'], errors='coerce')
df_merged = df_merged[df_merged['average_yearly_fee'] > 0]
df_merged['average_yearly_fee'] = df_merged['average_yearly_fee'].fillna(df_merged['average_yearly_fee'].median())  
df_merged['price_segment'] = df_merged['average_yearly_fee'].apply(get_price_segment)

# Clustering method
def optimize_clustering(dataframe):
    results = []
    
    # Loop 1: lakukan clustering per provinsi
    for prov in dataframe['province'].unique():
        prov_data = dataframe[dataframe['province'] == prov]
        
        # Loop 2: lakukan clustering per kategori prodi
        for cat in prov_data['broad_category'].unique():
            cat_data = prov_data[prov_data['broad_category'] == cat]
            
            # Loop 3: lakukan clustering per segment harga
            for segment in cat_data['price_segment'].unique():
                subset = cat_data[cat_data['price_segment'] == segment].copy()
                
                if len(subset) == 0: continue # Skip empty
                
                # Kalau data terlalu sedikit, tandai semua sebagai outlier
                if len(subset) < MIN_DATA_FOR_CLUSTERING:
                    subset['cluster_id'] = 0
                    subset['is_outlier'] = True
                    
                    min_fee = subset['average_yearly_fee'].min()
                    max_fee = subset['average_yearly_fee'].max()
                    
                    tag_fee = get_price_tag((min_fee + max_fee) / 2)
                    
                    # Safe Mode for Accreditation
                    if not subset['final_acc_score'].empty:
                        tag_acc = get_acc_dashboard_label(subset['final_acc_score'].mode()[0])
                    else:
                        tag_acc = "Belum Terakreditasi"
                    
                    subset['cluster_label'] = f"{prov} | {cat} | {tag_fee} | {tag_acc}"
                    results.append(subset)
                    continue
                
                scaler_fee = RobustScaler() # pakai robust scaler biar tahan outlier
                scaler_acc = MinMaxScaler() # akreditasinya kan 0-5, jadi minmax aja
                
                fee_reshaped = subset['average_yearly_fee'].values.reshape(-1, 1)
                fee_scaled = scaler_fee.fit_transform(fee_reshaped)
                acc_scaled = scaler_acc.fit_transform(subset[['final_acc_score']]) * AKRED_WEIGHT
                
                X = np.hstack([fee_scaled, acc_scaled])
                
                best_score = -1
                best_labels = None
                
                max_k = min(6, len(subset) // 2)
                if max_k < 2: max_k = 2
                
                for k in range(2, max_k + 1):
                    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
                    labels = kmeans.fit_predict(X)
                    try: score = silhouette_score(X, labels)
                    except: score = 0
                    
                    if score > best_score:
                        best_score = score
                        best_k = k
                        best_labels = labels
                
                if best_labels is None: best_labels = np.zeros(len(subset))
                
                subset['cluster_id'] = best_labels
                subset['is_outlier'] = False
                
                # --- LABELING ---
                for cid in np.unique(best_labels):
                    # SAFETY CHECK UNTUK NAN
                    if pd.isna(cid): continue
                    
                    mask = subset['cluster_id'] == cid
                    items = subset[mask]
                    
                    # SAFETY CHECK UNTUK ITEMS KOSONG
                    if len(items) == 0: continue
                    
                    min_fee = items['average_yearly_fee'].min()
                    max_fee = items['average_yearly_fee'].max()
                    avg_fee = (min_fee + max_fee) / 2
                    
                    tag_fee = get_price_tag(avg_fee)
                    
                    # Koreksi Mutlak Harga: ini untuk kasus outlier. Kalau misal cluster harga menengah tapi max fee nya di bawah 10jt, berarti harusnya masuk kategori lebih murah
                    if tag_fee == "Menengah (10-20jt)" and max_fee < 10000000: tag_fee = "Terjangkau (5-10jt)"
                    if tag_fee == "Mahal (20-50jt)" and max_fee < 20000000: tag_fee = "Menengah (10-20jt)"
                    
                    if not items['final_acc_score'].empty:
                        mode_score = items['final_acc_score'].mode()[0]
                        tag_acc = get_acc_dashboard_label(mode_score)
                    else:
                        tag_acc = "Belum Terakreditasi"
                    
                    is_tiny = len(items) < 3
                    outlier_prefix = "[OUTLIER] " if is_tiny else ""
                    
                    # Label: Provinsi | Kategori Prodi | Tag Harga | Tag Akreditasi
                    final_label = f"{outlier_prefix}{prov} | {cat} | {tag_fee} | {tag_acc}"
                    
                    subset.loc[mask, 'cluster_label'] = final_label
                    subset.loc[mask, 'is_outlier'] = is_tiny
                
                results.append(subset)
    return pd.concat(results) if results else pd.DataFrame()

df_final = optimize_clustering(df_merged)

# Sorting
df_final = df_final.sort_values(
    by=['is_outlier', 'province', 'broad_category', 'average_yearly_fee'],
    ascending=[True, True, True, False]
)

file_out = os.path.join(OUTPUT_PATH, 'final_clustering_result.csv')

#filter hanya is outlier false
df_final = df_final[df_final['is_outlier'] == False]
cols = [
    'institution_code', 'institution_name', 'province', 'prodi_normalized',
    'campus_accreditation', 'average_yearly_fee', 'contact',
    'cluster_label', 'link'
]

df_final = df_final[cols]
df_final['campus_accreditation'] = df_final['campus_accreditation'].replace(['B', 'BAIK'], 'B (BAIK)')
df_final.to_csv(file_out, index=False)

print("\nHead Data Hasil Clustering:")
print(df_final[['prodi_normalized', 'cluster_label']].head(5))
print(df_final.columns)