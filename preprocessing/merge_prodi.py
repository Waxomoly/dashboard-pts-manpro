import pandas as pd
import os
import re
from googletrans import Translator, constants
from tqdm import tqdm
translator = Translator()

# INI KATA-KATA UMUM YANG INGIN DIHAPUS DARI NAMA PRODI
WORDS_TO_REMOVE = [
    'TEKNIK', 'ILMU', 'DAN', 'PENDIDIKAN', 'PJJ'
]

PRODI_MAPPING = {
    'TEKNOLOGI HASIL PERTANIAN': 'PERTANIAN',
    'GURU SD': 'GURU SEKOLAH DASAR',
    'DOKTER': 'KEDOKTERAN',
    'SISTEM TEKNOLOGI INFORMASI': 'SISTEM INFORMASI',
    'BAHASA ARAB': 'SASTRA ARAB',
    'HUKUM KELUARGA': 'HUKUM',
    'HUKUM BISNIS': 'HUKUM',
    'HUKUM KELUARGA ISLAM': 'HUKUM',
    'MANAJEMEN SUMBER DAYA PERAIRAN': 'BUDIDAYA PERAIRAN',
    'JASMANI KESEHATAN REKREASI': 'JASMANI',
    'PEMANFAATAN SUMBER DAYA PERIKANAN': 'PERIKANAN',
    'TEKNOLOGI INDUSTRI PERTANIAN': 'PERTANIAN',
    'NERS' : 'KEPERAWATAN',
    'AGROBISNIS PERIKANAN': 'PERIKANAN',
    'BAHASA KEBUDAYAAN INGGRIS': 'SASTRA INGGRIS',
    'KEPENDIDIKAN SEKOLAH DASAR PRASEKOLAH': 'GURU SEKOLAH DASAR',
    'FILM TELEVISI':'FILM',
    'BIDAN': 'KEBIDANAN',
    'BAHASA MANDARIN': 'SASTRA CINA',
    'MANAJEMEN PEMASARAN': 'MARKETING',
    'SOSIAL EKONOMI PERIKANAN':'PERIKANAN',
    'OLAH RAGA': 'OLAHRAGA',
    'MANAJEMEN BISNIS': 'BISNIS',
    'MANAJEMEN PERHOTELAN': 'PERHOTELAN',
    'INFORMATIKA KOMPUTER': 'INFORMATIKA',
    'FARMASI KLINIK KOMUNITAS':'FARMASI',
    'SAINS AKTUARIA': 'AKTUARIA',
    'FARMASI KLINIS KOMUNITAS': 'FARMASI',
    'MANAJEMEN PERUSAHAAN': 'BISNIS',
    'KOMUNIKASI': 'ILMU KOMUNIKASI',
    'FILSAFAT KEILAHIAN': 'FILSAFAT',
    'FARMASI KLINIS': 'FARMASI',
    'BAHASA SASTRA DAERAH': 'SASTRA DAERAH',
    'KESEHATAN KESELAMATAN KERJA': 'KESELAMATAN KESEHATAN KERJA',
    'PEMANFAATAN SUMBERDAYA PERIKANAN': 'PERIKANAN',
    'SAINS BIOMEDIS': 'BIOMEDIS',
    'TEKNOLOGI PANGAN HASIL PERTANIAN': 'TEKNOLOGI PANGAN',
    'ANALIS KESEHATAN': 'KESEHATAN MASYARAKAT',
    'INFORMATIKA KOMPUTER': 'INFORMATIKA',
    'OLAHRAGA KESEHATAN': 'OLAHRAGA',
    'SEKRETARIS':'SEKRETARIAT',
    'MANAJEMEN PERHOTELAN PARIWISATA': 'PERHOTELAN',
    'MESIN OTOMOTIF': 'OTOMOTIF',
    'ARSITEKTUR LANSKAP': 'ARSITEKTUR',
    'TARI': 'SENI TARI',
    'MANAJEMEN LOGISTIK': 'LOGISTIK',
    'BAHASA KEBUDAYAAN JEPANG': 'SASTRA JEPANG',
    'SENI RUPA MURNI': 'SENI RUPA',
    'BIOLOGI TERAPAN': 'BIOLOGI',
    'ROBOTIKA KECERDASAN BUATAN': 'ROBOTIKA',
    'SAINS KOMUNIKASI': 'ILMU KOMUNIKASI',
    'SENI DRAMA TARI MUSIK': 'SENI TARI',
    'BISNIS JASA': 'BISNIS',
    'BUDIDAYA PERIKANAN': 'PERIKANAN',
    'TEKNOLOGI INFORMATIKA KOMPUTER': 'INFORMATIKA',
    'BAHASA SASTRA INGGRIS': 'SASTRA INGGRIS',
    'PRODUKSI PROSES MANUFAKTUR': 'MANUFAKTUR',
    'ELEKTRONIKA': 'ELEKTRO',
    'SAINS PERTANIAN': 'PERTANIAN',
    'MANAJEMEN HOTEL': 'PERHOTELAN',
    'PENCIPTAAN BISNIS': 'BISNIS',
    'PENGELOLAAN SUMBER DAYA PERAIRAN': 'BUDIDAYA PERAIRAN',
    'SISTEM INFORMASI BISNIS': 'SISTEM INFORMASI',
    'KAJIAN FILM': 'FILM',
    'KOMUNIKASI DIGITAL': 'ILMU KOMUNIKASI',
    'KOMUNIKASI STRATEGIS': 'ILMU KOMUNIKASI',
    'PEMANFAATAN SUMBERDAYA PERAIRAN': 'BUDIDAYA PERAIRAN',
    'TEKNOLOGI KOMPUTER': 'INFORMATIKA',
    'PERIKANAN TANGKAP': 'PERIKANAN',
    'TEKNOLOGI INFORMASI KOMUNIKASI': 'ILMU KOMUNIKASI',
    'TELEVISI FILM': 'FILM',
    'BAHASA SASTRA ARAB': 'SASTRA ARAB',
    'SAINS KELAUTAN': 'KELAUTAN',
    'BAHASA MANDARIN KEBUDAYAAN TIONGKOK': 'SASTRA CINA',
    'HASIL PERTANIAN': 'PERTANIAN',
    'MANAJEMEN SUMBERDAYA PERAIRAN' : 'BUDIDAYA PERAIRAN',
    'BAHASA JERMAN': 'SASTRA JERMAN',
    'REKAYASA PERTANIAN BIOSISTEM': 'PERTANIAN',
    'KOMUNIKASI PEMASARAN': 'PEMASARAN',
    'TEOLOGI KRISTEN PROTESTAN': 'TEOLOGI',
    'BAHASA KEBUDAYAAN ARAB': 'SASTRA ARAB',
    'MANAJEMEN HUTAN': 'KEHUTANAN',
    'DOKTER HEWAN': 'KEDOKTERAN HEWAN',
    'MANAJEMEN LOGISTIK MATERIAL': 'LOGISTIK',
    'MANAJEMEN TRANSPORTASI LAUT': 'KELAUTAN',
    'MANAJEMEN HOTEL PARIWISATA': 'PERHOTELAN',
    'IKLAN KOMUNIKASI MEDIA DIGITAL': 'ILMU KOMUNIKASI',
    'KOMUNIKASI BISNIS PENGUSAHA': 'ILMU KOMUNIKASI',
    'KOMUNIKASI MASSA': 'ILMU KOMUNIKASI',
    'KOMUNIKASI PERHOTELAN': 'ILMU KOMUNIKASI',
    'KOMUNIKASI SENI PERFORMA': 'ILMU KOMUNIKASI',
    'LOGISTIK E-PERDAGANGAN': 'LOGISTIK',
    'BUDIDAYA PERKEBUNAN KELAPA SAWIT': 'PERKEBUNAN',
    'TEKNOLOGI PRODUKSI TANAMAN PERKEBUNAN' : 'PERKEBUNAN',
    'TEKNOLOGI REKAYASA PERANGKAT LUNAK': 'REKAYASA PERANGKAT LUNAK',
    'PENGELOLAAN PERHOTELAN': 'PERHOTELAN',
    'BUDIDAYA TANAMAN PERKEBUNAN': 'PERKEBUNAN',
    'KEGURUAN': 'GURU',
    'ANALITIK BISNIS': 'BISNIS',
    'APLIKASI TEKNOLOGI GAME': 'TEKNOLOGI GAME',
    'APLIKASI TEKNOLOGI SELULER': 'INFORMATIKA',
    'BISNIS FASHION': 'FASHION',
    'DIGITAL MEDIA DESAIN': 'MEDIA DIGITAL',
    'FILSAFAT AGAMA KRISTEN': 'FILSAFAT',
    'MANAJEMEN KOMUNIKASI': 'ILMU KOMUNIKASI',
    'MANAJEMEN PERDAGANGAN': 'BISNIS',
    'IMU KESEHATAN MASYARAKAT': 'KESEHATAN MASYARAKAT',
    'MULTIMEDIA JARINGAN': 'MULTIMEDIA',
    'REKAYASA TRANSPORTASI LAUT': 'KELAUTAN',
    'ANALISIS FARMASI MAKANAN': 'FARMASI',
    'GIGI': 'KEDOKTERAN GIGI',
    'MANAJEMEN KEPELABUHAN PELAYARAN': 'PELAYARAN',
    'BAHASA JAWA': 'SASTRA JAWA',
    'SENI KARAWITAN': 'SENI',
    'KOMUNIKASI KREATIF': 'ILMU KOMUNIKASI',
    'MANAJEMEN HOTEL BISNIS': 'PERHOTELAN',
    'MANAJEMEN SISTEM INFORMASI': 'SISTEM INFORMASI',
    'PEMASARAN BISNIS GLOBAL': 'PEMASARAN',
    'REKAYASA MESIN': 'MESIN',
    'BAHASA SASTRA INDONESIA': 'SASTRA INDONESIA',
    'BAHASA SASTRA INGGRIS': 'SASTRA INGGRIS',
    'AKUNTANSI DATA ANALISIS': 'AKUNTANSI',
    'BISNIS MENEJEMEN': 'BISnis',
    'KIMIA FARMASI': 'FARMASI',
    'TEKNOLOGI MANUFAKTUR': 'MANUFAKTUR',
    'PEMERINTAHAN POLITIK': 'PEMERINTAHAN',
    'KEPERAWATAN UMUM': 'KEPERAWATAN',
    'BAHASA KEBUDAYAAN TIONGKOK': 'SASTRA CINA',
    'BIO INFORMATIKA': 'BIOINFORMATIKA',
    'BIO MEDIS REKAYASA HAYATI': 'BIOMEDIS',
    'BIO TEKNOLOGI': 'BIOTEKNOLOGI',
    'KEPERAWATAN ANESTESIOLOGI': 'KEPERAWATAN',
    'PELATIHAN AWAK KABIN AIRLINES': 'PENERBANGAN',
    'STRATEGI KOMUNIKASI BISNIS FASHION': 'FASHION',
    'SASTRA BAHASA INGGRIS': 'SASTRA INGGRIS',
    'SISTEM INFORMASI MANAJEMEN': 'SISTEM INFORMASI',
    'ANALITIK TI DATA BESAR': 'DATA SAINS',
    'INFORMATIKA KAMPUS KOTA PONTIANAK': 'INFORMATIKA',
    'INFORMATIKA KAMPUS KOTA SUKABUMI': 'INFORMATIKA',
    'SISTEM INFORMASI KAMPUS KOTA PONTIANAK': 'SISTEM INFORMASI',
    'SISTEM INFORMASI KAMPUS KOTA SUKABUMI': 'SISTEM INFORMASI',
    'PERANGKAT LUNAK': 'REKAYASA PERANGKAT LUNAK',
    'DATA': 'DATA SAINS',
    'PENGELOLAAN SUMBERDAYA PERAIRAN': 'BUDIDAYA PERAIRAN',
    'REKAYASA KEHUTANAN': 'KEHUTANAN',
    'TEKNOLOGI PERTANIAN': 'PERTANIAN',
    'AKUNTANSI SOSIAL LINGKUNGAN': 'AKUNTANSI',
    'BIOMEDIK / BIOMEDIS': 'BIOMEDIS',
    'PEREKAM INFORMASI KESEHATAN': 'REKAM MEDIS',
    'REKAM MEDIK INFORMASI KESEHATAN': 'REKAM MEDIS',
    'REKAM MEDIS': 'REKAM MEDIS',
    'MANAJEMEN PARIWISATA': 'PARIWISATA',
    'PRODUKSI FILM TELEVISI': 'FILM',
    'TEKNOLOGI PANGAN GIZI': 'TEKNOLOGI PANGAN',
    'ELEKTRO K JEMBRANA': 'ELEKTRO',
    'BAHASA SASTRA INDONESIA DAERAH': 'SASTRA INDONESIA',
    'REKAM MEDIK INFORMASI KESEHATAN': 'REKAM MEDIS',
    'PERIKANAN KELAUTAN': 'PERIKANAN',
    'SASTRA TIONGKOK': 'SASTRA CINA',
    'DESAIN MANAJEMEN PRODUK': 'DESAIN PRODUK',
    'DESAIN FASHION PRODUK LIFESTYLE': 'FASHION',
    'MANAJEMEN PELABUHAN LOGISTIK MARITIM': 'LOGISTIK',
    'TEKNOLOGI REKAYASA OPERASI KAPAL': 'PERKAPALAN',
    'TEKNOLOGI REKAYASA PERMESINAN KAPAL': 'PERKAPALAN',
    'ADMINISTRASI PERPAJAKAN': 'PERPAJAKAN',
    'DESAIN ROBOTIKA INDUSTRI': 'ROBOTIKA',
    'MANAJEMEN PENGELOLAAN SUMBERDAYA PERAIRAN': 'BUDIDAYA PERAIRAN',
    'IF - P. K. SISTEM INFORMASI BISNIS': 'SISTEM INFORMASI',
    'MANEJEMEN': 'MANAJEMEN',
    'TERNAK': 'PETERNAKAN',
    'REKAYASA TEKSTIL': 'TEKSTIL',
    'INVESTASI KEUANGAN PERANTARA KEUANGAN': 'KEUANGAN',
    'P K MANUFAKTUR': 'MANUFAKTUR',
    'INFORMATIKA K JEMBRANA': 'INFORMATIKA',
    'GURU SEKOLAH DASAR K JEMBRANA': 'GURU SEKOLAH DASAR',
    'BAHASA INGGRIS K JEMBRANA': 'SASTRA INGGRIS',
    'BAHASA SASTRA INDONESIA': 'SASTRA INDONESIA',
    'BAHASA INDONESIA': 'SASTRA INDONESIA',
    'KEPELATIHAN OLAHRAGA': 'OLAHRAGA',
    'KEOLAHRAGAAN': 'OLAHRAGA',
    'BAHASA JEPANG': 'SASTRA JEPANG',
    'MUSIK': 'SENI MUSIK',
    'REKAYASA SISTEM KOMPUTER': 'REKAYASA PERANGKAT LUNAK',
    'HOSPITALITY PARIWISATA': 'PERHOTELAN PARIWISATA',
    'USAHA PERJALANAN WISATA': 'PARIWISATA',
    'HOSPITALITI PARIWISATA': 'PERHOTELAN PARIWISATA',
    'BAHASA SASTRA ACEH': 'SASTRA ACEH',
    'KRIYA': 'SENI KRIYA',
    'PARIWISATA BUDAYA KEAGAMAAN': 'PARIWISATA',
    'MANAJEMEN HOTEL PARIWISATA': 'PERHOTELAN PARIWISATA',
    'AKUNTANSI KEUANGAN PASAR MODAL': 'AKUNTANSI',
    'AKUNTANSI MANAJEMEN DALAM EKONOMI DIGITAL': 'AKUNTANSI',
    'AKUNTANSI PROFESIONAL PA': 'AKUNTANSI',
    'JIKA PK MULTIMEDIA': 'MULTIMEDIA',
    'PERHOTELAN SENI KULINER': 'PERHOTELAN',
    'DESTINASI PARIWISATA': 'PARIWISATA',
    'MULTIMEDIA KREATIF DIGITAL': 'MULTIMEDIA',
    'PANGAN NUTRISI': 'TEKNOLOGI PANGAN',
    'AIR TRANSPORT FUNDAMENTAL': 'PENERBANGAN',
    'MEKANISASI PERTANIAN': 'PERTANIAN',
    'AKUNTANSI PAJAK': 'PERPAJAKAN',
    'CINA': 'SASTRA CINA',
    'DESAIN GAYA INTERIOR': 'DESAIN INTERIOR',
    'DESAIN PRODUK INTERIOR': 'DESAIN INTERIOR',
    'GURU DASAR': 'GURU SEKOLAH DASAR',
    'INDUSTRI OTOMOTIF': 'OTOMOTIF',
    'PARIWISATA KREATIF': 'PARIWISATA',
    'PENGETAHUAN ANALITIK DATA': 'DATA SAINS',
    'PERHOTELAN PARIWISATA': 'PERHOTELAN PARIWISATA',
    'MANAJEMEN TRANSPORTASI': 'TRANSPORTASI',
    'AGRIBISNIS K MATARAM': 'AGRIBISNIS',
    'MANAJEMEN K MATARAM': 'MANAJEMEN',
    'DESTINASI WISATA': 'PARIWISATA',
    'PERJALANAN WISATA': 'PARIWISATA',
    'DOKTER GIGI': 'KEDOKTERAN GIGI',
    'KONSTRUKSI PERKAPALAN': 'PERKAPALAN',
    'OTOMOTIF ALAT BERAT': 'OTOMOTIF',
    'ELEKTRO TEKNOLOGI ELEKTRONIKA': 'ELEKTRO',
    'BAHASA INDONESIA SASTRA INDONESIA': 'SASTRA INDONESIA',
    'PENYULUH PERTANIAN': 'PERTANIAN',
    'MANAJEMEN K SINTANG': 'MANAJEMEN',
    'REKAYASA ELEKTRO': 'ELEKTRO',
    'PENGOLAHAN HASIL PERIKANAN': 'PERIKANAN',
    'TEKNOLOGI PENGOLAHAN HASIL PERIKANAN': 'PERIKANAN',
    'STUDI PEMERINTAHAN': 'PEMERINTAHAN',
    'TEKNOLOGI AGROINDUSTRI': 'AGROINDUSTRI',
    'PERTANIAN BIOSISTEM': 'PERTANIAN',
    'BUDIDAYA PERTANIAN': 'PERTANIAN',
    'KEGURUAN OLAHRAGA': 'OLAHRAGA',
    'AGROTEKNOLOGI AGROEKOTEKNOLOGI': 'AGROTEKNOLOGI',
    'ELEKTRO KAMPUS KOTA SERANG': 'ELEKTRO',
    'MESIN KAMPUS KOTA SERANG': 'MESIN',
    'AGROTEKNOLOGI PERTANIAN': 'AGROTEKNOLOGI',
    'PANGAN': 'TEKNOLOGI PANGAN',
    'TEKNOLOGI BISNIS PANGAN': 'TEKNOLOGI PANGAN',
    'JASMANI K JEMBRANA': 'JASMANI',
    'TEKNOLOGI INDUSTRI PANGAN': 'TEKNOLOGI PANGAN',
    'KRIYA SENI': 'SENI KRIYA',
    'BAHASA SASTRA JAWA': 'SASTRA JAWA',
    'BAHASA KOREA': 'SASTRA KOREA',
    'IPA': 'PENGETAHUAN ALAM',
    'MIPA': 'PENGETAHUAN ALAM',
    'ANAK USIA DINI': 'GURU ANAK USIA DINI',
    'MANAJEMEN RETAIL': 'MANAJEMEN RITEL',
    'BIO KEWIRAUSAHAAN': 'KEWIRAUSAHAAN',
    'DESAIN PENCIPTAAN FASHION': 'FASHION',
    'KEWIRASWASTAAN': 'KEWIRAUSAHAAN',
    'PEMASARAN WIRAUSAHA': 'KEWIRAUSAHAAN',
    'REKAYASA LOGISTIK': 'LOGISTIK',
    'DESAIN FASHION TEKSTIL': 'FASHION',
    'LOGISTIK E PERDAGANGAN': 'LOGISTIK',
    'KEWIRAUSAHAAAN': 'KEWIRAUSAHAAN',
    'INDUSTRI KAMPUS KOTA SEMARANG': 'PERENCANAAN WILAYAH KOTA',
    'DESAAIN PRODUK': 'DESAIN PRODUK',
    'KESEKRETARIATAN': 'SEKRETARIAT',
    'AKUNTANSI SEKTOR BISNIS': 'AKUNTANSI',
    'PAJAK': 'PERPAJAKAN',
    'TEKSTIL': 'FASHION',
    'ARSITEKTUR DESAIN KOMUNIKASI VISUAL': 'DESAIN KOMUNIKASI VISUAL',
    'HUKUM KESEHATAN': 'HUKUM',
    'LINGKUNGAN PERKOTAAN': 'PERENCANAAN WILAYAH KOTA',
    'MANAJEMEN BISNIS DIGITAL': 'MANAJEMEN',
    'PERENCANAAN KOTA REAL ESTATE': 'PERENCANAAN WILAYAH KOTA',
    'ARSITEKTUR PERUMAHAN BERKELANJUTAN REAL ESTATE': 'ARSITEK',
    'DESAIN TEKSTIL FASHION': 'FASHION',
    'GURU ANAK DINI': 'GURU ANAK USIA DINI',
    'LOGISTIK GLOBAL RANTAI PASOKAN': 'LOGISTIK',
    'TEKNOLOGI HASIL PETERNAKAN': 'PETERNAKAN',
    'PEMERINTAH': 'PEMERINTAHAN',
    'PRODUKSI TERNAK': 'PETERNAKAN',
    'KOMPUTER MATEMATIKA': 'MATEMATIKA',
    'KOMPUTER STATISTIK': 'STATISTIK',
    'ENERGI WIRAUSAHA': 'KEWIRAUSAHAAN',
    'PERANGKAT LUNAK PERUSAHAAN': 'PERANGKAT PERANGKAT LUNAK',
    'PG ANAK USIA DINI': 'GURU ANAK USIA DINI',
    'INDUSTRI PERTANIAN': 'PERTANIAN',
    'DESAIN FASHION PRODUK LIFESTYLE': 'FASHION',
    'DESAIN MANAJEMEN PRODUK': 'MANAJEMEN',
    'IF P K SISTEM INFORMASI BISNIS': 'SISTEM INFORMASI',
    'KEWIRAUSAHAAN SUMBER DAYA MANUSIA': 'KEWIRAUSAHAAN',
    'MANAJEMEN BISNIS PARIWISATA': 'MANAJEMEN',
    'ARSITEKTUR LANSEKAP': 'ARSITEKUR',
    'MANAJEMEN JASA': 'MANAJEMEN',
    'MANAJEMEN AGRIBISNIS': 'MANAJEMEN',
    'KEAMANAN SIBER FORENSIK DIGITAL': 'KEAMANAN SIBER',
    'KOMPUTASI TEKNOLOGI GAME': 'TEKNOLOGI GAME',
    'MANAJEMEN BISNIS REKREASI': 'MANAJEMEN',
    'BUDIDAYA PERKEBUNAN': 'PERKEBUNAN',
    'TEKNOLOGI CERDAS': 'KECERDASAN BUATAN',
    'REKAYASA GEOLOGI': 'GEOLOGI',
    'PEMERINTAHAN POLITIK': 'POLITIK',
    
}

# Prioritas Sumber: Quipper (0) > Rencanamu (1) > BAN-PT (2)
SOURCE_PRIORITY = {'quipper': 0, 'rencanamu': 1, 'banpt': 2}

def normalize_prodi_name(series):
    # 1. Pembersihan dasar
    cleaned = (series.astype(str).str.split(',')
                    .str[0]
                    .str.replace(r'\s*\([^)]*\)', '', regex=True)
                    .str.replace(r'[^\w\s]', ' ', regex=True)
                    .str.upper().str.strip())

    # 2. Hapus kata-kata "noise" - INI BUAT HAPUS YG GELARAN, JURUSAN, PRODI, DLL
    generic_words = [r'\bS1\b', r'\bSARJANA\b', r'\bPROGRAM STUDI\b', r'\bPRODI\b', r'\bJURUSAN\b', r'\bPROGRAM\b']
    for word in generic_words:
        cleaned = cleaned.str.replace(word, '', regex=True)

    # 3. Hapus kata-kata umum dari daftar WORDS_TO_REMOVE
    for word in WORDS_TO_REMOVE:
        cleaned = cleaned.str.replace(r'\b' + word + r'\b', '', regex=True)
        
    # 4. Hapus spasi ganda dan rapikan
    cleaned = cleaned.str.replace(r'\s+', ' ', regex=True).str.strip()
    
    cleaned = cleaned.replace(PRODI_MAPPING)

    cleaned = cleaned.str.replace(r'\s+', ' ', regex=True).str.strip()
    return cleaned

def aggregate_by_priority(series, priority_map):
    temp_df = pd.DataFrame({
        'value': series.dropna(),
        'source': series.dropna().index.get_level_values('source')
    })
    
    if temp_df.empty:
        return '-'
    
    temp_df['priority'] = temp_df['source'].map(priority_map).fillna(99)
    temp_df.sort_values(by='priority', inplace=True)
    
    # Ambil nilai pertama yang valid (bukan '-' dan bukan kosong)
    valid_values = temp_df.loc[temp_df['value'].astype(str).str.strip() != '-', 'value']
    if not valid_values.empty:
        return valid_values.iloc[0]
    
    # Kalau semua '-', baru fallback ke '-'
    return '-'

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    BASE_PATH = os.path.join(parent_dir, "csv_result") + os.sep
except NameError:
    BASE_PATH = "csv_result/"

try:
    df_prodi_rencanamu = pd.read_csv(BASE_PATH + "rencanamu_prodi_preprocessed.csv")
    df_prodi_quipper = pd.read_csv(BASE_PATH + "quipper_prodi_clean.csv")
    df_prodi_banpt = pd.read_csv(BASE_PATH + "banpt_prodi_clean.csv")
    df_inst_rencanamu = pd.read_csv(BASE_PATH + "rencanamu_institutions_preprocessed.csv")
    df_inst_quipper = pd.read_csv(BASE_PATH + "quipper_institution_clean.csv")
    df_inst_banpt = pd.read_csv(BASE_PATH + "banpt_institution_clean.csv")
    df_normalisasi_csv = pd.read_csv(BASE_PATH + "normalisasi_prodi.csv")

except FileNotFoundError as e:
    print(f"Error: File tidak ditemukan: {e.filename}")
    exit()

# translate all prodi to indonesian
unique_prodi_series = pd.concat([
    df_prodi_quipper['prodi'], 
    df_prodi_rencanamu['prodi'], 
    df_prodi_banpt['prodi_name']
]).dropna().astype(str).drop_duplicates()

print(f"Total unique prodi names to translate: {len(unique_prodi_series)}")

tqdm.pandas(desc="Translating Program Names")
detected_langs_series = unique_prodi_series.progress_apply(lambda x: translator.translate(x, dest='id').text)
print("Translation completed.")

debug_df = pd.DataFrame({
    'prodi_name': unique_prodi_series,  # Use the original list of unique names
    'indonesian_name': detected_langs_series.values, # Use the detection results
})
print(debug_df)

mapping_series = debug_df.set_index('prodi_name')['indonesian_name']

# df_prodi_quipper['prodi_slug'] = df_prodi_quipper['prodi'].map(mapping_series)
print("Mapping prodi names to Indonesian...")
print("Mapping quipper prodi names...")
df_prodi_quipper['prodi'] = df_prodi_quipper['prodi'].map(mapping_series)
print("Mapping rencanamu prodi names...")
df_prodi_rencanamu['prodi'] = df_prodi_rencanamu['prodi'].map(mapping_series)
print("Mapping banpt prodi names...")
df_prodi_banpt['prodi_name'] = df_prodi_banpt['prodi_name'].map(mapping_series)

# df_prodi_quipper['prodi'] = df_prodi_quipper['prodi'].apply(lambda x: translator.translate(x, dest='id').text)
# df_prodi_rencanamu['prodi'] = df_prodi_rencanamu['prodi'].apply(lambda x: translator.translate(x, dest='id').text)
# df_prodi_banpt['prodi'] = df_prodi_banpt['prodi'].apply(lambda x: translator.translate(x, dest='id').text)

map_rencanamu_code_to_name = pd.Series(df_inst_rencanamu.institution_name.values, index=df_inst_rencanamu.institution_code).to_dict()
map_quipper_code_to_name = pd.Series(df_inst_quipper.institution_name.values, index=df_inst_quipper.institution_code).to_dict()
map_banpt_code_to_name = pd.Series(df_inst_banpt.institution_name.values, index=df_inst_banpt.institution_code).to_dict()

df_normalisasi_csv['normalisasi'] = df_normalisasi_csv['normalisasi'].astype(str)
df_normalisasi_csv['normalisasi'] = df_normalisasi_csv['normalisasi'].replace('nan', '', regex=False).fillna('')
df_norm_filtered = df_normalisasi_csv[df_normalisasi_csv['normalisasi'] != ''].copy()
prodi_map_from_csv = pd.Series(
    df_norm_filtered['normalisasi'].values, 
    index=df_norm_filtered['prodi']
).to_dict()


required_cols = ['institution_name', 'prodi_name', 'edu_level', 'accreditation', 'faculty', 'source', 
                 'quipper_code', 'rencanamu_code', 'banpt_code', 'pddikti_code']

# A. Proses RENCANAMU
df_prodi_rencanamu['institution_name'] = df_prodi_rencanamu['institution_code'].map(map_rencanamu_code_to_name)
df_prodi_rencanamu.rename(columns={'prodi': 'prodi_name', 'akreditasi_prodi': 'accreditation'}, inplace=True)
df_prodi_rencanamu['faculty'] = df_prodi_rencanamu['faculty'].replace('UNKNOWN', '-').fillna('-')
df_prodi_rencanamu['edu_level'] = 'S1'
df_prodi_rencanamu['source'] = 'rencanamu'
df_prodi_rencanamu['rencanamu_code'] = df_prodi_rencanamu['institution_code'] 
processed_rencanamu = df_prodi_rencanamu.drop(columns=['institution_code'], errors='ignore')

# B. Proses QUIPPER
df_prodi_quipper['institution_name'] = df_prodi_quipper['institution_code'].map(map_quipper_code_to_name)
df_prodi_quipper.rename(columns={'prodi': 'prodi_name'}, inplace=True)
df_prodi_quipper['accreditation'] = '-' 
df_prodi_quipper['edu_level'] = 'S1'
df_prodi_quipper['source'] = 'quipper'
df_prodi_quipper['faculty'] = df_prodi_quipper['faculty'].fillna('-')
df_prodi_quipper['quipper_code'] = df_prodi_quipper['institution_code'] 
processed_quipper = df_prodi_quipper.drop(columns=['institution_code'], errors='ignore')

# C. Proses BAN-PT
# df_prodi_banpt['institution_name'] = df_prodi_banpt['institution_code'].map(map_banpt_code_to_name)
# df_prodi_banpt.rename(columns={'jenjang': 'edu_level', 'akreditasi_prodi': 'accreditation'}, inplace=True)
# df_prodi_banpt['faculty'] = '-'
# df_prodi_banpt['source'] = 'banpt'
# df_prodi_banpt['banpt_code'] = df_prodi_banpt['institution_code']
# processed_banpt = df_prodi_banpt.drop(columns=['institution_code'], errors='ignore')

df_prodi_banpt['institution_name'] = df_prodi_banpt['institution_code'].map(map_banpt_code_to_name)
# cari baris yang gagal di map
failed_mask = df_prodi_banpt['institution_name'].isnull()
# untuk yang gagal, ambil nama dari kode placeholde
if failed_mask.any():
    print(f"Mengambil {failed_mask.sum()} nama institusi BAN-PT dari placeholder...")
    # Ambil nama dari placeholder dengan menghapus 'banpt-'
    placeholder_names = df_prodi_banpt.loc[failed_mask, 'institution_code'].str.replace('banpt-', '', n=1)
    df_prodi_banpt['institution_name'] = df_prodi_banpt['institution_name'].fillna(placeholder_names)

df_prodi_banpt.rename(columns={'jenjang': 'edu_level', 'akreditasi_prodi': 'accreditation'}, inplace=True)
df_prodi_banpt['faculty'] = '-'
df_prodi_banpt['source'] = 'banpt'
df_prodi_banpt['banpt_code'] = df_prodi_banpt['institution_code']
processed_banpt = df_prodi_banpt.drop(columns=['institution_code'], errors='ignore')


for df in [processed_rencanamu, processed_quipper, processed_banpt]:
    for col in ['quipper_code', 'rencanamu_code', 'banpt_code', 'pddikti_code']:
        if col not in df.columns:
            df[col] = '-'

df_combined = pd.concat([
    processed_rencanamu.reindex(columns=required_cols), 
    processed_quipper.reindex(columns=required_cols), 
    processed_banpt.reindex(columns=required_cols)
], ignore_index=True)

df_combined['prodi_name_normalized'] = normalize_prodi_name(df_combined['prodi_name'])



print("Menerapkan normalisasi prodi dari file CSV...")
df_combined['prodi_csv_norm'] = df_combined['prodi_name_normalized'].map(prodi_map_from_csv)
cond_delete = (df_combined['prodi_csv_norm'] == '-')
rows_to_delete = cond_delete.sum()
if rows_to_delete > 0:
    df_combined = df_combined[~cond_delete].copy()
cond_update = df_combined['prodi_csv_norm'].notna()
rows_to_update = cond_update.sum()
if rows_to_update > 0:
    df_combined.loc[cond_update, 'prodi_name_normalized'] = df_combined.loc[cond_update, 'prodi_csv_norm']
df_combined = df_combined.drop(columns=['prodi_csv_norm'])


# Hapus fakultas yang bukan S1
filter_pattern_faculty = r'\bVOKASI\b|\bMAGISTER\b|\bPROFESI\b|\bDIPLOMA\b|\bD3\b|\bPASCASARJANA\b|\bDOKTOR\b|\bAKADEMI\b'
is_invalid_faculty = df_combined['faculty'].str.contains(
    filter_pattern_faculty, 
    case=False,  
    na=False,   
    regex=True 
)
df_combined = df_combined[~is_invalid_faculty].copy()
print(f"Total baris setelah filter fakultas non-S1: {len(df_combined)}")


# Hapus prodi yang tidak sesuai
filter_pattern_prodi = (
    r'\bD1\b|\bD2\b|\bD3\b|\bD4\b|\bPROFESI\b|\bMAGISTER\b|\bVOKASI\b|\bDIPLOMA\b|\bPASCASARJANA\b|'
    r'\bVOKASIONAL\b|\bDOKTORAL\b|\bDOKTOR\b|\bLAINNYA\b|\bAL-AHWAL AL-SYAKHSIYAH\b|\bTANAH\b|'
    r'\bBANGUNAN\b|\bALQUR AN TAFSIR\b|\bGURU MI\b|\bAHWAL AL-SYAKHSHIYAH\b|\bKERAMAHAN\b|'
    r'\bSYARI\b|\bAL AHWAL AL SYAKHSIYYAH\b|\bHINDU\b|\bAGAMA KATOLIK\b|\bAGAMA KRISTEN\b|'
    r'\bKEAGAMAAN KATOLIK\b|\bAL-QUR\b|\bKESEJAHTERAAN KELUARGA\b|\bGURU ROUDHATUL ATHFAL\b|'
    r'\bKENOTARIATAN\b|\bKESELAMATAN\b|\bNON FORMAL\b|\bSYARIAH\b|\bBISNIS PERHOTELAN\b|'
    r'\bHUKUM ADAT\b|\bSOSIATRI\b|\bBIO-KEWIRAUSAHAAN\b|\bTADRIS MATEMATIKA\b|\bALQURAN TAFSIR\b|'
    r'\bISLAM\b|\bMANAJEMEN ZAKAT WAKAF\b|\bAHWAL AL SYAKHSHIYAH\b|'
    r'\bMUAMALAT\b|\bAL-QURAN TAFSIR\b|\bLAYANAN HOTEL TERAPUNG\b|\bDESAIN TEKNOLOGI INTERAKTIF\b|'
    r'\b62201\b|\bIPS\b|\bPENGATAHUAN SOSIAL\b|\bKEILAHIAN\b|\bSYAR\b|\bDESIGN PRODUK\b|'
    r'\bMANAJEMEN REKAYASA KONSTRUKSI\b|\bBISNIS JASA MAKANAN\b|\bGURU VOKASI\b|\bTAFSIR HADITS\b|'
    r'\bKEWIRAUSAHAAN BIO\b|\bMAKANAN\b|\bOBAT BIO\b|\bLNFORMATIKA\b|\bMANAJEMAN DAKWAH\b|'
    r'\bPENGINDRAAN JAUH SISTEM INFORMASI GEOGRAFIS\b|\bHADIS\b|\bAL QUR\b|'
    r'\bPERBANDINGAN MADZHAB\b|\bSTUDI AGAMA AGAMA\b|\bTEKNOLOGI BANK DARAH\b|'
    r'\bSURVEI PEMETAAN\b|\bDIKUNJUNGI\b|\bOSEANOGRAFI\b|\bOSEANOLOGI\b|\bFILSAFAT HINDU\b|'
    r'\bSENI RUPA ORNAMEN HINDU\b|\bSENI TARI KEAGAMAAN HINDU\b|\bMANAJEMEN HAJI UMRAH\b|'
    r'\bI\.B\.N\b|\bAKHWAL SYAKSIYAH\b|\bMIPA\b|\bPERTAMANAN\b|'
    r'\bJARINGAN BISNIS INTERNASIONAL/IBN\b|\bJARINGAN BISNIS PEMBERDAYAAN\b|'
    r'\bADMINISTRASI FISKAL\b|\bPERPUSTAKAAN SAINS INFORMASI\b|\bAGROEKNOLOGI\b|'
    r'\bJARINGAN TELEKOMUNIKASI DIGITAL\b|\bPENGEMBANGAN WILAYAH KOTA\b|'
    r'\bJIKA - PK MULTIMEDIA\b|\bPENYIARAN\b|\bKHUSUS\b|\bKESEHATAN AYURWEDA\b|\bDAKWAH\b|'
    r'\bDASAR PENGOPERASIAN BANDARA\b|\bKESADARAN KEAMANAN PENERBANGAN\b|'
    r'\bLAYANAN DASAR PENUMPANG\b|\bJOINT DEGREE KIMIA\b|\bAGAMA\b|\bKAJIAN KONFLIK PERDAMAIAN\b|'
    r'\bMUSIK GEREJA\b|\bPASTORAL KONSELING\b|\bPENGELOLAAN\b|\bMUAMALAH\b|\bBAHASA\b|'
    r'\bTADRIS BIOLOGI\b|\bQURAN TAFSIR\b|\bTASAWUF PSIKTERAPI\b|\bZAKAT WAKAF\b|\bNONFORMAL\b|'
    r'\bPGRA\b|\bMANAJEMEN KEUANGAN PERBANKAN SYARIAH\b|\bAKADEMI AWS\b|\bBIOPROSES\b|'
    r'\bMANAJEMEN SUPPLY CHAIN\b|\bKONSELING PASTORAL\b|\bNERS KEPERAWATAN\b|'
    r'\bGURU MADRASAN IBTIDAIYAH\b|\bFALAK\b|\bMANAJEMEN SYARIAH\b|\bSAINS PERKOPIAN\b|'
    r'\bTEKNOLOGI BATIK\b|\bMEREK\b|\bPERISTIWA\b|\bKEUANGAN PERBANKAN SYARIAH\b|\bKEPERCAYAAN TERHADAP TUHAN YANG MAHA ESA\b'
)

is_invalid_prodi = df_combined['prodi_name'].str.contains(
    filter_pattern_prodi, 
    case=False,  
    na=False,   
    regex=True 
)
df_combined = df_combined[~is_invalid_prodi].copy()
print(f"Total baris setelah filter jenjang S1: {len(df_combined)}")

agg_funcs = {
    'quipper_code': lambda x: x[x != '-'].iloc[0] if (x != '-').any() else '-',
    'rencanamu_code': lambda x: x[x != '-'].iloc[0] if (x != '-').any() else '-',
    'banpt_code': lambda x: x[x != '-'].iloc[0] if (x != '-').any() else '-',
    'pddikti_code': 'first', 
    'prodi_name': 'first',
    'accreditation': lambda x: aggregate_by_priority(x.str.upper().str.strip().replace('', '-'), SOURCE_PRIORITY),
    'faculty': lambda x: aggregate_by_priority(x, SOURCE_PRIORITY),
    'edu_level': 'first', 
}

df_final = (
    df_combined
    .set_index(['institution_name', 'prodi_name_normalized', 'source'])
    .groupby(level=['institution_name', 'prodi_name_normalized'])
    .agg(agg_funcs)
)

df_final = df_final.reset_index()
print(f"Total baris setelah agregasi: {len(df_final)}")

final_columns = [
    'quipper_code', 
    'rencanamu_code', 
    'banpt_code', 
    'pddikti_code', 
    'faculty',
    'prodi_name_normalized', 
    'edu_level', 
    'accreditation', 
]

df_final = df_final.reindex(columns=final_columns)
df_final.rename(columns={'prodi_name_normalized': 'prodi'}, inplace=True)
output_path = BASE_PATH + "merged_prodi.csv"
df_final.to_csv(output_path, index=False, encoding='utf-8-sig')