import pandas as pd
import re
import os
from datetime import datetime

class DataPreprocessor:
    def __init__(self, input_file):
        """Initialize preprocessor dengan file input"""
        self.input_file = input_file
        self.df = None
        self.log = []
        
        # Mapping provinsi ID ke nama
        self.provinsi_map = {
            1: "ACEH",
            2: "SUMATERA UTARA",
            3: "SUMATERA BARAT",
            4: "RIAU",
            5: "KEPULAUAN RIAU",
            6: "JAMBI",
            7: "BENGKULU",
            8: "SUMATERA SELATAN",
            9: "KEPULAUAN BANGKA BELITUNG",
            10: "LAMPUNG",
            11: "BANTEN",
            12: "JAWA BARAT",
            13: "DKI JAKARTA",
            14: "JAWA TENGAH",
            15: "JAWA TIMUR",
            16: "DI YOGYAKARTA",
            17: "BALI",
            18: "NUSA TENGGARA BARAT",
            19: "NUSA TENGGARA TIMUR",
            20: "KALIMANTAN BARAT",
            21: "KALIMANTAN SELATAN",
            22: "KALIMANTAN TENGAH",
            23: "KALIMANTAN TIMUR",
            24: "KALIMANTAN UTARA",
            25: "GORONTALO",
            26: "SULAWESI SELATAN",
            27: "SULAWESI TENGGARA",
            28: "SULAWESI TENGAH",
            29: "SULAWESI UTARA",
            30: "SULAWESI BARAT",
            31: "MALUKU",
            32: "MALUKU UTARA",
            33: "PAPUA",
            34: "PAPUA BARAT"
        }
        
    def log_info(self, message):
        """Catat informasi ke log"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        self.log.append(log_message)
        print(log_message)
    
    def load_data(self):
        """Load CSV file"""
        try:
            self.df = pd.read_csv(self.input_file, encoding='utf-8-sig')
            self.log_info(f"✓ Data loaded: {len(self.df)} rows, {len(self.df.columns)} columns")
            return True
        except Exception as e:
            self.log_info(f"✗ Error loading file: {e}")
            return False
    
    def step1_remove_unwanted_columns(self):
        """STEP 1: Hapus kolom yang tidak diperlukan"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 1: HAPUS KOLOM TIDAK DIPERLUKAN")
        self.log_info("="*60)
        
        columns_to_remove = [
            'link_kampus', 
            'status', 
            'kota', 
            'website',
            'ranking_webometric'
        ]
        existing_cols = [col for col in columns_to_remove if col in self.df.columns]
        
        if existing_cols:
            self.df = self.df.drop(columns=existing_cols)
            self.log_info(f"✓ Kolom dihapus: {', '.join(existing_cols)}")
        else:
            self.log_info("  Tidak ada kolom yang perlu dihapus")
    
    def step2_uppercase_all_text(self):
        """STEP 2: Uppercase semua text dan bersihkan akreditasi"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 2: UPPERCASE SEMUA TEXT & BERSIHKAN AKREDITASI")
        self.log_info("="*60)
        
        text_columns = [
            'nama_kampus', 'akreditasi_kampus', 'alamat', 'provinsi',
            'fakultas', 'prodi', 'akreditasi_prodi'
        ]
        
        for col in text_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(
                    lambda x: x.upper() if isinstance(x, str) and x != '-' else x
                )
        
        # Bersihkan format akreditasi (hapus prefix seperti i:, n:, dll)
        akreditasi_cols = ['akreditasi_kampus', 'akreditasi_prodi']
        for col in akreditasi_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(
                    lambda x: self._clean_akreditasi(x)
                )
        
        self.log_info(f"✓ Text columns di-uppercase")
        self.log_info(f"✓ Format akreditasi dibersihkan (hapus prefix)")
        self.log_info(f"✓ Akreditasi kosong/tidak terdefinisi diisi dengan '-'")
    
    def _clean_akreditasi(self, value):
        """Bersihkan format akreditasi"""
        if pd.isna(value) or value == '' or str(value).strip() == '':
            return '-'
        
        # Convert to string and uppercase
        val = str(value).upper().strip()
        
        # Hapus prefix seperti I:, N:, dll
        val = re.sub(r'^[A-Z]+:', '', val).strip()
        
        # Jika kosong setelah dibersihkan atau tidak valid, return '-'
        if val == '' or val.upper() not in ['A', 'B', 'C', 'UNGGUL', 'BAIK SEKALI', 'BAIK']:
            return '-'
        
        return val
    
    def step3_remove_brackets_and_content(self):
        """STEP 3: Hapus semua kurung dan isinya"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 3: HAPUS KURUNG DAN ISINYA")
        self.log_info("="*60)
        
        text_columns = ['nama_kampus', 'alamat', 'provinsi', 'fakultas', 'prodi']
        total_removed = 0
        
        for col in text_columns:
            if col in self.df.columns:
                before = self.df[col].copy()
                self.df[col] = self.df[col].apply(
                    lambda x: re.sub(r'\([^)]*\)|\[[^\]]*\]|\{[^}]*\}', '', str(x)) 
                    if isinstance(x, str) and x != '-' else x
                )
                changed = (before != self.df[col]).sum()
                if changed > 0:
                    total_removed += changed
                    self.log_info(f"  {col}: {changed} entries diubah")
        
        self.log_info(f"✓ Total entries dengan kurung dihapus: {total_removed}")
    
    def step4_trim_all_text(self):
        """STEP 4: TRIM semua text"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 4: TRIM SEMUA TEXT")
        self.log_info("="*60)
        
        for col in self.df.columns:
            if self.df[col].dtype == 'object':
                self.df[col] = self.df[col].apply(
                    lambda x: ' '.join(str(x).split()).strip() 
                    if isinstance(x, str) and x != '-' else x
                )
        
        self.log_info("✓ Semua text di-trim")
    
    def step5_restructure_biaya_columns(self):
        """STEP 5: Restructure kolom biaya - 6 kolom (Yearly & Semester dengan Average/Starting/Ending)"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 5: RESTRUCTURE KOLOM BIAYA (6 KOLOM BARU)")
        self.log_info("="*60)
        
        def to_numeric(val):
            if val == '-' or pd.isna(val) or val == '':
                return None
            try:
                return float(str(val).replace(',', '').replace('.', ''))
            except:
                return None
        
        # Hitung biaya baru
        yearly_average = []
        yearly_starting = []
        yearly_ending = []
        semester_average = []
        semester_starting = []
        semester_ending = []
        
        for idx, row in self.df.iterrows():
            sem_min = to_numeric(row.get('biaya_semester_min'))
            sem_max = to_numeric(row.get('biaya_semester_max'))
            
            if sem_min and sem_max:
                # SEMESTER (per 6 bulan)
                # Average: rata-rata min dan max per semester
                sem_avg = (sem_min + sem_max) / 2
                # Starting: biaya semester pertama (menggunakan rata-rata)
                sem_start = sem_avg
                # Ending: total biaya 8 semester (4 tahun)
                sem_end = sem_avg * 8
                
                # YEARLY (tahunan)
                # Average: rata-rata biaya per tahun (2 semester)
                year_avg = sem_avg * 2
                # Starting: biaya tahun pertama
                year_start = sem_avg * 2
                # Ending: total biaya 4 tahun kuliah
                year_end = sem_avg * 8
                
                semester_average.append(int(sem_avg))
                semester_starting.append(int(sem_start))
                semester_ending.append(int(sem_end))
                yearly_average.append(int(year_avg))
                yearly_starting.append(int(year_start))
                yearly_ending.append(int(year_end))
            else:
                semester_average.append('-')
                semester_starting.append('-')
                semester_ending.append('-')
                yearly_average.append('-')
                yearly_starting.append('-')
                yearly_ending.append('-')
        
        # Hapus kolom biaya lama
        old_biaya_cols = [
            'biaya_semester_min', 'biaya_semester_max', 'biaya_rata_tahunan',
            'biaya_rata_keseluruhan', 'uang_pangkal', 'biaya_semester',
            'biaya_per_bulan', 'biaya_per_tahun', 'biaya_total'
        ]
        
        for col in old_biaya_cols:
            if col in self.df.columns:
                self.df = self.df.drop(columns=[col])
        
        # Tambah kolom biaya baru (urutan: Yearly dulu, baru Semester)
        self.df['yearly_average'] = yearly_average
        self.df['yearly_starting'] = yearly_starting
        self.df['yearly_ending'] = yearly_ending
        self.df['semester_average'] = semester_average
        self.df['semester_starting'] = semester_starting
        self.df['semester_ending'] = semester_ending
        
        self.log_info("✓ Struktur biaya berhasil diubah menjadi 6 kolom:")
        self.log_info("\n  YEARLY (Tahunan):")
        self.log_info("    - yearly_average: rata-rata biaya per tahun (2 semester)")
        self.log_info("    - yearly_starting: biaya tahun pertama")
        self.log_info("    - yearly_ending: total biaya 4 tahun kuliah")
        self.log_info("\n  SEMESTER (Per 6 bulan):")
        self.log_info("    - semester_average: rata-rata biaya per semester")
        self.log_info("    - semester_starting: biaya semester pertama")
        self.log_info("    - semester_ending: total biaya 8 semester (4 tahun)")
    
    def step6_map_provinsi(self):
        """STEP 6: Map provinsi dari provinsi_id ke nama provinsi"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 6: MAPPING PROVINSI")
        self.log_info("="*60)
        
        if 'provinsi_id' in self.df.columns:
            # Cek berapa banyak yang perlu di-mapping
            needs_mapping = self.df['provinsi_id'].notna().sum()
            
            if needs_mapping > 0:
                # Map provinsi_id ke nama provinsi
                self.df['provinsi'] = self.df['provinsi_id'].map(self.provinsi_map)
                
                # Fill NaN dengan '-' jika ada
                self.df['provinsi'] = self.df['provinsi'].fillna('-')
                
                # Hapus provinsi_id setelah mapping
                self.df = self.df.drop(columns=['provinsi_id'])
                
                self.log_info(f"✓ Provinsi berhasil di-mapping: {needs_mapping} entries")
                self.log_info(f"  Total provinsi unique: {self.df['provinsi'].nunique()}")
            else:
                self.log_info("  Tidak ada provinsi_id yang perlu di-mapping")
                if 'provinsi_id' in self.df.columns:
                    self.df = self.df.drop(columns=['provinsi_id'])
        else:
            self.log_info("  Kolom provinsi_id tidak ditemukan")
    
    def step7_remove_duplicates(self):
        """STEP 7: Hapus duplikat berdasarkan semua kolom"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 7: HAPUS DUPLIKAT")
        self.log_info("="*60)
        
        before_count = len(self.df)
        
        # Identifikasi duplikat
        duplicates = self.df[self.df.duplicated(keep='first')]
        
        if len(duplicates) > 0:
            self.log_info(f"\n  Contoh duplikat (5 pertama):")
            for idx, row in duplicates.head().iterrows():
                self.log_info(f"    - {row.get('nama_kampus', 'N/A')} | {row.get('prodi', 'N/A')}")
        
        # Hapus duplikat
        self.df = self.df.drop_duplicates(keep='first')
        
        after_count = len(self.df)
        removed = before_count - after_count
        
        self.log_info(f"✓ Duplikat dihapus: {removed} rows")
        self.log_info(f"  Data sebelum: {before_count} rows")
        self.log_info(f"  Data setelah: {after_count} rows")
    
    def step8_check_null_values(self):
        """STEP 8: Cek kolom NULL"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 8: CEK NULL VALUES")
        self.log_info("="*60)
        
        null_report = []
        
        for col in self.df.columns:
            null_count = self.df[col].isna().sum()
            dash_count = (self.df[col] == '-').sum()
            empty_count = (self.df[col] == '').sum()
            
            total_missing = null_count + dash_count + empty_count
            percentage = (total_missing / len(self.df)) * 100
            
            if total_missing > 0:
                null_report.append({
                    'kolom': col,
                    'null': null_count,
                    'dash': dash_count,
                    'empty': empty_count,
                    'total_missing': total_missing,
                    'percentage': percentage
                })
        
        if null_report:
            self.log_info("\n  LAPORAN MISSING DATA:")
            self.log_info("  " + "-"*80)
            self.log_info(f"  {'Kolom':<30} {'NULL':<8} {'Dash':<8} {'Empty':<8} {'Total':<8} {'%':<8}")
            self.log_info("  " + "-"*80)
            
            for item in sorted(null_report, key=lambda x: x['total_missing'], reverse=True):
                self.log_info(
                    f"  {item['kolom']:<30} {item['null']:<8} {item['dash']:<8} "
                    f"{item['empty']:<8} {item['total_missing']:<8} {item['percentage']:.1f}%"
                )
        else:
            self.log_info("  ✓ Tidak ada missing data!")
    
    def save_processed_data(self, output_file=None):
        """Simpan data yang sudah diproses"""
        if output_file is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            preprocess_dir = os.path.join(base_dir, "preprocess")
            os.makedirs(preprocess_dir, exist_ok=True)
            
            filename = os.path.basename(self.input_file).replace('.csv', '_preprocessed.csv')
            output_file = os.path.join(preprocess_dir, filename)
        
        self.df.to_csv(output_file, index=False, encoding='utf-8-sig')
        self.log_info(f"\n✓ Data preprocessed disimpan di: {output_file}")
        self.log_info(f"  Total rows: {len(self.df)}")
        
        return output_file
    
    def save_log(self, log_file=None):
        """Simpan log preprocessing"""
        if log_file is None:
            log_file = self.input_file.replace('.csv', '_preprocessing_log.txt')
        
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(self.log))
        
        self.log_info(f"✓ Log disimpan di: {log_file}")
    
    def run_all_steps(self):
        """Jalankan semua step preprocessing"""
        self.log_info("="*60)
        self.log_info("MULAI PREPROCESSING DATA - 6 KOLOM BIAYA VERSION")
        self.log_info("="*60)
        
        if not self.load_data():
            return False
        
        # Original data info
        self.log_info(f"\nData Original:")
        self.log_info(f"  Rows: {len(self.df)}")
        self.log_info(f"  Columns: {len(self.df.columns)}")
        if 'nama_kampus' in self.df.columns:
            self.log_info(f"  Kampus Unique: {self.df['nama_kampus'].nunique()}")
        
        # Jalankan semua step
        self.step1_remove_unwanted_columns()
        self.step2_uppercase_all_text()
        self.step3_remove_brackets_and_content()
        self.step4_trim_all_text()
        self.step5_restructure_biaya_columns()
        self.step6_map_provinsi()
        self.step7_remove_duplicates()
        self.step8_check_null_values()
        
        # Simpan hasil
        output_file = self.save_processed_data()
        self.save_log()
        
        self.log_info("\n" + "="*60)
        self.log_info("PREPROCESSING SELESAI!")
        self.log_info("="*60)
        
        return output_file


# ============================================
# MAIN PROGRAM
# ============================================

if __name__ == "__main__":
    
    # Path file input
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_file = os.path.join(base_dir, "csv_result", "rencanamu.csv")
    
    # Cek file exists
    if not os.path.exists(input_file):
        print(f"❌ File tidak ditemukan: {input_file}")
        print("Pastikan file CSV ada di lokasi yang benar!")
    else:
        # Jalankan preprocessing
        preprocessor = DataPreprocessor(input_file)
        output_file = preprocessor.run_all_steps()
        
        if output_file:
            print(f"\n{'='*60}")
            print("HASIL PREPROCESSING:")
            print(f"{'='*60}")
            print(f"✓ File output: {output_file}")
            print(f"✓ Log file: {input_file.replace('.csv', '_preprocessing_log.txt')}")
            
            # Tampilkan summary
            df_final = pd.read_csv(output_file)
            print(f"\nSummary Data Akhir:")
            print(f"  Total rows: {len(df_final)}")
            print(f"  Total kolom: {len(df_final.columns)}")
            print(f"  Kolom: {', '.join(df_final.columns.tolist())}")
            if 'nama_kampus' in df_final.columns:
                print(f"  Total kampus unique: {df_final['nama_kampus'].nunique()}")
            if 'prodi' in df_final.columns:
                print(f"  Total prodi: {len(df_final)}")
            
            # Tampilkan contoh biaya
            print(f"\nContoh Perhitungan Biaya (5 baris pertama dengan biaya):")
            biaya_cols = ['yearly_average', 'yearly_starting', 'yearly_ending', 
                         'semester_average', 'semester_starting', 'semester_ending']
            df_with_biaya = df_final[df_final['yearly_average'] != '-']
            if len(df_with_biaya) > 0:
                print(df_with_biaya[['nama_kampus', 'prodi'] + biaya_cols].head())