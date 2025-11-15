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
        """STEP 1: Hapus kolom yang tidak diperlukan (KECUALI link_kampus dan status)"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 1: HAPUS KOLOM TIDAK DIPERLUKAN")
        self.log_info("="*60)
        self.log_info("# Menghapus kolom yang tidak relevan untuk analisis")
        
        # REVISI: Jangan hapus 'status' karena akan dijadikan body_type
        columns_to_remove = [
            'kota', 'ranking_webometric',
            'fax', 'luas_kampus', 'jenjang_tersedia', 'fasilitas', 
            'beasiswa', 'kerjasama', 'prestasi', 'visi', 'misi'
        ]
        existing_cols = [col for col in columns_to_remove if col in self.df.columns]
        
        if existing_cols:
            self.df = self.df.drop(columns=existing_cols)
            self.log_info(f"✓ Kolom dihapus: {', '.join(existing_cols)}")
        else:
            self.log_info("  Tidak ada kolom yang perlu dihapus")
        
        # Log: link_kampus dan status akan tetap dipertahankan
        if 'link_kampus' in self.df.columns:
            self.log_info("✓ Kolom 'link_kampus' dipertahankan untuk digunakan sebagai 'link'")
        if 'status' in self.df.columns:
            self.log_info("✓ Kolom 'status' dipertahankan untuk dijadikan 'body_type'")
    
    def step2_uppercase_all_text(self):
        """STEP 2: Uppercase semua text dan bersihkan akreditasi"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 2: UPPERCASE SEMUA TEXT & BERSIHKAN AKREDITASI")
        self.log_info("="*60)
        
        text_columns = ['nama_kampus', 'akreditasi_kampus', 'alamat', 'provinsi',
                       'fakultas', 'prodi', 'akreditasi_prodi', 'status']
        
        for col in text_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(
                    lambda x: x.upper() if isinstance(x, str) and x != '-' else x
                )
        
        akreditasi_cols = ['akreditasi_kampus', 'akreditasi_prodi']
        for col in akreditasi_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(lambda x: self._clean_akreditasi(x))
        
        self.log_info(f"✓ Text columns di-uppercase dan akreditasi dibersihkan")
    
    def _clean_akreditasi(self, value):
        """Bersihkan format akreditasi"""
        if pd.isna(value) or value == '' or str(value).strip() == '':
            return '-'
        val = str(value).upper().strip()
        val = re.sub(r'^[A-Z]+:', '', val).strip()
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
        """STEP 5: Restructure kolom biaya"""
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
                sem_avg = (sem_min + sem_max) / 2
                sem_start = sem_avg
                sem_end = sem_avg * 8
                year_avg = sem_avg * 2
                year_start = sem_avg * 2
                year_end = sem_avg * 8
                
                semester_average.append(int(sem_avg))
                semester_starting.append(int(sem_start))
                semester_ending.append(int(sem_end))
                yearly_average.append(int(year_avg))
                yearly_starting.append(int(year_start))
                yearly_ending.append(int(year_end))
            else:
                semester_average.append(-1)
                semester_starting.append(-1)
                semester_ending.append(-1)
                yearly_average.append(-1)
                yearly_starting.append(-1)
                yearly_ending.append(-1)
        
        old_biaya_cols = ['biaya_semester_min', 'biaya_semester_max', 'biaya_rata_tahunan',
                         'biaya_rata_keseluruhan', 'uang_pangkal', 'biaya_semester',
                         'biaya_per_bulan', 'biaya_per_tahun', 'biaya_total']
        
        for col in old_biaya_cols:
            if col in self.df.columns:
                self.df = self.df.drop(columns=[col])
        
        self.df['average_semester_fee'] = semester_average
        self.df['starting_semester_fee'] = semester_starting
        self.df['ending_semester_fee'] = semester_ending
        self.df['average_yearly_fee'] = yearly_average
        self.df['starting_yearly_fee'] = yearly_starting
        self.df['ending_yearly_fee'] = yearly_ending
        
        self.log_info("✓ Struktur biaya berhasil diubah menjadi 6 kolom")
        self.log_info("  Missing data biaya diisi dengan -1")
    
    def step6_map_provinsi(self):
        """STEP 6: Map provinsi dari provinsi_id ke nama provinsi"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 6: MAPPING PROVINSI")
        self.log_info("="*60)
        
        if 'provinsi_id' in self.df.columns:
            needs_mapping = self.df['provinsi_id'].notna().sum()
            
            if needs_mapping > 0:
                self.df['provinsi'] = self.df['provinsi_id'].map(self.provinsi_map)
                self.df['provinsi'] = self.df['provinsi'].fillna('-')
                self.df = self.df.drop(columns=['provinsi_id'])
                self.log_info(f"✓ Provinsi berhasil di-mapping: {needs_mapping} entries")
            else:
                self.log_info("  Tidak ada provinsi_id yang perlu di-mapping")
                if 'provinsi_id' in self.df.columns:
                    self.df = self.df.drop(columns=['provinsi_id'])
        else:
            self.log_info("  Kolom provinsi_id tidak ditemukan")
    
    def step7_add_new_columns(self):
        """STEP 7: Tambahkan kolom baru yang diperlukan"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 7: TAMBAH KOLOM BARU")
        self.log_info("="*60)
        
        # Mapping untuk extract data yang ada
        def safe_get(val, default='-'):
            if pd.isna(val) or val == '' or str(val).strip() == '':
                return default
            return val
        
        # 1. body_type - DARI STATUS (REVISI!)
        if 'status' in self.df.columns:
            self.df['body_type'] = self.df['status'].apply(lambda x: safe_get(x))
            self.log_info("✓ Kolom 'body_type' ditambahkan (dari kolom status: NEGERI/SWASTA)")
        else:
            self.df['body_type'] = '-'
            self.log_info("⚠️  Kolom 'body_type' ditambahkan dengan default '-' (status tidak ada)")
        
        # 2. link - gunakan link_kampus (BUKAN website kampus)
        if 'link_kampus' in self.df.columns:
            self.df['link'] = self.df['link_kampus'].apply(lambda x: safe_get(x))
            self.log_info("✓ Kolom 'link' ditambahkan (dari link_kampus rencanamu.id)")
        else:
            self.df['link'] = '-'
            self.log_info("⚠️  Kolom 'link' ditambahkan dengan default '-' (link_kampus tidak ada)")
        
        # 3. description - default '-'
        self.df['description'] = '-'
        self.log_info("✓ Kolom 'description' ditambahkan (default: '-')")
        
        # 4. contact - gabungan dari telepon dan email
        def create_contact(row):
            parts = []
            if 'telepon' in self.df.columns:
                tel = safe_get(row.get('telepon'))
                if tel != '-':
                    parts.append(f"Tel: {tel}")
            if 'email' in self.df.columns:
                email = safe_get(row.get('email'))
                if email != '-':
                    parts.append(f"Email: {email}")
            return ' | '.join(parts) if parts else '-'
        
        self.df['contact'] = self.df.apply(create_contact, axis=1)
        self.log_info("✓ Kolom 'contact' ditambahkan (dari telepon & email)")
        
        # 5. lecturer_amount - dari jumlah_dosen atau -1
        if 'jumlah_dosen' in self.df.columns:
            self.df['lecturer_amount'] = self.df['jumlah_dosen'].apply(
                lambda x: int(x) if pd.notna(x) and str(x).isdigit() else -1
            )
        else:
            self.df['lecturer_amount'] = -1
        self.log_info("✓ Kolom 'lecturer_amount' ditambahkan")
        
        # 6. student_amount - dari jumlah_mahasiswa atau -1
        if 'jumlah_mahasiswa' in self.df.columns:
            self.df['student_amount'] = self.df['jumlah_mahasiswa'].apply(
                lambda x: int(x) if pd.notna(x) and str(x).isdigit() else -1
            )
        else:
            self.df['student_amount'] = -1
        self.log_info("✓ Kolom 'student_amount' ditambahkan")
        
        # Hapus kolom lama yang sudah tidak diperlukan (termasuk status, link_kampus, website)
        cols_to_drop = ['status', 'link_kampus', 'website', 'telepon', 'email', 
                       'jumlah_dosen', 'jumlah_mahasiswa', 'tahun_berdiri']
        existing = [col for col in cols_to_drop if col in self.df.columns]
        if existing:
            self.df = self.df.drop(columns=existing)
            self.log_info(f"✓ Kolom tidak diperlukan dihapus: {', '.join(existing)}")
    
    def step8_remove_duplicates(self):
        """STEP 8: Hapus duplikat"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 8: HAPUS DUPLIKAT")
        self.log_info("="*60)
        
        before_count = len(self.df)
        self.df = self.df.drop_duplicates(keep='first')
        after_count = len(self.df)
        removed = before_count - after_count
        
        self.log_info(f"✓ Duplikat dihapus: {removed} rows")
        self.log_info(f"  Data sebelum: {before_count} rows")
        self.log_info(f"  Data setelah: {after_count} rows")
    
    def step9_add_institution_code(self):
        """STEP 9: Tambahkan kolom institution_code berdasarkan unique institution"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 9: TAMBAH INSTITUTION CODE")
        self.log_info("="*60)
        
        # Buat mapping institution_name -> institution_code
        unique_institutions = self.df['institution_name'].unique()
        institution_code_map = {
            name: f'rencanamu-{i+1}' 
            for i, name in enumerate(sorted(unique_institutions))
        }
        
        # Assign institution_code berdasarkan nama kampus
        self.df.insert(0, 'institution_code', 
                      self.df['institution_name'].map(institution_code_map))
        
        self.log_info(f"✓ Institution code ditambahkan untuk {len(unique_institutions)} kampus unique")
        self.log_info(f"  Range: rencanamu-1 sampai rencanamu-{len(unique_institutions)}")
    
    def step10_rename_columns(self):
        """STEP 10: Rename kolom sesuai standar baru"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 10: RENAME KOLOM")
        self.log_info("="*60)
        
        rename_dict = {
            'nama_kampus': 'institution_name',
            'fakultas': 'faculty',
            'alamat': 'address',
            'provinsi': 'province',
            'akreditasi_kampus': 'campus_accreditation'
        }
        
        renamed = []
        for old_name, new_name in rename_dict.items():
            if old_name in self.df.columns:
                self.df = self.df.rename(columns={old_name: new_name})
                renamed.append(f"{old_name} → {new_name}")
        
        if renamed:
            self.log_info("✓ Kolom berhasil direname:")
            for r in renamed:
                self.log_info(f"  - {r}")

    def step11_separate_prodi(self):
        """STEP 11: Pisahkan data prodi ke file terpisah"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 11: PISAHKAN DATA PRODI")
        self.log_info("="*60)
        
        # Buat dataframe institusi (unique per kampus) dengan aggregation
        agg_dict = {
            'institution_name': 'first',
            'campus_accreditation': 'first',
            'address': 'first',
            'province': 'first',
            'body_type': 'first',
            'link': 'first',
            'description': 'first',
            'contact': 'first',
            'lecturer_amount': 'first',
            'student_amount': 'first',
            'average_semester_fee': 'mean',
            'starting_semester_fee': 'mean',
            'ending_semester_fee': 'mean',
            'average_yearly_fee': 'mean',
            'starting_yearly_fee': 'mean',
            'ending_yearly_fee': 'mean'
        }
        
        self.df_institution = self.df.groupby('institution_code', as_index=False).agg(agg_dict)
        
        # Convert fee columns ke integer (bulatkan) dan handle -1
        fee_cols = ['average_semester_fee', 'starting_semester_fee', 'ending_semester_fee',
                    'average_yearly_fee', 'starting_yearly_fee', 'ending_yearly_fee']
        for col in fee_cols:
            self.df_institution[col] = self.df_institution[col].apply(
                lambda x: -1 if x == -1 else int(round(x))
            )
        
        # Buat dataframe prodi TANPA fee
        prodi_cols = ['institution_code', 'faculty', 'prodi', 'akreditasi_prodi']
        self.df_prodi = self.df[prodi_cols].copy()
        
        # Tambahkan prodi_code
        self.df_prodi.insert(0, 'prodi_code', 
                            [f'prodi-{i+1}' for i in range(len(self.df_prodi))])
        
        self.log_info(f"✓ Data institusi: {len(self.df_institution)} kampus (body_type = NEGERI/SWASTA)")
        self.log_info(f"✓ Data prodi: {len(self.df_prodi)} program studi (tanpa fee)")
        
        return self.df_institution, self.df_prodi
    
    def step12_check_null_values(self):
        """STEP 12: Cek kolom NULL untuk kedua tabel"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 12: CEK NULL VALUES")
        self.log_info("="*60)
        
        for table_name, df in [('INSTITUSI', self.df_institution), ('PRODI', self.df_prodi)]:
            self.log_info(f"\n  TABEL {table_name}:")
            self.log_info("  " + "-"*80)
            
            null_report = []
            
            for col in df.columns:
                null_count = df[col].isna().sum()
                dash_count = (df[col] == '-').sum()
                minus_one_count = (df[col] == -1).sum()
                empty_count = (df[col] == '').sum()
                total_missing = null_count + dash_count + minus_one_count + empty_count
                percentage = (total_missing / len(df)) * 100
                
                if total_missing > 0:
                    null_report.append({
                        'kolom': col,
                        'null': null_count,
                        'dash': dash_count,
                        'minus_one': minus_one_count,
                        'empty': empty_count,
                        'total_missing': total_missing,
                        'percentage': percentage
                    })
            
            if null_report:
                self.log_info(f"  {'Kolom':<30} {'NULL':<7} {'Dash':<7} {'-1':<7} {'Empty':<7} {'Total':<7} {'%':<8}")
                self.log_info("  " + "-"*80)
                
                for item in sorted(null_report, key=lambda x: x['total_missing'], reverse=True):
                    self.log_info(
                        f"  {item['kolom']:<30} {item['null']:<7} {item['dash']:<7} "
                        f"{item['minus_one']:<7} {item['empty']:<7} {item['total_missing']:<7} {item['percentage']:.1f}%"
                    )
            else:
                self.log_info("  ✓ Tidak ada missing data!")

    def step13_remove_empty_rows(self):
        """STEP 13: Hapus baris kosong di kedua tabel"""
        self.log_info("\n" + "="*60)
        self.log_info("STEP 13: HAPUS BARIS KOSONG")
        self.log_info("="*60)
        
        # Hapus baris kosong di df_institution
        condition_to_drop = self.df_institution['institution_name'] == '-'
        self.df_institution = self.df_institution[~condition_to_drop]
        removed_inst = condition_to_drop.sum()
        self.log_info(f"✓ Baris kosong di tabel INSTITUSI dihapus: {removed_inst} rows")
        
        # Hapus baris kosong di df_prodi
        condition_to_drop = self.df_prodi['prodi'] == '-'
        self.df_prodi = self.df_prodi[~condition_to_drop]
        removed_prodi = condition_to_drop.sum()
        self.log_info(f"✓ Baris kosong di tabel PRODI dihapus: {removed_prodi} rows")
    
    def save_processed_data(self, output_dir=None):
        """Simpan data yang sudah diproses"""
        if output_dir is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            output_dir = os.path.join(base_dir, "csv_result")
            os.makedirs(output_dir, exist_ok=True)
        
        # Simpan tabel institusi
        institution_file = os.path.join(output_dir, "rencanamu_institutions_preprocessed.csv")
        self.df_institution.to_csv(institution_file, index=False, encoding='utf-8-sig')
        self.log_info(f"\n✓ Data institusi disimpan: {institution_file}")
        self.log_info(f"  Total rows: {len(self.df_institution)}")
        
        # Simpan tabel prodi
        prodi_file = os.path.join(output_dir, "rencanamu_prodi_preprocessed.csv")
        self.df_prodi.to_csv(prodi_file, index=False, encoding='utf-8-sig')
        self.log_info(f"✓ Data prodi disimpan: {prodi_file}")
        self.log_info(f"  Total rows: {len(self.df_prodi)}")
        
        return institution_file, prodi_file
    
    # def save_log(self, log_file=None):
        """Simpan log preprocessing"""
        if log_file is None:
            log_file = self.input_file.replace('.csv', '_preprocessing_log.txt')
        
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(self.log))
        
        self.log_info(f"✓ Log disimpan di: {log_file}")
    
    def run_all_steps(self):
        """Jalankan semua step preprocessing"""
        self.log_info("="*60)
        self.log_info("MULAI PREPROCESSING DATA")
        self.log_info("="*60)
        
        if not self.load_data():
            return False
        
        self.log_info(f"\nData Original:")
        self.log_info(f"  Rows: {len(self.df)}")
        self.log_info(f"  Columns: {len(self.df.columns)}")
        
        self.step1_remove_unwanted_columns()
        self.step2_uppercase_all_text()
        self.step3_remove_brackets_and_content()
        self.step4_trim_all_text()
        self.step5_restructure_biaya_columns()
        self.step6_map_provinsi()
        self.step7_add_new_columns()
        self.step8_remove_duplicates()
        self.step10_rename_columns()
        self.step9_add_institution_code()
        self.step11_separate_prodi()
        self.step12_check_null_values()
        self.step13_remove_empty_rows()
        
        institution_file, prodi_file = self.save_processed_data()
        
        self.log_info("\n" + "="*60)
        self.log_info("PREPROCESSING SELESAI!")
        self.log_info("="*60)
        self.log_info("\n✓ body_type sekarang berisi: NEGERI atau SWASTA")
        
        return institution_file, prodi_file


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_file = os.path.join(base_dir, "csv_result", "rencanamu.csv")
    
    if not os.path.exists(input_file):
        print(f"❌ File tidak ditemukan: {input_file}")
        print("Pastikan file CSV ada di lokasi yang benar!")
    else:
        preprocessor = DataPreprocessor(input_file)
        result = preprocessor.run_all_steps()
        
        if result:
            institution_file, prodi_file = result
            print(f"\n{'='*60}")
            print("HASIL PREPROCESSING:")
            print(f"{'='*60}")
            print(f"✓ File institusi: {institution_file}")
            print(f"✓ File prodi: {prodi_file}")
            
            df_inst = pd.read_csv(institution_file)
            df_prodi = pd.read_csv(prodi_file)
            
            print(f"\nTABEL INSTITUSI:")
            print(f"  Total kampus: {len(df_inst)}")
            print(f"  Kolom: {', '.join(df_inst.columns.tolist())}")
            
            print(f"\nTABEL PRODI:")
            print(f"  Total program studi: {len(df_prodi)}")
            print(f"  Kolom: {', '.join(df_prodi.columns.tolist())}")
            
            print(f"\nRelasi:")
            print(f"  institution_code sebagai foreign key di tabel prodi")
            
            # Cek sample data untuk link dan body_type
            print(f"\nSAMPLE DATA LINK & BODY_TYPE:")
            print(df_inst[['institution_name', 'body_type', 'link']].head(3).to_string(index=False))