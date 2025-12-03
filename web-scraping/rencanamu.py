import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import csv
import re
from urllib.parse import urljoin
import os

class RencanamuScraper:
    def __init__(self):
        self.base_url = "https://rencanamu.id"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
    
    def safe_value(self, value):
        """Return '-' if value is None, empty, or 'None'"""
        if not value or value == 'None' or str(value).strip() == '':
            return '-'
        return str(value).strip()
    
    def get_soup(self, url):
        """Mengambil dan parse HTML dari URL"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def clean_soup_from_scripts(self, soup):
        """Hapus semua script, style, dan elemen tidak berguna"""
        if not soup:
            return soup
        
        for script in soup.find_all('script'):
            script.decompose()
        
        for style in soup.find_all('style'):
            style.decompose()
        
        for noscript in soup.find_all('noscript'):
            noscript.decompose()
        
        from bs4 import Comment
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        return soup
    
    def is_javascript_content(self, text):
        """Deteksi apakah text mengandung JavaScript/JSON"""
        if not text or len(text) < 10:
            return False
        
        js_patterns = [
            r'function\s*\(',
            r'var\s+\w+\s*=',
            r'const\s+\w+\s*=',
            r'let\s+\w+\s*=',
            r'\$\(',
            r'\.ajax\s*\(',
            r'\.get\s*\(',
            r'\.post\s*\(',
            r'=>\s*{',
            r'data:\s*{',
            r'success:\s*function',
            r'\.val\(\)',
            r'\.html\(',
            r'document\.',
            r'window\.',
            r'console\.',
            r'return\s+\w+',
            r'\{"".*"":\s*"".*""\}',
            r'pp_id|pp_for|pp_post',
            r'ppc_id|ppa_id',
            r'owl.*Carousel',
            r'getElementById',
            r'querySelector',
        ]
        
        text_sample = text[:500]
        
        for pattern in js_patterns:
            if re.search(pattern, text_sample, re.IGNORECASE):
                return True
        
        return False
    
    def get_clean_text(self, soup):
        """Ambil text yang sudah dibersihkan dari script/style dan JavaScript"""
        if not soup:
            return ""
        
        clean_soup = BeautifulSoup(str(soup), 'html.parser')
        clean_soup = self.clean_soup_from_scripts(clean_soup)
        
        text = clean_soup.get_text()
        lines = text.split('\n')
        clean_lines = []
        
        for line in lines:
            line = line.strip()
            
            if not line:
                continue
            
            if self.is_javascript_content(line):
                continue
            
            if len(line) < 3:
                continue
            
            clean_lines.append(line)
        
        text = ' '.join(clean_lines)
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('\xa0', ' ')
        
        return text.strip()
    
    def get_all_provinces(self):
        """ID provinsi dari 1-34"""
        return list(range(1, 35))
    
    def get_universities_by_province(self, prov_id, max_pages=100):
        """Scraping daftar universitas per provinsi"""
        universities = []
        
        for page in range(1, max_pages + 1):
            url = f"{self.base_url}/cari-kampus/page/{page}?prov={prov_id}&jurusan=&status=S"
            print(f"Scraping provinsi {prov_id}, halaman {page}...", end=' ')
            
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code != 200:
                    print("Tidak ada halaman")
                    break
                    
                soup = BeautifulSoup(response.content, 'html.parser')
                links = soup.find_all('a', href=re.compile(r'/cari-kampus/[^/]+$'))
                
                if not links:
                    print("Tidak ada kampus")
                    break
                
                page_unis = []
                for link in links:
                    href = link.get('href', '')
                    if href and '/cari-kampus/' in href:
                        full_url = urljoin(self.base_url, href)
                        if full_url not in [u['link_kampus'] for u in universities]:
                            page_unis.append({
                                'link_kampus': full_url,
                                'prov_id': prov_id
                            })
                
                universities.extend(page_unis)
                print(f"{len(page_unis)} kampus")
                
                if len(page_unis) == 0:
                    break
                
                time.sleep(1)
                
            except Exception as e:
                print(f"Error: {e}")
                break
        
        print(f"Total kampus di provinsi {prov_id}: {len(universities)}")
        return universities
    
    def extract_biaya_from_text(self, text, soup):
        """Ekstrak semua informasi biaya dari text dan HTML yang sudah dibersihkan"""
        biaya_info = {
            'biaya_semester_min': '-',
            'biaya_semester_max': '-',
            'biaya_rata_tahunan': '-',
            'biaya_rata_keseluruhan': '-',
            'uang_pangkal': '-',
            'biaya_semester': '-',
            'biaya_per_bulan': '-',
            'biaya_per_tahun': '-',
            'biaya_total': '-'
        }
        
        text = text.replace('\xa0', ' ')
        
        biaya_sections = soup.find_all(['div', 'section', 'span', 'p', 'td', 'th'], 
                                       class_=re.compile(r'biaya|cost|price|tuition|fee', re.IGNORECASE))
        
        combined_text = text
        for section in biaya_sections:
            section_copy = BeautifulSoup(str(section), 'html.parser')
            for script in section_copy.find_all('script'):
                script.decompose()
            
            section_text = section_copy.get_text()
            
            if not self.is_javascript_content(section_text):
                combined_text += " " + section_text
        
        # Pattern untuk Biaya Semester Min/Max
        sem_min = re.search(r'Biaya\s+Semester\s+Min[:\s]*Rp\.?\s*([0-9.,]+)', combined_text, re.IGNORECASE)
        if sem_min:
            biaya_info['biaya_semester_min'] = sem_min.group(1).replace('.', '').replace(',', '')
        
        sem_max = re.search(r'Biaya\s+Semester\s+Max[:\s]*Rp\.?\s*([0-9.,]+)', combined_text, re.IGNORECASE)
        if sem_max:
            biaya_info['biaya_semester_max'] = sem_max.group(1).replace('.', '').replace(',', '')
        
        # Pattern untuk Biaya Rata-Rata Tahunan
        rata_tahun = re.search(r'Biaya\s+Rata[-\s]*Rata\s+Tahunan[:\s]*Rp\.?\s*([0-9.,]+)', combined_text, re.IGNORECASE)
        if rata_tahun:
            biaya_info['biaya_rata_tahunan'] = rata_tahun.group(1).replace('.', '').replace(',', '')
        
        # Pattern untuk Biaya Rata-Rata Keseluruhan
        rata_total = re.search(r'Biaya\s+Rata[-\s]*Rata\s+Keseluruhan[:\s]*Rp\.?\s*([0-9.,]+)', combined_text, re.IGNORECASE)
        if rata_total:
            biaya_info['biaya_rata_keseluruhan'] = rata_total.group(1).replace('.', '').replace(',', '')
        
        # Pattern untuk uang pangkal
        pangkal = re.search(r'[Uu]ang\s+[Pp]angkal[:\s]*Rp\.?\s*([0-9.,]+)', combined_text, re.IGNORECASE)
        if pangkal:
            biaya_info['uang_pangkal'] = pangkal.group(1).replace('.', '').replace(',', '')
        
        # Pattern untuk biaya semester (generic)
        semester = re.search(r'[Bb]iaya\s+(?:[Kk]uliah\s+)?(?:[Pp]er\s+)?[Ss]emester[:\s]*Rp\.?\s*([0-9.,]+)', combined_text, re.IGNORECASE)
        if semester and biaya_info['biaya_semester_min'] == '-':
            biaya_info['biaya_semester'] = semester.group(1).replace('.', '').replace(',', '')
        
        # Pattern untuk biaya bulanan
        bulan = re.search(r'[Bb]iaya\s+(?:[Kk]uliah\s+)?[Pp]er\s*[Bb]ulan[:\s]*Rp\.?\s*([0-9.,]+)', combined_text, re.IGNORECASE)
        if bulan:
            biaya_info['biaya_per_bulan'] = bulan.group(1).replace('.', '').replace(',', '')
        
        # Pattern untuk biaya per tahun
        tahun = re.search(r'[Bb]iaya\s+(?:[Pp]er\s+)?[Tt]ahun[:\s]*Rp\.?\s*([0-9.,]+)', combined_text, re.IGNORECASE)
        if tahun and biaya_info['biaya_rata_tahunan'] == '-':
            biaya_info['biaya_per_tahun'] = tahun.group(1).replace('.', '').replace(',', '')
        
        # Pattern untuk total biaya
        total = re.search(r'[Tt]otal\s+(?:[Bb]iaya|[Dd]ana)[:\s]*Rp\.?\s*([0-9.,]+)', combined_text, re.IGNORECASE)
        if total and biaya_info['biaya_rata_keseluruhan'] == '-':
            biaya_info['biaya_total'] = total.group(1).replace('.', '').replace(',', '')
        
        # Format: Rp. 5.200.000 - Rp. 10.000.000
        range_pattern = re.search(r'Rp\.?\s*([0-9.,]+)\s*[-–—]\s*Rp\.?\s*([0-9.,]+)', combined_text)
        if range_pattern and biaya_info['biaya_semester_min'] == '-':
            biaya_info['biaya_semester_min'] = range_pattern.group(1).replace('.', '').replace(',', '')
            biaya_info['biaya_semester_max'] = range_pattern.group(2).replace('.', '').replace(',', '')
        
        return biaya_info
    
    def extract_additional_info(self, soup, text_content):
        """Ekstrak informasi tambahan dari website"""
        additional_info = {
            'telepon': '-',
            'email': '-',
            'fax': '-',
            'tahun_berdiri': '-',
            'jumlah_mahasiswa': '-',
            'jumlah_dosen': '-',
            'luas_kampus': '-',
            'jenjang_tersedia': '-',
            'fasilitas': '-',
            'beasiswa': '-',
            'kerjasama': '-',
            'prestasi': '-',
            'visi': '-',
            'misi': '-'
        }
        
        # Telepon
        phone_patterns = [
            r'telepon[:\s]*([0-9\s\-\+\(\)]+)',
            r'telp[:\s]*([0-9\s\-\+\(\)]+)',
            r'phone[:\s]*([0-9\s\-\+\(\)]+)',
            r'kontak[:\s]*([0-9\s\-\+\(\)]+)'
        ]
        for pattern in phone_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                phone = match.group(1).strip()
                if len(phone) >= 8:  # Minimal 8 digit
                    additional_info['telepon'] = phone
                    break
        
        # Email
        email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text_content)
        if email_match:
            additional_info['email'] = email_match.group(0)
        
        # Fax
        fax_match = re.search(r'fax[:\s]*([0-9\s\-\+\(\)]+)', text_content, re.IGNORECASE)
        if fax_match:
            additional_info['fax'] = fax_match.group(1).strip()
        
        # Tahun Berdiri
        tahun_patterns = [
            r'(?:didirikan|berdiri|dibentuk)[:\s]*(?:pada\s+)?(?:tahun\s+)?([12]\d{3})',
            r'(?:sejak|since)[:\s]*([12]\d{3})',
            r'established[:\s]*([12]\d{3})'
        ]
        for pattern in tahun_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                year = int(match.group(1))
                if 1900 <= year <= 2024:
                    additional_info['tahun_berdiri'] = str(year)
                    break
        
        # Jumlah Mahasiswa
        mhs_patterns = [
            r'(?:jumlah\s+)?mahasiswa[:\s]*(?:sekitar\s+)?([0-9.,]+)',
            r'([0-9.,]+)\s+mahasiswa',
            r'student[s]?[:\s]*([0-9.,]+)'
        ]
        for pattern in mhs_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                jumlah = match.group(1).replace('.', '').replace(',', '')
                if jumlah.isdigit() and int(jumlah) > 50:  # Minimal 50 mahasiswa
                    additional_info['jumlah_mahasiswa'] = jumlah
                    break
        
        # Jumlah Dosen
        dosen_patterns = [
            r'(?:jumlah\s+)?dosen[:\s]*(?:sekitar\s+)?([0-9.,]+)',
            r'([0-9.,]+)\s+dosen',
            r'(?:pengajar|faculty)[:\s]*([0-9.,]+)'
        ]
        for pattern in dosen_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                jumlah = match.group(1).replace('.', '').replace(',', '')
                if jumlah.isdigit() and int(jumlah) > 5:  # Minimal 5 dosen
                    additional_info['jumlah_dosen'] = jumlah
                    break
        
        # Luas Kampus
        luas_patterns = [
            r'luas[:\s]*(?:kampus|area|tanah)?[:\s]*([0-9.,]+)\s*(?:ha|hektar|m2|meter)',
            r'([0-9.,]+)\s*(?:ha|hektar|m2|meter)',
        ]
        for pattern in luas_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                additional_info['luas_kampus'] = match.group(1).strip() + ' ' + match.group(0).split()[-1]
                break
        
        # Jenjang Tersedia
        jenjang_list = []
        if re.search(r'\b(?:D1|D-1|Diploma\s+1)\b', text_content, re.IGNORECASE):
            jenjang_list.append('D1')
        if re.search(r'\b(?:D2|D-2|Diploma\s+2)\b', text_content, re.IGNORECASE):
            jenjang_list.append('D2')
        if re.search(r'\b(?:D3|D-3|Diploma\s+3)\b', text_content, re.IGNORECASE):
            jenjang_list.append('D3')
        if re.search(r'\b(?:D4|D-4|Diploma\s+4)\b', text_content, re.IGNORECASE):
            jenjang_list.append('D4')
        if re.search(r'\b(?:S1|S-1|Sarjana|Bachelor)\b', text_content, re.IGNORECASE):
            jenjang_list.append('S1')
        if re.search(r'\b(?:S2|S-2|Magister|Master)\b', text_content, re.IGNORECASE):
            jenjang_list.append('S2')
        if re.search(r'\b(?:S3|S-3|Doktor|PhD|Doctorate)\b', text_content, re.IGNORECASE):
            jenjang_list.append('S3')
        if re.search(r'\b(?:Profesi|Professional)\b', text_content, re.IGNORECASE):
            jenjang_list.append('Profesi')
        
        if jenjang_list:
            additional_info['jenjang_tersedia'] = ', '.join(sorted(set(jenjang_list)))
        
        # Fasilitas (ambil dari section fasilitas)
        fasilitas_section = soup.find(string=re.compile(r'fasilitas', re.IGNORECASE))
        if fasilitas_section:
            parent = fasilitas_section.find_parent()
            if parent:
                fasilitas_text = parent.get_text(strip=True)
                # Ambil maksimal 200 karakter
                if len(fasilitas_text) > 200:
                    additional_info['fasilitas'] = fasilitas_text[:200] + '...'
                else:
                    additional_info['fasilitas'] = fasilitas_text
        
        # Beasiswa
        if re.search(r'beasiswa|scholarship', text_content, re.IGNORECASE):
            additional_info['beasiswa'] = 'Tersedia'
        
        # Kerjasama
        kerjasama_match = re.search(r'(?:kerjasama|kerja sama|partnership|collaboration)[:\s]*([^\.]+)', text_content, re.IGNORECASE)
        if kerjasama_match:
            kerjasama_text = kerjasama_match.group(1).strip()
            if len(kerjasama_text) > 200:
                additional_info['kerjasama'] = kerjasama_text[:200] + '...'
            else:
                additional_info['kerjasama'] = kerjasama_text
        
        # Prestasi
        prestasi_match = re.search(r'(?:prestasi|achievement|award)[:\s]*([^\.]+)', text_content, re.IGNORECASE)
        if prestasi_match:
            prestasi_text = prestasi_match.group(1).strip()
            if len(prestasi_text) > 200:
                additional_info['prestasi'] = prestasi_text[:200] + '...'
            else:
                additional_info['prestasi'] = prestasi_text
        
        # Visi
        visi_match = re.search(r'visi[:\s]*([^\.]+(?:\.[^\.]+){0,2})', text_content, re.IGNORECASE)
        if visi_match:
            visi_text = visi_match.group(1).strip()
            if len(visi_text) > 300:
                additional_info['visi'] = visi_text[:300] + '...'
            else:
                additional_info['visi'] = visi_text
        
        # Misi
        misi_match = re.search(r'misi[:\s]*([^\.]+(?:\.[^\.]+){0,2})', text_content, re.IGNORECASE)
        if misi_match:
            misi_text = misi_match.group(1).strip()
            if len(misi_text) > 300:
                additional_info['misi'] = misi_text[:300] + '...'
            else:
                additional_info['misi'] = misi_text
        
        return additional_info
    
    def get_university_details(self, kampus_url):
        """Scraping detail universitas lengkap"""
        try:
            soup = self.get_soup(kampus_url)
            if not soup:
                return None
            
            clean_soup = self.clean_soup_from_scripts(soup)
            
            # Nama kampus
            nama = "-"
            title = soup.find('title')
            if title:
                nama = title.get_text(strip=True).split('-')[0].strip()
            if not nama or nama == "-":
                h1 = soup.find('h1')
                if h1:
                    nama = h1.get_text(strip=True)
            if not nama or nama == "-":
                meta = soup.find('meta', property='og:title')
                if meta:
                    nama = meta.get('content', '').split('-')[0].strip()
            if not nama or nama == "-":
                nama = kampus_url.split('/')[-1].replace('-', ' ').title()
            
            # Akreditasi
            akreditasi = "-"
            akred_elem = clean_soup.find(string=re.compile(r'Akreditas', re.IGNORECASE))
            if akred_elem:
                parent = akred_elem.find_parent()
                if parent:
                    akreditasi = parent.get_text(strip=True).replace('Akreditas', '').replace('Akreditasi', '').strip()
            
            # Lokasi
            alamat = "-"
            kota = "-"
            provinsi = "-"
            
            alamat_elem = clean_soup.find(string=re.compile(r'Alamat', re.IGNORECASE))
            if alamat_elem:
                addr_div = alamat_elem.find_next()
                if addr_div:
                    alamat = addr_div.get_text(strip=True)
                    if ',' in alamat:
                        parts = [p.strip() for p in alamat.split(',')]
                        for part in parts:
                            if any(x in part.lower() for x in ['kota', 'kabupaten', 'kab.']):
                                kota = part
                            elif len(part) > 3 and not any(c.isdigit() for c in part[:5]):
                                if provinsi == "-":
                                    provinsi = part
            
            # Website
            website = "-"
            web_link = clean_soup.find('a', href=re.compile(r'^https?://'))
            if web_link:
                href = web_link.get('href', '')
                if 'rencanamu.id' not in href:
                    website = href
            
            # Ranking
            ranking = "-"
            rank_elem = clean_soup.find(string=re.compile(r'Webometic|Ranking', re.IGNORECASE))
            if rank_elem:
                rank_parent = rank_elem.find_parent()
                if rank_parent:
                    rank_text = rank_parent.get_text(strip=True)
                    ranking = re.sub(r'Webometic Rank|Ranking', '', rank_text, flags=re.IGNORECASE).strip()
            
            # Status
            status = "-"
            status_elem = clean_soup.find(string=re.compile(r'Status', re.IGNORECASE))
            if status_elem:
                status_parent = status_elem.find_parent()
                if status_parent:
                    status_text = status_parent.get_text(strip=True).lower()
                    if 'swasta' in status_text:
                        status = "Swasta"
                    elif 'negeri' in status_text:
                        status = "Negeri"
            
            if status == "-":
                page_text = self.get_clean_text(clean_soup).lower()
                if 'perguruan tinggi swasta' in page_text or 'pts' in page_text:
                    status = "Swasta"
                elif 'perguruan tinggi negeri' in page_text or 'ptn' in page_text:
                    status = "Negeri"
            
            # Ekstrak biaya
            text_content = self.get_clean_text(clean_soup)
            biaya_info = self.extract_biaya_from_text(text_content, clean_soup)
            
            # Ekstrak informasi tambahan
            additional_info = self.extract_additional_info(clean_soup, text_content)
            
            data = {
                'nama_kampus': self.safe_value(nama),
                'link_kampus': self.safe_value(kampus_url),
                'akreditasi': self.safe_value(akreditasi),
                'status': self.safe_value(status),
                'alamat': self.safe_value(alamat),
                'kota': self.safe_value(kota),
                'provinsi': self.safe_value(provinsi),
                'website': self.safe_value(website),
                'ranking_webometric': self.safe_value(ranking),
                'biaya_semester_min': self.safe_value(biaya_info['biaya_semester_min']),
                'biaya_semester_max': self.safe_value(biaya_info['biaya_semester_max']),
                'biaya_rata_tahunan': self.safe_value(biaya_info['biaya_rata_tahunan']),
                'biaya_rata_keseluruhan': self.safe_value(biaya_info['biaya_rata_keseluruhan']),
                'uang_pangkal': self.safe_value(biaya_info['uang_pangkal']),
                'biaya_semester': self.safe_value(biaya_info['biaya_semester']),
                'biaya_per_bulan': self.safe_value(biaya_info['biaya_per_bulan']),
                'biaya_per_tahun': self.safe_value(biaya_info['biaya_per_tahun']),
                'biaya_total': self.safe_value(biaya_info['biaya_total']),
                'telepon': self.safe_value(additional_info['telepon']),
                'email': self.safe_value(additional_info['email']),
                'fax': self.safe_value(additional_info['fax']),
                'tahun_berdiri': self.safe_value(additional_info['tahun_berdiri']),
                'jumlah_mahasiswa': self.safe_value(additional_info['jumlah_mahasiswa']),
                'jumlah_dosen': self.safe_value(additional_info['jumlah_dosen']),
                'luas_kampus': self.safe_value(additional_info['luas_kampus']),
                'jenjang_tersedia': self.safe_value(additional_info['jenjang_tersedia']),
                'fasilitas': self.safe_value(additional_info['fasilitas']),
                'beasiswa': self.safe_value(additional_info['beasiswa']),
                'kerjasama': self.safe_value(additional_info['kerjasama']),
                'prestasi': self.safe_value(additional_info['prestasi']),
                'visi': self.safe_value(additional_info['visi']),
                'misi': self.safe_value(additional_info['misi']),
                'provinsi_id': ''
            }
            
            return data
            
        except Exception as e:
            print(f"Error scraping {kampus_url}: {e}")
            return None
    
    def get_study_programs(self, kampus_url):
        """Scraping detail jurusan/prodi"""
        detail_url = kampus_url.rstrip('/') + '/detail-jurusan'
        
        try:
            soup = self.get_soup(detail_url)
            if not soup:
                return []
            
            clean_soup = self.clean_soup_from_scripts(soup)
            
            programs = []
            tables = clean_soup.find_all('table')
            
            for table in tables:
                heading = table.find_previous(['h2', 'h3', 'h4', 'h5', 'div', 'p'])
                fakultas_name = "UNKNOWN"
                
                if heading:
                    heading_text = heading.get_text(strip=True)
                    if 'fakultas' in heading_text.lower() or 'program' in heading_text.lower():
                        fakultas_name = heading_text
                
                rows = table.find_all('tr')
                
                for row in rows:
                    cols = row.find_all('td')
                    
                    if not cols or len(cols) < 2:
                        continue
                    
                    prodi_name = cols[0].get_text(strip=True)
                    akred = cols[1].get_text(strip=True) if len(cols) > 1 else '-'
                    
                    if not prodi_name or prodi_name.lower() in ['program studi', 'jurusan', 'prodi', 'no', 'nama prodi']:
                        continue
                    
                    programs.append({
                        'fakultas': self.safe_value(fakultas_name),
                        'prodi': self.safe_value(prodi_name),
                        'akreditasi_prodi': self.safe_value(akred) if akred and akred != '-' else '-'
                    })
            
            return programs
            
        except Exception as e:
            print(f"Error scraping programs: {e}")
            return []
    
    def scrape_single_kampus(self, kampus_slug):
        """Scrape data lengkap satu kampus (info + jurusan)"""
        print(f"\n{'='*50}")
        print(f"Memproses: {kampus_slug}")
        print(f"{'='*50}")
        
        kampus_url = f"{self.base_url}/cari-kampus/{kampus_slug}"
        
        kampus_info = self.get_university_details(kampus_url)
        time.sleep(1)
        
        jurusan_list = self.get_study_programs(kampus_url)
        time.sleep(1)
        
        return kampus_info, jurusan_list
    
    def validate_data(self, kampus_data):
        """Validasi data yang di-scrape untuk quality check"""
        issues = []
        
        if not kampus_data:
            return ["Data kampus kosong!"]
        
        if kampus_data.get('nama_kampus') == '-':
            issues.append("⚠️  Nama kampus tidak ditemukan")
        
        if kampus_data.get('alamat') == '-':
            issues.append("⚠️  Alamat tidak ditemukan")
        
        if kampus_data.get('akreditasi') == '-':
            issues.append("⚠️  Akreditasi tidak ditemukan")
        
        biaya_fields = ['biaya_semester_min', 'biaya_semester_max', 
                       'biaya_rata_tahunan', 'biaya_rata_keseluruhan']
        
        all_biaya_empty = all(kampus_data.get(field) == '-' for field in biaya_fields)
        if all_biaya_empty:
            issues.append("⚠️  Semua informasi biaya tidak ditemukan")
        
        if kampus_data.get('status') == '-':
            issues.append("⚠️  Status (Negeri/Swasta) tidak ditemukan")
        
        return issues
    
    # def export_summary_report_single_file(self, csv_filename, output_filename='scraping_summary.txt'):
        """Generate laporan summary dari 1 file CSV gabungan"""
        try:
            df = pd.read_csv(csv_filename)
            
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write("="*60 + "\n")
                f.write("LAPORAN HASIL SCRAPING RENCANAMU.ID\n")
                f.write("="*60 + "\n\n")
                
                total_kampus = df['nama_kampus'].nunique()
                total_prodi = len(df)
                
                f.write(f"Total Kampus (Unique): {total_kampus}\n")
                f.write(f"Total Baris Data (Prodi): {total_prodi}\n")
                f.write(f"Rata-rata Prodi per Kampus: {total_prodi/total_kampus:.1f}\n\n")
                
                f.write("="*60 + "\n")
                f.write("BREAKDOWN PER PROVINSI\n")
                f.write("="*60 + "\n")
                prov_counts = df.groupby('provinsi_id')['nama_kampus'].nunique().sort_index()
                for prov_id, count in prov_counts.items():
                    f.write(f"Provinsi {prov_id}: {count} kampus\n")
                
                f.write("\n" + "="*60 + "\n")
                f.write("BREAKDOWN STATUS KAMPUS\n")
                f.write("="*60 + "\n")
                status_counts = df.groupby('status')['nama_kampus'].nunique()
                for status, count in status_counts.items():
                    f.write(f"{status}: {count} kampus\n")
                
                f.write("\n" + "="*60 + "\n")
                f.write("QUALITY CHECK\n")
                f.write("="*60 + "\n")
                
                kampus_unique = df.drop_duplicates(subset='nama_kampus')
                
                missing_akreditasi = (kampus_unique['akreditasi_kampus'] == '-').sum()
                missing_biaya = ((kampus_unique['biaya_semester_min'] == '-') & 
                                (kampus_unique['biaya_rata_tahunan'] == '-')).sum()
                missing_alamat = (kampus_unique['alamat'] == '-').sum()
                
                f.write(f"Kampus tanpa info akreditasi: {missing_akreditasi}\n")
                f.write(f"Kampus tanpa info biaya: {missing_biaya}\n")
                f.write(f"Kampus tanpa alamat lengkap: {missing_alamat}\n")
                
                # Info tambahan yang baru
                if 'telepon' in kampus_unique.columns:
                    missing_telepon = (kampus_unique['telepon'] == '-').sum()
                    f.write(f"Kampus tanpa info telepon: {missing_telepon}\n")
                
                if 'email' in kampus_unique.columns:
                    missing_email = (kampus_unique['email'] == '-').sum()
                    f.write(f"Kampus tanpa info email: {missing_email}\n")
                
                if 'tahun_berdiri' in kampus_unique.columns:
                    missing_tahun = (kampus_unique['tahun_berdiri'] == '-').sum()
                    f.write(f"Kampus tanpa info tahun berdiri: {missing_tahun}\n")
                
                f.write("\n" + "="*60 + "\n")
                f.write("TOP 10 KAMPUS DENGAN PROGRAM STUDI TERBANYAK\n")
                f.write("="*60 + "\n")
                top_prodi = df['nama_kampus'].value_counts().head(10)
                for idx, (kampus, count) in enumerate(top_prodi.items(), 1):
                    f.write(f"{idx}. {kampus}: {count} prodi\n")
                
                f.write("\n" + "="*60 + "\n")
                f.write("TOP 10 FAKULTAS PALING BANYAK\n")
                f.write("="*60 + "\n")
                top_fakultas = df[df['fakultas'] != '-']['fakultas'].value_counts().head(10)
                for idx, (fak, count) in enumerate(top_fakultas.items(), 1):
                    f.write(f"{idx}. {fak}: {count} kali muncul\n")
            
            print(f"\n✓ Laporan summary tersimpan di: {output_filename}")
            
        except Exception as e:
            print(f"Error generating summary: {e}")
    
    # def export_summary_report(self, output_filename='scraping_summary.txt'):
        """Generate laporan summary hasil scraping (legacy untuk 2 file)"""
        self.export_summary_report_single_file('rencanamu.csv', output_filename)
    
    def scrape_all(self, start_prov=1, end_prov=34):
        """Scraping semua data dan save ke SATU CSV gabungan"""
        
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_dir = os.path.join(base_dir, "csv_result")
        os.makedirs(csv_dir, exist_ok=True)
        output_csv = os.path.join(csv_dir, "rencanamu.csv")
        
        # Header CSV gabungan dengan kolom tambahan
        headers = [
            'nama_kampus', 'link_kampus', 'akreditasi_kampus', 'status', 'alamat', 
            'kota', 'provinsi', 'website', 'ranking_webometric', 
            'biaya_semester_min', 'biaya_semester_max', 
            'biaya_rata_tahunan', 'biaya_rata_keseluruhan',
            'uang_pangkal', 'biaya_semester', 'biaya_per_bulan', 
            'biaya_per_tahun', 'biaya_total', 'provinsi_id',
            'telepon', 'email', 'fax', 'tahun_berdiri',
            'jumlah_mahasiswa', 'jumlah_dosen', 'luas_kampus',
            'jenjang_tersedia', 'fasilitas', 'beasiswa',
            'kerjasama', 'prestasi', 'visi', 'misi',
            'fakultas', 'prodi', 'akreditasi_prodi'
        ]
        
        output_file = open(output_csv, 'w', newline='', encoding='utf-8-sig')
        writer = csv.DictWriter(output_file, fieldnames=headers)
        writer.writeheader()
        
        provinces = list(range(start_prov, end_prov + 1))
        total_kampus = 0
        total_prodi = 0
        
        for prov_id in provinces:
            print(f"\n{'='*60}")
            print(f"PROVINSI {prov_id}")
            print(f"{'='*60}")
            
            universities = self.get_universities_by_province(prov_id)
            
            for idx, uni in enumerate(universities, 1):
                kampus_url = uni['link_kampus']
                print(f"\n[{idx}/{len(universities)}] {kampus_url}")
                
                details = self.get_university_details(kampus_url)
                if not details:
                    continue
                
                details['provinsi_id'] = prov_id
                nama_kampus = details.get('nama_kampus', '-')
                
                validation_issues = self.validate_data(details)
                if validation_issues:
                    print(f"  Issues: {len(validation_issues)} masalah ditemukan")
                    for issue in validation_issues:
                        print(f"    {issue}")
                
                print(f"  {nama_kampus}")
                if details.get('biaya_semester_min') != '-':
                    print(f"  Biaya: Rp {details['biaya_semester_min']} - Rp {details['biaya_semester_max']}")
                
                # Tampilkan info tambahan yang berhasil di-scrape
                if details.get('telepon') != '-':
                    print(f"  Telepon: {details['telepon']}")
                if details.get('email') != '-':
                    print(f"  Email: {details['email']}")
                if details.get('tahun_berdiri') != '-':
                    print(f"  Tahun Berdiri: {details['tahun_berdiri']}")
                if details.get('jenjang_tersedia') != '-':
                    print(f"  Jenjang: {details['jenjang_tersedia']}")
                
                programs = self.get_study_programs(kampus_url)
                print(f"  {len(programs)} program studi")
                
                details['akreditasi_kampus'] = details.pop('akreditasi')
                
                if programs:
                    for prog in programs:
                        row = {**details, **prog}
                        writer.writerow(row)
                        total_prodi += 1
                else:
                    row = {**details, 'fakultas': '-', 'prodi': '-', 'akreditasi_prodi': '-'}
                    writer.writerow(row)
                
                output_file.flush()
                total_kampus += 1
                time.sleep(2)
            
            time.sleep(3)
        
        output_file.close()
        
        print(f"\n{'='*60}")
        print(f"SELESAI")
        print(f"{'='*60}")
        print(f"Total kampus: {total_kampus}")
        print(f"Total prodi (baris): {total_prodi}")
        print(f"Data lengkap: {output_csv}")
        
        self.export_summary_report_single_file(output_csv)
    
    def save_to_csv(self, kampus_data, jurusan_data, kampus_filename='kampus_data_single.csv'):
        """Simpan data ke 1 file CSV gabungan"""
        if not kampus_data:
            print("❌ Tidak ada data kampus!")
            return
        
        kampus_data['akreditasi_kampus'] = kampus_data.pop('akreditasi')
        
        all_rows = []
        
        if jurusan_data:
            for prodi in jurusan_data:
                row = {**kampus_data, **prodi}
                all_rows.append(row)
        else:
            kampus_data['fakultas'] = '-'
            kampus_data['prodi'] = '-'
            kampus_data['akreditasi_prodi'] = '-'
            all_rows.append(kampus_data)
        
        df = pd.DataFrame(all_rows)
        df.to_csv(kampus_filename, index=False, encoding='utf-8-sig')
        
        print(f"\n✓ Data tersimpan di: {kampus_filename}")
        print(f"  Total baris: {len(all_rows)}")
        if jurusan_data:
            print(f"  Total prodi: {len(jurusan_data)}")


def scrape_multiple_kampus(kampus_list):
    """Scrape beberapa kampus sekaligus ke 1 file CSV"""
    scraper = RencanamuScraper()
    
    all_rows = []
    
    for slug in kampus_list:
        try:
            kampus_info, jurusan_list = scraper.scrape_single_kampus(slug)
            
            if not kampus_info:
                continue
            
            kampus_info['akreditasi_kampus'] = kampus_info.pop('akreditasi')
            
            if jurusan_list:
                for prodi in jurusan_list:
                    row = {**kampus_info, **prodi}
                    all_rows.append(row)
            else:
                kampus_info['fakultas'] = '-'
                kampus_info['prodi'] = '-'
                kampus_info['akreditasi_prodi'] = '-'
                all_rows.append(kampus_info)
            
            time.sleep(2)
            
        except Exception as e:
            print(f"Error processing {slug}: {e}")
            continue
    
    if all_rows:
        df = pd.DataFrame(all_rows)
        df.to_csv('all_kampus_prodi.csv', index=False, encoding='utf-8-sig')
        
        total_kampus = df['nama_kampus'].nunique()
        total_prodi = len(df)
        
        print(f"\n✓ Data tersimpan di: all_kampus_prodi.csv")
        print(f"  Total kampus: {total_kampus}")
        print(f"  Total baris (prodi): {total_prodi}")


if __name__ == "__main__":
    scraper = RencanamuScraper()
    
    print("="*60)
    print("RENCANAMU.ID SCRAPER - ENHANCED VERSION")
    print("="*60)
    
    scraper.scrape_all()
    
    print("\n✓ Selesai!")