# link: https://lldikti7.kemdikbud.go.id/rekap-data-pts
# hasil akhir adalah 2 dataframe dengan kolom: 
# 1. institution_code (act as primary key, increasing integer) | email | institution_name | akreditasi_institusi | link
# 2. prodi_code (act as primary key, increasing integer) | prodi_name | jenjang | akreditasi_prodi | institution_code (sama kyk yg di dataframe pertama)


import requests
from bs4 import BeautifulSoup
import pandas as pd

base_url = "https://lldikti7.kemdikbud.go.id"
list_url = f"{base_url}/rekap-data-pts?page="

institutions = []
prodi_list = []
institution_id = 1
prodi_id = 1

# Loop beberapa halaman (misalnya 1-31)
for page in range(1, 32):
    url = list_url + str(page)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    rows = soup.select("table#ptTable tbody tr")
    for row in rows:
        cols = row.find_all("td")
        if not cols:
            continue

        name = cols[1].get_text(strip=True)
        detail_link = cols[4].a["href"]

        # --- buka halaman detail ---
        r_detail = requests.get(detail_link)
        soup_detail = BeautifulSoup(r_detail.text, "html.parser")

        # ambil email & akreditasi institusi (cari sesuai struktur di detail page)
        email_tag = soup_detail.find("a", href=lambda x: x and "mailto:" in x)
        email = email_tag.get_text(strip=True) if email_tag else None

        akreditasi_inst = None
        akreditasi_tag = soup_detail.find(string="Akreditasi Institusi")
        if akreditasi_tag:
            akreditasi_inst = akreditasi_tag.find_next("td").get_text(strip=True)

        # simpan ke dataframe institusi
        institutions.append({
            "institution_code": institution_id,
            "institution_name": name,
            "email": email,
            "akreditasi_institusi": akreditasi_inst,
            "link": detail_link
        })

        # --- ambil tabel prodi ---
       # ambil semua tabel di halaman detail
    tables = soup_detail.find_all("table")

# biasanya tabel prodi ada di urutan kedua (cek di inspect element)
    if len(tables) > 1:
         prodi_table = tables[1]  # ambil tabel ke-2
    else:
        prodi_table = None

        if prodi_table:
            for prow in prodi_table.select("tbody tr"):
                pcols = prow.find_all("td")
                if not pcols:
                    continue
                prodi_name = pcols[1].get_text(strip=True)
                jenjang = pcols[2].get_text(strip=True)
                akreditasi_prodi = pcols[3].get_text(strip=True)

                prodi_list.append({
                    "prodi_code": prodi_id,
                    "prodi_name": prodi_name,
                    "jenjang": jenjang,
                    "akreditasi_prodi": akreditasi_prodi,
                    "institution_code": institution_id
                })
                prodi_id += 1

        institution_id += 1

# Convert ke DataFrame
# df_institutions = pd.DataFrame(institutions)
df_prodi = pd.DataFrame(prodi.csv)

# print(df_institutions.head())
print(df_prodi.head())
