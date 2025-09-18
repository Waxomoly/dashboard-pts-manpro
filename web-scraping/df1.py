# 1. institution_code (act as primary key, increasing integer) | email | institution_name | akreditasi_institusi | link

import requests
from bs4 import BeautifulSoup
import pandas as pd

base_url = "https://lldikti7.kemdikbud.go.id"
list_url = f"{base_url}/rekap-data-pts?page="

institutions = []
institution_id = 1

# Loop beberapa halaman (misalnya 1–31)
for page in range(1, 32):
    url = list_url + str(page)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    rows = soup.select("table#ptTable tbody tr")
    for row in rows:
        cols = row.find_all("td")
        if not cols:
            continue

        # Nama kampus
        name = cols[1].get_text(strip=True)

        # Link detail
        detail_link = cols[4].a["href"]

        # --- buka halaman detail institusi ---
        r_detail = requests.get(detail_link)
        soup_detail = BeautifulSoup(r_detail.text, "html.parser")

        # Ambil email
        email_tag = soup_detail.find("a", href=lambda x: x and "mailto:" in x)
        email = email_tag.get_text(strip=True) if email_tag else None

        # Ambil akreditasi institusi
        akreditasi_inst = None
        akreditasi_tag = soup_detail.find(string="Akreditasi Institusi")
        if akreditasi_tag:
            td_tag = akreditasi_tag.find_next("td")
            if td_tag:
                akreditasi_inst = td_tag.get_text(strip=True)

        # Simpan ke list
        institutions.append({
            "institution_code": institution_id,
            "institution_name": name,
            "email": email,
            "akreditasi_institusi": akreditasi_inst,
            "link": detail_link
        })

        institution_id += 1

# Convert ke DataFrame
df_institutions = pd.DataFrame(institutions)

# Preview
print(df_institutions.head())

# Simpan ke Excel
# df_institutions.to_excel("data_institusi.xlsx", index=False)
