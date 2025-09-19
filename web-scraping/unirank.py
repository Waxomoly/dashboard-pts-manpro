from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time

# URL target
url = 'https://www.unirank.org/id/east-java/'

# Setup Chrome
options = Options()
options.add_experimental_option("detach", True)  # biar Chrome nggak auto-close
driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

# Buka halaman
driver.get(url)

# Tunggu sebentar biar halaman kebuka penuh
time.sleep(3)

# Ambil HTML
html_content = driver.page_source
soup = BeautifulSoup(html_content, 'html.parser')

# Cari tabel utama
table = soup.find("table", {"class": "table"})
rows = table.find_all("tr")

data = []
primary_key = 1

# Loop baris tabel (skip header)
for row in rows[1:]:
    cols = row.find_all("td")
    if len(cols) >= 2:
        rank = cols[0].get_text(strip=True)
        institution_name = cols[1].get_text(strip=True)

        data.append([primary_key, institution_name, rank])
        primary_key += 1

# Buat DataFrame
df = pd.DataFrame(data, columns=["primary_key", "institution_name", "rank"])
print(df)

# Simpan ke CSV
df.to_csv("unirank_east_java.csv", index=False, encoding="utf-8-sig")
print("✅ Data berhasil disimpan ke unirank_east_java.csv")

# Tutup browser
driver.quit()
