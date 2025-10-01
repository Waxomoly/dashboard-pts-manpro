# link: https://lldikti7.kemdikbud.go.id/rekap-data-pts
# hasil akhir adalah 2 dataframe dengan kolom: 
# 1. institution_code (act as primary key, increasing integer) | email | institution_name | akreditasi_institusi | link
<<<<<<< HEAD
# 2. prodi_code (act as primary key, increasing integer) | prodi_name | jenjang | akreditasi_prodi | institution_code (sama kyk yg di dataframe pertama)


# --- import library ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import requests

# --- Setup Selenium ---
options = Options()
options.add_experimental_option("detach", True)  # biar ga langsung close
driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

base_url = "https://lldikti7.kemdikbud.go.id"
list_url = f"{base_url}/rekap-data-pts?page="

# --------------------
# BAGIAN DF1 - Institutions
# --------------------
links_info = []  # simpan nama + link

for page in range(1, 32):  # total 31 halaman
    driver.get(list_url + str(page))
    time.sleep(2)
    print(f"Scraping page {page}...")

    rows = driver.find_elements(By.CSS_SELECTOR, "table#ptTable tbody tr")
    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if not cols:
            continue

        name = cols[1].text.strip()
        href = cols[4].find_element(By.TAG_NAME, "a").get_attribute("href")
        detail_link = href if href.startswith("http") else base_url + href
        links_info.append((name, detail_link))

# tutup selenium sementara (biar hemat resource)
driver.quit()

institutions = []
for institution_id, (name, detail_link) in enumerate(links_info, start=1):
    try:
        resp = requests.get(detail_link)
        soup_detail = BeautifulSoup(resp.text, "html.parser")

        email = None
        akreditasi_inst = None

        detail_box = soup_detail.find("div", class_="details-container")
        if detail_box:
            for p in detail_box.find_all("p"):
                strong = p.find("strong")
                if not strong:
                    continue
                label = strong.get_text(strip=True).replace(":", "")
                value = p.get_text(strip=True).replace(strong.get_text(strip=True), "").strip()

                if label == "Alamat Email":
                    email = value
                elif "Akreditasi" in label:
                    akreditasi_inst = value

        institutions.append({
            "institution_code": institution_id,
            "institution_name": name,
            "email": email,
            "akreditasi_institusi": akreditasi_inst,
            "link": detail_link
        })

        print(f"Scraped: {institution_id} | {name} | {email} | {akreditasi_inst}")

    except Exception as e:
        print(f"Error scraping {name}: {e}")

# Convert ke DataFrame df1
df_institutions = pd.DataFrame(institutions)
df_institutions.to_csv("institutions.csv", index=False)
print(f"Total institutions: {len(df_institutions)}")


# BAGIAN DF2 - prodi

# buka ulang selenium untuk scrape prodi
driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

prodi_list = []
prodi_code = 1

for _, row in df_institutions.iterrows():
    institution_code = row["institution_code"]
    detail_link = row["link"]

    driver.get(detail_link)
    time.sleep(1)

    soup_detail = BeautifulSoup(driver.page_source, "html.parser")

    prodi_table = soup_detail.find("table", class_="table table-bordered")
    if not prodi_table:
        continue

    rows = prodi_table.find("tbody").find_all("tr")
    for row_tr in rows:
        cols = [c.get_text(strip=True) for c in row_tr.find_all("td")]
        if len(cols) < 4:
            continue

        prodi_list.append({
            "prodi_code": prodi_code,
            "prodi_name": cols[1],
            "jenjang": cols[2],
            "akreditasi_prodi": cols[3],
            "institution_code": institution_code
        })
        prodi_code += 1

df_prodi = pd.DataFrame(prodi_list)
df_prodi.to_csv("prodi.csv", index=False)
print(f"Total prodi scraped: {len(df_prodi)}")

driver.quit()
=======
# 2. prodi_code (act as primary key, increasing integer) | prodi_name | jenjang | akreditasi_prodi | institution_code (sama kyk yg di dataframe pertama)
>>>>>>> 58e1cc38c3fdc8ca9ee0e003b19c9f1c5fe01f4d
