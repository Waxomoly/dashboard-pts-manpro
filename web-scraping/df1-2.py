#dataframe 1
# 1. institution_code (act as primary key, increasing integer) | email | institution_name | akreditasi_institusi | link


# df1 (institutions) coba ke 2
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

links_info = []  # simpan nama + link

# Tahap 1: Ambil semua link dari 31 halaman
for page in range(1, 32):
    driver.get(list_url + str(page))
    time.sleep(2)  # tunggu halaman load
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

driver.quit()

# Tahap 2: Loop ke setiap halaman detail
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

# Convert ke DataFrame
df_institutions = pd.DataFrame(institutions)
print(df_institutions.head())
print(f"Total institutions: {len(df_institutions)}")

# Simpan ke CSV
df_institutions.to_csv("institutions.csv", index=False)
