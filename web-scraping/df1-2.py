#dataframe 1
# 1. institution_code (act as primary key, increasing integer) | email | institution_name | akreditasi_institusi | link


# df1 coba ke 2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time

# --- Setup Selenium ---
options = Options()
options.add_experimental_option("detach", True)  # biar ga langsung close
driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

base_url = "https://lldikti7.kemdikbud.go.id"
list_url = f"{base_url}/rekap-data-pts?page="

institutions = []
links_info= []
institution_id = 1

# Loop semua halaman
for page in range(1, 32):
    driver.get(list_url + str(page))
    time.sleep(2)  # tunggu halaman utama load
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
        
# Sekarang baru loop ke setiap detail_link
for institution_id, (name, detail_link) in enumerate(links_info, start=1):
    driver.get(detail_link)
    time.sleep(1)
    try:
        soup_detail = BeautifulSoup(driver.page_source, "html.parser")
        # ambil email & akreditasi
        email_tag = soup_detail.find("a", href=lambda x: x and "mailto:" in x)
        email = email_tag.get_text(strip=True) if email_tag else None

        # ambil akreditasi institusi
        akreditasi_inst = None
        akreditasi_tag = soup_detail.find(string="Akreditasi Institusi")
        if akreditasi_tag:
            td = akreditasi_tag.find_next("td")
            if td:
                akreditasi_inst = td_next.get_text(strip=True)
        
        # --- Simpan ---
        institutions.append({
            "institution_code": institution_id,
            "institution_name": name,
            "email": email,
            "akreditasi_institusi": akreditasi_inst,
            "link": detail_link
        })

        print(f"Scraped: {name} | {email} | {akreditasi_inst}")

    except Exception as e:
        print(f"Error scraping {name}: {e}")
    

        # # --- Ambil email ---
        # email_tag = soup_detail.find("a", href=lambda x: x and "mailto:" in x)
        # email = email_tag.get_text(strip=True) if email_tag else None

        # # --- Ambil akreditasi institusi ---
        # akreditasi_inst = None
        # akreditasi_tag = soup_detail.find(string=lambda text: text and "Akreditasi Institusi" in text)
        # if akreditasi_tag:
        #     td_next = akreditasi_tag.find_next("td")
        #     if td_next:
        #         akreditasi_inst = td_next.get_text(strip=True)

        
        # print(f"Scraped: {name}")
        # institution_id += 1

# Convert ke DataFrame
df_institutions = pd.DataFrame(institutions)
print(df_institutions.head())
print(f"Total institutions: {len(df_institutions)}")

# Simpan ke CSV
df_institutions.to_csv("institutions.csv", index=False)


driver.quit()

