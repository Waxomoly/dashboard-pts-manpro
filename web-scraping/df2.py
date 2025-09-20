# 2. prodi_code (act as primary key, increasing integer) | prodi_name | jenjang | akreditasi_prodi | institution_code (sama kyk yg di dataframe pertama)

# df 2 

# sama kyk df 1
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


##### DATA FRAME 2 PRODI

# Load institutions.csv dari df1
df_institutions = pd.read_csv("institutions.csv")

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

