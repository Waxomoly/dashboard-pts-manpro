import pandas as pd
import re

# constants
BASE_PATH = "csv_result/"

# 1. Load data
df = pd.read_csv(BASE_PATH + "unirank_nasional.csv")

# 2. UPPERCASE semua text di seluruh kolom object
df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)

# 3. Hapus semua kurung dan isinya
def remove_parentheses(text):
    return re.sub(r"\(.*?\)", "", text).strip()

df = df.applymap(lambda x: remove_parentheses(x) if isinstance(x, str) else x)

# 4. TRIM spasi berlebih
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# 5. Hapus duplikat (baris yang identik seluruh kolomnya)
df = df.drop_duplicates()

# 6. Cari kolom NULL / kosong
null_columns = df.columns[df.isnull().any()].tolist()
if null_columns:
    print(" Ada kolom NULL:", null_columns)
else:
    print("✅ Tidak ada kolom NULL")

# 7. Simpan hasil bersih ke file baru
df.to_csv(BASE_PATH +"unirank_nasional_clean.csv", index=False)
print("🎉 Preprocessing selesai! Data disimpan di unirank_nasional_clean.csv")
