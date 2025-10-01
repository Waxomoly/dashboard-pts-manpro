import pandas as pd
import re

# Load CSV
df = pd.read_csv("prodi.csv")

# Uppercase semua text di setiap kolom bertipe string
df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)

# Hapus semua kurung dan isinya
df = df.applymap(lambda x: re.sub(r"\(.*?\)", "", x).strip() if isinstance(x, str) else x)

# RIM (hapus spasi berlebih)
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Drop duplikat (jika semua kolom sama persis)
df = df.drop_duplicates()

# Cek kolom null
null_info = df.isnull().sum()
null_columns = null_info[null_info > 0]

# Ganti semua nilai NULL dengan "-"
df = df.fillna("-")

# Ganti string kosong ("") juga jadi "-"
df = df.replace(r'^\s*$', '-', regex=True)

print("Kolom dengan nilai NULL/kosong:")
print(null_columns)

# Simpan
df.to_csv("prodi_clean.csv", index=False)
