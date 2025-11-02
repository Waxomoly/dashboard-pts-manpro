import pandas as pd
from googletrans import Translator, constants
translator = Translator()

BASE_PATH = "csv_result/"
df = pd.read_csv(BASE_PATH + "merged_prodi.csv")

df['prodi'].value_counts().to_csv('./debugging/prodi_count.csv', index=True, encoding='utf-8-sig')