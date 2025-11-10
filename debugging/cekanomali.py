import pandas as pd

BASE_PATH = "csv_result/"
df = pd.read_csv(BASE_PATH + "merged_institutions.csv")

fee_columns = [
    'average_semester_fee', 'starting_semester_fee', 'ending_semester_fee',
    'average_yearly_fee', 'starting_yearly_fee', 'ending_yearly_fee'
]


df_res = df[(df[fee_columns].abs() > 999999999).any(axis=1) | (df[fee_columns] < 100000).any(axis=1)]

# print(len(df_res))

# df_res = df[df['province'].str.contains(',', na=False)]
# print(len(df_res))

df_res.to_csv('./debug_anomali_harga.csv', index=False, encoding='utf-8-sig')