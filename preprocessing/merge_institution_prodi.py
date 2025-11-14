import pandas as pd
import helpers.csv_crud as csv_crud

# BASE_PATH = "csv_result/"
df_prodi = csv_crud.read_csv_file("merged_prodi.csv")
df_institution = csv_crud.read_csv_file("merged_institutions.csv")

df_quipper_map = df_institution[['quipper_code', 'institution_code']].copy()
df_rencanamu_map = df_institution[['rencanamu_code', 'institution_code']].copy()
df_pddikti_map = df_institution[['pddikti_code', 'institution_code']].copy()
df_banpt_map = df_institution[['banpt_code', 'institution_code']].copy()

# clean the dfs
df_quipper_map = df_quipper_map[df_quipper_map['quipper_code'] != '-']
df_rencanamu_map = df_rencanamu_map[df_rencanamu_map['rencanamu_code'] != '-']
df_pddikti_map = df_pddikti_map[df_pddikti_map['pddikti_code'] != '-']
df_banpt_map = df_banpt_map[df_banpt_map['banpt_code'] != '-']

df_prodi['institution_code'] = -1

quipper_dict = df_quipper_map.set_index('quipper_code')['institution_code'].to_dict()
rencanamu_dict = df_rencanamu_map.set_index('rencanamu_code')['institution_code'].to_dict()
pddikti_dict = df_pddikti_map.set_index('pddikti_code')['institution_code'].to_dict()
banpt_dict = df_banpt_map.set_index('banpt_code')['institution_code'].to_dict()


def get_institution_code(row):
    if row['quipper_code'] != '-':
        return quipper_dict.get(row['quipper_code'], -1)
    elif row['rencanamu_code'] != '-':
        return rencanamu_dict.get(row['rencanamu_code'], -1)
    elif row['pddikti_code'] != '-':
        return pddikti_dict.get(row['pddikti_code'], -1)
    elif row['banpt_code'] != '-':
        return banpt_dict.get(row['banpt_code'], -1)
    else:
        return -1


df_prodi['institution_code'] = df_prodi.apply(get_institution_code, axis=1)
print(f"Jumlah prodi sebelum matching: {len(df_prodi)}")
df_prodi = df_prodi[df_prodi['institution_code'] != -1]
print(f"Jumlah prodi setelah matching: {len(df_prodi)}")

# df_prodi.to_csv(BASE_PATH + 'merged_prodi_final.csv', index=False, encoding='utf-8-sig')
csv_crud.save_csv_file(df_prodi, 'merged_prodi_final.csv')
print(f"\nFile final 'merged_prodi_final.csv' telah dibuat dengan institution_code yang sesuai.")