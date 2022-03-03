import pandas as pd

jerarquia = pd.read_csv('D_CNAE09_0_sdmx.csv', sep=';', dtype='string')
# print(jerarquia)

union = jerarquia['PARENTCODE'][~jerarquia['PARENTCODE'].isin(jerarquia['ID'])]
print(union)
#
# counts = jerarquia['ID'].value_counts()
# print(counts.max())
