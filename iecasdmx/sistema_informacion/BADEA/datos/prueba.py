import pandas as pd

datos = pd.read_csv('64582.csv',sep=';',dtype='string')


unique = datos['D_EDAD_0'].unique()
print(unique)