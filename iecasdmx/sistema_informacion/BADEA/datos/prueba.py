import pandas as pd

datos = pd.read_csv('DEFCAU/2/2.csv', sep=';', dtype='string')


print(len(datos))
columnas = [columna  for columna in datos.columns if columna != 'OBS_VALUE']
print(len(datos.drop_duplicates(subset=columnas)))
