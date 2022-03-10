import pandas as pd
from os import listdir
from os.path import isfile, join,isdir

if __name__ == "__main__":
    directorio = 'sistema_informacion/BADEA/datos/DEFCAU'
    directorios_datos = [f for f in listdir(directorio) if isdir(join(directorio, f))]
    for directorio_datos  in directorios_datos :
        if '2' in directorio_datos and listdir(join(directorio,directorio_datos)):
            ficheros_fusionar = listdir(join(directorio,directorio_datos))
            cuadros_de_datos_fusionados = pd.concat([pd.read_csv(join(directorio,directorio_datos,fichero),dtype='string',index_col=None) for fichero in ficheros_fusionar])
            try:
                cuadros_de_datos_fusionados.to_csv(join(directorio,directorio_datos,directorio_datos)+'.csv',index=False)
            except Exception as e:
                print(e)
        # directorios_datos = [f for f in listdir(directorio_datos) if isfile(join(directorio,directorio_datos, f))]
        # print(directorios_datos)

    
