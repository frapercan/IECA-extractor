import os
import subprocess
import sys
import glob

sys.stdout.reconfigure(encoding='utf-8')

dsds_directorios = glob.glob("C:/Users/index/OneDrive/Documentos/DSD_s/*.xml")
cubos_directorios = glob.glob("C:/Users/index/OneDrive/Documentos/DataFlows/*")

destino_ficheros = "C:/Users/index/OneDrive/Documentos/RESULTADOS_STRUVAL/"

if not os.path.exists(destino_ficheros):
    os.makedirs(destino_ficheros)
#
for directorio_dsd,directorio_cubo in zip(dsds_directorios,cubos_directorios):




    for cubo in glob.glob(os.path.join(directorio_cubo,'*.xml')):
        print(directorio_dsd)
        print(cubo)
        dsd = directorio_dsd.replace('\\','/')
        cubo = cubo.replace('\\','/')

        result = []
        win_cmd = f'cd C:/Users/index/OneDrive/Documentos/struval_v3_externalaccesstutorial/ExternalAccessTutorial/ & runClient.cmd {cubo} {dsd} SDMX_ML '
        process = subprocess.Popen(win_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE )
#
        destino_ficheros_categoria = destino_ficheros + '/'+dsd.split('/')[-1].split('+')[0][4:]
        destino_ficheros_categoria = destino_ficheros_categoria.replace('\\','/')
        if not os.path.exists(destino_ficheros_categoria):
            os.makedirs(destino_ficheros_categoria)

        fichero_destino = destino_ficheros_categoria + '/' + cubo.split('/')[-1].split('+')[0]+'.txt'
        fichero_destino = fichero_destino.replace('\\','/')
        fichero_destino = fichero_destino.replace('//','/')



        # if os.path.exists():
        #     os.makedirs()
        with open(fichero_destino,'w') as file:
            for line in process.stdout:
                file.write(line.decode("utf-8") )

