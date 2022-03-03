import os
import sys

import requests
import pandas as pd
import itertools
import numpy as np

import logging

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Jerarquia:
    """Estructura de datos para manejar las jerarquias encontradas dentro
    de las consultas del IECA, es necesario hacer una petición HTTP para expandir la jerarquia
    y traernos los valores de las listas de código pertinentes.

        :param jerarquia: Diccionario con los metadatos de la jerarquia.
        :type jerarquia: JSON
        """

    def __init__(self, jerarquia):
        self.id_jerarquia = jerarquia["alias"]

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info('Extrayendo listas de código - %s', self.id_jerarquia)

        self.logger.info('Petición API')
        self.jerarquia_completa = requests.get(jerarquia['url']).json()
        self.logger.info('Petición API Finalizada')

        self.datos = self.obtener_dataframe()

        self.metadatos = jerarquia

    def obtener_dataframe(self, propiedades=('id', 'cod', 'label', 'des', 'parentId', 'order')):
        """Transforma el arbol de la jerarquia hacia un formato tabular,
          extrayendo las propiedades seleccionadas.

          Devuelve un cuadro de datos con la jerarquia.

         :returns:  dict(metadatos),list(Jerarquia),dict(measures),dict(data).
         """
        self.logger.info('Transformando Jerarquias')
        data = [self.jerarquia_completa['data']]

        def recorrer_arbol_recursivamente(datos_jerarquia):
            datos_nivel_actual = [[jerarquia[propiedad] for propiedad in propiedades]
                                  for jerarquia in datos_jerarquia]

            es_ultimo_nivel_rama = np.all(
                [jerarquia['children'] == [] or jerarquia['isLastLevel'] for jerarquia in datos_jerarquia])
            if es_ultimo_nivel_rama:
                return datos_nivel_actual

            return datos_nivel_actual + list(itertools.chain(
                *[recorrer_arbol_recursivamente(jerarquia['children']) for jerarquia in datos_jerarquia]))

        datos_jerarquia = recorrer_arbol_recursivamente(data)

        jerarquia_df = pd.DataFrame(datos_jerarquia, columns=propiedades, dtype='string')
        jerarquia_df.replace('null', '', inplace=True)
        jerarquia_df.drop_duplicates('cod', keep='first', inplace=True)
        self.logger.info('Jerarquia transformada')

        return jerarquia_df

    def guardar_datos(self, directorio='iecasdmx/sistema_informacion/BADEA/jerarquias/'):
        self.logger.info('Almacenando datos Jerarquia: %s', self.id_jerarquia)
        columnas = ['ID', 'COD', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']
        columnas_sdmx = ['ID', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']
        if not os.path.exists(directorio):
            os.makedirs(directorio)
        datos = self.datos.__deepcopy__()
        datos.columns = columnas
        datos_sdmx = datos[columnas_sdmx]
        datos.to_csv(f'{os.path.join(directorio, self.id_jerarquia)}.csv', sep=';', index=False)
        datos_sdmx.to_csv(f'{os.path.join(directorio, self.id_jerarquia)}_sdmx.csv', sep=';', index=False)
        self.logger.info('Jerarquia %s Almacenada', self.id_jerarquia)
