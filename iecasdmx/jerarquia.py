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

    def __init__(self, jerarquia, configuracion_global, actividad):
        self.configuracion_global = configuracion_global
        self.actividad = actividad
        self.metadatos = jerarquia
        self.id_jerarquia = self.metadatos["alias"] + '-' + self.metadatos['cod']
        self.logger = logging.getLogger(f'{self.__class__.__name__} [{self.id_jerarquia}]')

        self.datos = self.solicitar_informacion_jerarquia()

        self.logger.info('Extrayendo lista de código')

    def convertir_jerarquia_a_dataframe(self, datos_jerarquia,
                                        propiedades=('id', 'cod', 'label', 'des', 'parentId', 'order')):
        """Transforma el arbol de la jerarquia hacia un formato tabular,
          extrayendo las propiedades seleccionadas.

          Devuelve un cuadro de datos con la jerarquia.

         :returns:  dict(metadatos),list(Jerarquia),dict(measures),dict(data).
         """
        self.logger.info('Transformando Jerarquias')
        data = [datos_jerarquia['data']]

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
        datos_jerarquia.append(['_Z', 'No aplica', 'No aplica', 'No aplica', 'null', 'null'])

        jerarquia_df = pd.DataFrame(datos_jerarquia, columns=[propiedad.upper() for propiedad in propiedades],
                                    dtype='string')

        jerarquia_df.replace('null', '', inplace=True)
        jerarquia_df.drop_duplicates('COD', keep='first', inplace=True)
        self.logger.info('Jerarquia transformada')

        return jerarquia_df

    def guardar_datos(self):
        directorio = os.path.join(self.configuracion_global['directorio_jerarquias'], self.actividad)
        directorio_original = os.path.join(directorio, 'original')
        if not os.path.exists(directorio_original):
            os.makedirs(directorio_original)

        directorio_sdmx = os.path.join(directorio, 'sdmx')
        if not os.path.exists(directorio_sdmx):
            os.makedirs(directorio_sdmx)
        self.logger.info('Almacenando datos Jerarquia')
        columnas = ['ID', 'COD', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']
        columnas_sdmx = ['ID', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']

        datos = self.datos.__deepcopy__()
        datos.columns = columnas
        datos_sdmx = datos[columnas_sdmx]
        datos.to_csv(f'{os.path.join(directorio_original, self.id_jerarquia)}.csv', sep=';', index=False)
        datos_sdmx.to_csv(f'{os.path.join(directorio_sdmx, self.id_jerarquia)}.csv', sep=';', index=False)
        self.logger.info('Jerarquia Almacenada')

    def solicitar_informacion_jerarquia(self):
        directorio_csv = os.path.join(self.configuracion_global['directorio_jerarquias'], self.actividad, 'original',
                                      self.id_jerarquia + '.csv')
        datos = None
        try:
            self.logger.info('Buscando el CSV de la jerarquia en local')
            with open(directorio_csv, 'r', encoding='utf-8') as csv_file:
                datos = pd.read_csv(csv_file, sep=';', dtype='string')
                self.logger.info('CSV leido correctamente')
        except Exception as e:
            self.logger.warning('No se ha encontrado el fichero %s', directorio_csv)
            self.logger.warning('Excepción: %s', e)
            self.logger.info('Iniciando peticion a la API del IECA')
            datos = self.convertir_jerarquia_a_dataframe(requests.get(self.metadatos['url']).json())
            self.logger.info('Petición API Finalizada')

        finally:
            if datos is not None:
                self.logger.info('Datos alcanzados correctamente')
            else:
                self.logger.warning('No hay información disponible')
        return datos
