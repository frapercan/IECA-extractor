import os

import requests
import pandas as pd
import itertools
import numpy as np


class Jerarquia:
    """Estructura de datos para manejar las jerarquias encontradas dentro
    de las consultas del IECA, es necesario hacer una petición HTTP para expandir la jerarquia
    y traernos los valores de las listas de código pertinentes.

        :param jerarquia: Diccionario con los metadatos de la jerarquia.
        :type jerarquia: JSON
        """

    def __init__(self, jerarquia):

        self.id_jerarquia = f'{jerarquia["alias"]}_{jerarquia["cod"]}'
        self.jerarquia_completa = requests.get(jerarquia['url']).json()

        self.datos = self.obtener_dataframe()

        self.metadatos = jerarquia
        self.guardar_datos()

    def obtener_dataframe(self, propiedades=('id', 'label', 'des', 'parentId')):
        """Transforma el arbol de la jerarquia hacia un formato tabular,
          extrayendo las propiedades seleccionadas.

          Devuelve un cuadro de datos con la jerarquia.

         :returns:  dict(metadatos),list(Jerarquia),dict(measures),dict(data).
         """
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

        return jerarquia_df


    def guardar_datos(self,directorio= 'iecasdmx/sistema_informacion/BADEA/jerarquias/'):
        self.datos.to_csv(f'{os.path.join(directorio,self.id_jerarquia)}.csv')
