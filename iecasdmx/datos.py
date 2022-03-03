import os
import sys

import pandas as pd

import logging
import numpy as np

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Datos:
    """Estructura de datos para manejar los datos encontrados dentro
    de las consultas del IECA. El proceso de inicialización de esta estructura
    realiza los siguientes pasos:
    #. Convierte de JSON a DataFrame utilizando las medidas y jerarquias para las columnas
    #. Desacopla las observaciones en base a las medidas
    #. Añade la dimension FREQ de SDMX
    :param id_consulta: ID de la consulta de BADEA.
    :type id_consulta: str
    :param configuracion: Información con respecto a las configuraciones del procesamiento.
    :type configuracion: JSON
    :param periodicidad: Periodicidad de la consulta de BADEA
    :type periodicidad: str
    :param datos: Diccionario con los datos de la consulta de BADEA.
    :type datos: JSON
    :param jerarquias: Jerarquias utilizadas en la consulta de BADEA.
    :type jerarquias: :class:'iecasdmx.Jerarquia'
    :param medidas: Diccionario con las medidas de la consulta de BADEA.
    :type medidas: JSON
    """

    def __init__(self, id_consulta, configuracion, periodicidad, datos, jerarquias, medidas):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.id_consulta = id_consulta
        self.configuracion = configuracion
        self.periodicidad = periodicidad
        self.jerarquias = jerarquias
        self.medidas = medidas

        self.logger.info('Procesando las Observaciones de Consulta: %s con periodicidad: %s',
                         self.id_consulta, self.periodicidad)
        self.datos = self.datos_a_dataframe(datos)
        self.datos_por_observacion = self.desacoplar_datos_por_medidas()
        insertar_freq(self.datos_por_observacion, self.periodicidad)
        self.logger.info('Finalización Procesamiento Observaciones de Consulta - %s', self.id_consulta)

    def datos_a_dataframe(self, datos):
        self.logger.info('Transformando los datos JSON a DataFrame')

        columnas_jerarquia = [jerarquia.metadatos['alias'] for jerarquia in self.jerarquias]
        columnas_medida = [medida['des'] for medida in
                           self.medidas]
        columnas = [jerarquia.metadatos['alias'] for jerarquia in self.jerarquias] + [medida['des'] for medida in
                                                                                      self.medidas]
        try:
            df = pd.DataFrame(datos, columns=columnas)
        except Exception as e:
            self.logger.error('Consulta sin datos - %s', self.id_consulta)
            raise e

        df.columns = columnas
        df[columnas_jerarquia] = df[columnas_jerarquia].applymap(lambda x: x['cod'][-1])
        print(df[columnas_medida].shape,df[columnas_medida].applymap(
            lambda x: x['val']).shape)
        df[columnas_medida] = df[columnas_medida].applymap(
            lambda x: x['val'])

        #
        dimension_temporal = self.configuracion['dimensiones_temporales']
        if dimension_temporal in df.columns:
            df[dimension_temporal] = transformar_formato_tiempo_segun_periodicidad(df[dimension_temporal],
                                                                                   self.periodicidad)

        # Parche IECA ya que están indexando por cod en lugar de id.
        for jerarquia in self.jerarquias:
            columna = jerarquia.metadatos['alias']

            if columna != dimension_temporal:
                df[columna] = df.merge(jerarquia.datos, how='left', left_on=columna, right_on='cod')['id'].values

        self.logger.info('Datos Transformados a DataFrame Correctamente')

        return df

    def desacoplar_datos_por_medidas(self):
        self.logger.info('Desacoplando las Observaciones del DataFrame')

        columnas_jerarquia = [jerarquia.metadatos['alias'] for jerarquia in self.jerarquias]
        columnas = columnas_jerarquia + ['INDICATOR', 'OBS_VALUE']
        df = pd.DataFrame(columns=columnas)

        for medida in [medida['des'] for medida in self.medidas]:
            if medida not in self.configuracion['indicadores_a_borrar']:
                self.logger.info('Desacoplando para la medida: %s', medida)

                columnas = columnas_jerarquia + ['OBS_VALUE', 'INDICATOR']
                columnas_ordenadas = columnas_jerarquia + ['INDICATOR', 'OBS_VALUE']

                valores_medida = self.datos[columnas_jerarquia + [medida]].copy()
                valores_medida.loc[:, 'INDICATOR'] = medida
                valores_medida.columns = columnas
                valores_medida = valores_medida[columnas_ordenadas]
                df = pd.concat([df, valores_medida])
                self.logger.info('Medida Desacoplada: %s', medida)

        self.logger.info('DataFrame Desacoplado')
        return df

    def guardar_datos_procesados(self, directorio='iecasdmx/sistema_informacion/BADEA/datos/'):
        self.datos_por_observacion.to_csv(os.path.join(directorio, str(self.id_consulta) + '.csv'), sep=';',
                                          index=False)

    def mapear_valores(self, directorio="iecasdmx/sistema_informacion/mapas/"):
        self.logger.info('Mapeando observaciones hacia SDMX')
        columnas_a_mapear = list(set.intersection(set(self.datos_por_observacion.columns),
                                                  set(self.configuracion['dimensiones_a_mapear'])))
        for columna in columnas_a_mapear:
            self.logger.info('Mapeando: %s', columna)
            mapa = pd.read_csv(os.path.join(directorio, columna), dtype='string')
            self.datos_por_observacion[columna] = \
                self.datos_por_observacion.merge(mapa, how='left', left_on=columna, right_on='SOURCE')['TARGET'].values

    def crear_plantilla_mapa(self, directorio="iecasdmx/sistema_informacion/mapas_plantillas"):
        columnas_plantilla = ['SOURCE', 'COD', 'NAME', 'TARGET']
        for column in self.datos_por_observacion.columns:
            if column in self.configuracion['dimensiones_a_mapear']:
                df_mapa = None

                if os.path.isfile(os.path.join(directorio, column)):
                    df_mapa = pd.read_csv(os.path.join(directorio, column), dtype='string')
                else:
                    df_mapa = pd.DataFrame(columns=columnas_plantilla, dtype='string')

                uniques = np.full([len(self.datos_por_observacion[column].unique()), len(columnas_plantilla)], None)
                uniques[:, 0] = self.datos_por_observacion[column].unique()

                df_auxiliar = pd.DataFrame(uniques, columns=columnas_plantilla, dtype='string')

                df_mapa = pd.concat([df_mapa, df_auxiliar]).drop_duplicates('SOURCE', keep='first')

                if column != 'INDICATOR':
                    jerarquia_codigos = pd.read_csv(
                        os.path.join("iecasdmx/sistema_informacion/BADEA/jerarquias",
                                     column + '.csv'), sep=';',
                        dtype='string')

                    df_mapa['COD'][df_mapa['COD'].isna()] = \
                        df_mapa[df_mapa['COD'].isna()].merge(jerarquia_codigos, how='left', left_on='SOURCE',
                                                             right_on='ID')['COD_y']
                    df_mapa['NAME'][df_mapa['NAME'].isna()] = \
                        df_mapa[df_mapa['NAME'].isna()].merge(jerarquia_codigos, how='left', left_on='SOURCE',
                                                              right_on='ID')['NAME_y']
                df_mapa.to_csv(os.path.join(directorio, column), index=False)


def transformar_cadena_numero(cadenas_df):
    return cadenas_df.replace(',', '.').replace('%', '')


def transformar_formato_tiempo_segun_periodicidad(serie, periodicidad):
    dimension_a_transformar = ['Mensual', 'Trimestral', 'Mensual  Fuente: Instituto Nacional de Estadística']
    if periodicidad in dimension_a_transformar:
        serie = serie.apply(lambda x: x[:4] + '-' + x[4:])
    return serie


def insertar_freq(df, periodicidad):
    diccionario_periodicidad_sdmx = {'Mensual': 'M', 'Anual': 'A',
                                     'Mensual  Fuente: Instituto Nacional de Estadística': 'M', '': 'M'}
    df['FREQ'] = diccionario_periodicidad_sdmx[periodicidad]
    return df
