import os

import yaml
import pandas as pd

pd.options.mode.chained_assignment = None


def traducir_cadena(cadena, lenguaje_origen, lenguaje_destino, traductor, directorio_traducciones):
    with open(directorio_traducciones, "r") as fichero_traducciones:
        traducciones = yaml.load(fichero_traducciones, Loader=yaml.FullLoader)
        try:
            cadena_multilenguaje = traducciones[cadena]
        except:
            cadena_multilenguaje = {lenguaje_origen: cadena,
                                    lenguaje_destino: traductor.translate_text(cadena,
                                                                               target_lang=lenguaje_destino if lenguaje_destino != 'en' else 'EN-US').text}
            traducciones[cadena] = cadena_multilenguaje
        with open(directorio_traducciones, "w+") as fichero_traducciones:
            yaml.dump(traducciones, fichero_traducciones, sort_keys=True)

    return cadena_multilenguaje


def traducir_dataframe_por_variables(df, variables, lenguaje_origen, lenguaje_destino, traductor,
                                     directorio_traducciones):
    if not (df is None):
        df_multilenguaje = {lenguaje_origen: df, lenguaje_destino: df.copy(deep=True)}
        with open(directorio_traducciones, "r") as fichero_traducciones:
            traducciones = yaml.load(fichero_traducciones, Loader=yaml.FullLoader)

            for variable in variables:
                valores_traducidos = []
                for valor in df_multilenguaje[lenguaje_destino][variable].values:
                    if len(valor) == 1:
                        valor = valor[0]
                    try:
                        valores_traducidos.append(traducciones[valor][lenguaje_destino])
                    except:
                        valor_traducido = traductor.translate_text(valor,
                                                                   target_lang=lenguaje_destino if lenguaje_destino != 'en' else 'EN-US').text
                        valores_traducidos.append(valor_traducido)
                        traducciones[valor] = {'en': '', 'es': ''}
                        traducciones[valor][lenguaje_origen] = valor
                        traducciones[valor][lenguaje_destino] = valor_traducido

                        with open(directorio_traducciones, "w+") as fichero_traducciones:
                            yaml.dump(traducciones, fichero_traducciones, sort_keys=True)
                df_multilenguaje[lenguaje_destino][variable] = valores_traducidos
        return df_multilenguaje


def aglutinar_jerarquias_desde_consultas_por_dimension(jerarquias, dimension):
    if dimension not in ['INDICATOR', 'FREQ']:
        jerarquias_filtradas = [jerarquia for jerarquia in jerarquias if jerarquia.nombre == dimension]
        concatenados = pd.concat([jerarquia.datos_sdmx for jerarquia in jerarquias_filtradas], axis=0)
        concatenados.drop_duplicates(subset=['ID'], inplace=True)
        concatenados.dropna(subset=['ID'], inplace=True)

        return concatenados


def mapear_id_por_dimension(df, dimension, directorio_mapas_dimensiones):
    directorio_mapa = os.path.join(directorio_mapas_dimensiones, dimension)
    df_mapa = pd.read_csv(directorio_mapa, sep=',', dtype='string')
    df.loc[:, 'ID'] = \
        df.merge(df_mapa, how='left', left_on='ID', right_on='SOURCE')['TARGET'].values
    df.loc[:, 'PARENTCODE'] = \
        df.merge(df_mapa, how='left', left_on='PARENTCODE', right_on='SOURCE')['TARGET'].values
    return df


def montar_medidas(directorio_mapas_dimensiones):
    mapa_indicadores = pd.read_csv(os.path.join(directorio_mapas_dimensiones, 'INDICATOR'), sep=',', dtype='string')
    mapa_indicadores.drop_duplicates(subset=['TARGET'], inplace=True)
    mapa_indicadores.dropna(subset=['TARGET'], inplace=True)

    indicadores = pd.DataFrame(mapa_indicadores[['TARGET', 'SOURCE', 'COD', 'COD', 'NAME']].values,
                               columns=[['ID', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']], dtype='string')



    return indicadores
