import pandas as pd


class Datos:
    """Estructura de datos para manejar los datos encontrados dentro
    de las consultas del IECA.

        :param datos: Diccionario con los datos de la consulta.
       :type datos: JSON
        """

    def __init__(self, datos, jerarquias, medidas):
        self.jerarquias = jerarquias
        self.medidas = medidas
        self.datos = self.datos_a_dataframe(datos)

    def datos_a_dataframe(self, datos):
        columnas = [jerarquia.metadatos['cod'] for jerarquia in self.jerarquias] + [medida['des'] for medida in
                                                                                    self.medidas]
        df = pd.DataFrame(datos,columns=columnas)
        df.columns = columnas
        df[columnas[:len(self.jerarquias)]] = df[columnas[:len(self.jerarquias)]].applymap(lambda x: x['cod'][0])
        df[columnas[len(self.jerarquias):]] = df[columnas[len(self.jerarquias):]].applymap(lambda x: x['val'])
        print(df)
        return df
