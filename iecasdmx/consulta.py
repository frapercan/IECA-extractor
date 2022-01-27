import requests
import yaml

from iecasdmx.datos import Datos
from iecasdmx.jerarquia import Jerarquia


class Consulta:
    """Estructura de datos para la información extraida de la API del IECA.
    Este objeto al inicializarse consultara la API del IECA utilizando el número
    de consulta estructurando la información en los siguientes atributos:
        #. Metadatos en forma de diccionario
        #. Lista de clases 'Jerarquia'
        #. Medidas en forma de diccionario
        #. Observaciones/Datos en una clase 'Datos'


        :param id_consulta: ID de la consulta en base a una actividad
        :type id_consulta: str
        """

    def __init__(self, id_consulta):
        self.id_consulta = id_consulta
        self.metadatos, self.jerarquias_sin_procesar, self.medidas, self.datos_sin_procesar = self.solicitar_informacion_api()

        self.jerarquias = [Jerarquia(jerarquia) for jerarquia in self.jerarquias_sin_procesar]
        self.datos = Datos(self.datos_sin_procesar, self.jerarquias, self.medidas)

        self.crear_plantilla_mapa()

    # def guardar_datos(self):
    #     pd.DataFrame(self.datos).to_csv(f'datos/{self.id_consulta}.csv')
    #
    #     with open(f'sistema_informacion/BADEA/datos/{self.id_consulta}.yaml', 'w', encoding='utf-8') as fichero:
    #         yaml.dump(self.datos, fichero, encoding='utf-8', allow_unicode=True)

    def crear_plantilla_mapa(self):
        ## Work in progress
        mapa = {}
        mapa["jerarquias"] = [{jerarquia.id_jerarquia: {'id': jerarquia.datos['id'].values.tolist(),
                                                        'label': jerarquia.datos['label'].values.tolist()}} for
                              jerarquia in self.jerarquias]

        mapa["medidas"] = [{'id': medida['id'], 'des': medida['des']} for medida in self.medidas]
        with open(f'iecasdmx/sistema_informacion/mapas_plantillas/{self.id_consulta}.yaml', 'w',
                  encoding='utf-8') as fichero:
            yaml.dump(mapa, fichero, encoding='utf-8', allow_unicode=True)

    def solicitar_informacion_api(self):
        """Consulta la API con el 'id_consulta' del objeto.
           Además Inicializa las Jerarquias

         :param name: The name to use.
         :type name: str.
         :param state: Current state to be in.
         :type state: bool.
         :returns:  dict(metadatos),list(Jerarquia),dict(measures),dict(data).
         """
        respuesta = requests.get(
            f"https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/{self.id_consulta}").json()

        return respuesta['metainfo'], \
               respuesta['hierarchies'], \
               respuesta['measures'], \
               respuesta['data']


if __name__ == "__main__":
    Consulta(49325)
