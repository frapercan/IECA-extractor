import sys

import requests

from iecasdmx.datos import Datos
from iecasdmx.jerarquia import Jerarquia

import logging

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


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
        :param configuracion: configuración de la ejecución
        :type configuracion: dict
        """

    def __init__(self, id_consulta, configuracion):
        self.id_consulta = id_consulta
        self.configuracion = configuracion

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info('Inicializando consulta: %s', self.id_consulta)

        self.metadatos, self.jerarquias_sin_procesar, self.medidas, self.datos_sin_procesar = \
            self.solicitar_informacion_api()

        self.jerarquias = [Jerarquia(jerarquia) for jerarquia in self.jerarquias_sin_procesar]
        self.datos = Datos(self.id_consulta, self.configuracion, self.metadatos['periodicity'], self.datos_sin_procesar,
                           self.jerarquias, self.medidas)

        self.logger.info('Consulta Finalizada: %s', self.id_consulta)

    def solicitar_informacion_api(self):
        """Consulta la API con el 'id_consulta' del objeto.

         :param name: The name to use.
         :type name: str.
         :param state: Current state to be in.
         :type state: bool.
         :returns:  dict(metadatos),list(Jerarquia),dict(measures),dict(data).
         """
        self.logger.info('Iniciando peticion a la API del IECA')
        respuesta = requests.get(
            f"https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/"
            f"{self.id_consulta}").json()

        self.logger.info('Petición Finalizada')

        return respuesta['metainfo'], \
               respuesta['hierarchies'], \
               respuesta['measures'], \
               respuesta['data']
