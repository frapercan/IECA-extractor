import json
import os
import sys

import requests

import logging

import iecasdmx

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Consulta:
    """Estructura de datos para la información extraida de la API del IECA.
    Este objeto al inicializarse consultara la API del IECA utilizando el número
    de consulta estructurando la información en los siguientes atributos:
        #. Metadatos en forma de diccionario
        #. Lista de :class:`iecasdmx.jerarquia.Jerarquia`
        #. Medidas en forma de diccionario
        #. Observaciones/Datos en una clase :class:`iecasdmx.datos.Jerarquia`

    Args:
        id_consulta (:class:`str`): ID de la consulta en base a una actividad
        configuracion (:class:`dict`): configuración de la ejecución

    Attributes:
        Metadatos (:class:`Diccionario`): Metainformación de la consulta con los siguientes campos:
            "id"
            "title"
            "subtitle"
            "activity"
            "source"
            "periodicity"
            "type"
            "notes"


        jerarquias (:obj:`Lista` de :class:`iecasdmx.jerarquia.Jerarquia`): Description of attr2
    """

    def __init__(self, id_consulta, configuracion):
        self.id_consulta = str(id_consulta)
        self.configuracion = configuracion

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info('Inicializando consulta: %s', self.id_consulta)

        self.metadatos, self.jerarquias_sin_procesar, self.medidas, self.datos_sin_procesar = \
            self.solicitar_informacion_api()

        if len(self.id_consulta) < 8:
            self.id_consulta = id_consulta
        else:
            self.id_consulta = id_consulta.split('?')[0]

        self.jerarquias = [iecasdmx.Jerarquia(jerarquia) for jerarquia in self.jerarquias_sin_procesar]
        self.datos = iecasdmx.Datos(self.id_consulta, self.configuracion, self.metadatos['periodicity'],
                                    self.datos_sin_procesar,
                                    self.jerarquias, self.medidas)

        self.logger.info('Consulta Finalizada: %s', self.id_consulta)

    def solicitar_informacion_api(self):
        """Consulta la API a través de la id_consulta.
        :param name: The name to use.
        :type name: str.
        :param state: Current state to be in.
        :type state: bool.
        :returns:  dict(metadatos),list(Jerarquia),dict(measures),dict(data).
         """

        # La maravillosa API del IECA colapsa con consultas grandes (20MB+ aprox)
        directorio_json = os.path.join('iecasdmx/sistema_informacion/BADEA/JSON/', self.id_consulta + '.json')
        respuesta = False
        try:
            self.logger.info('Buscando el JSON de la consulta en local')
            with open(directorio_json, 'r', encoding='utf-8') as json_file:
                respuesta = json.load(json_file)
            self.logger.info('JSON leido correctamente')

        except Exception as e:
            self.logger.warning('No se ha encontrado el fichero %s', directorio_json)
            self.logger.warning('Excepción: %s', e)
            self.logger.info('Iniciando peticion a la API del IECA')
            respuesta = requests.get(
                f"https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/"
                f"{self.id_consulta}").json()
            self.logger.info('Petición Finalizada')
            self.logger.info('Guardando JSON')
            with open(directorio_json, 'w', encoding='utf-8') as json_file:
                json.dump(respuesta, json_file)
            self.logger.info('JSON Guardado')

        finally:
            if respuesta:
                self.logger.info('Datos alcanzados correctamente')
            else:
                self.logger.warning('No hay información disponible')

        return respuesta['metainfo'], \
               respuesta['hierarchies'], \
               respuesta['measures'], \
               respuesta['data'] if respuesta else None
