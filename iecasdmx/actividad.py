import os
import sys
import logging
import yaml
import pandas as pd

from iecasdmx import Consulta

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Actividad:
    """
    """

    def __init__(self, configuracion_global, configuracion_actividad, plantilla_actividad, actividad):
        self.configuracion_global = configuracion_global
        self.configuracion_actividad = {**plantilla_actividad, **configuracion_actividad}
        self.actividad = actividad

        self.consultas = {}

        self.logger = logging.getLogger(f'{self.__class__.__name__} [{actividad}]')
        self.logger.info('Inicializando actividad completa')

    def generar_consultas(self):
        for consulta in self.configuracion_actividad['consultas']:
            try:
                consulta = Consulta(consulta, self.configuracion_global, self.configuracion_actividad,
                                    self.actividad)

                self.consultas[consulta.id_consulta] = consulta
            except Exception as e:
                raise e
            consulta.ejecutar()

    def ejecutar(self):
        self.logger.info('Ejecutando actividad')
        for accion in self.configuracion_actividad['acciones_actividad_completa'].keys():
            if self.configuracion_actividad['acciones_actividad_completa'][accion]:
                getattr(self, accion)()
        self.logger.info('Ejecución finalizada')

    def agrupar_consultas_SDMX(self):
        directorio = os.path.join(self.configuracion_global['directorio_datos_SDMX'], self.actividad)
        fichero = os.path.join(directorio, 'configuracion.yaml')
        if not os.path.exists(directorio):
            os.makedirs(directorio)

        self.logger.info('Creando fichero de configuración de la actividad')
        agrupacion = {'NOMBRE_DSD': 'DSD_' + self.actividad, 'grupos_consultas': {}, 'variables': []}
        for id_consulta, consulta in self.consultas.items():
            if consulta.metadatos['title'] not in agrupacion['grupos_consultas']:
                agrupacion['grupos_consultas'][consulta.metadatos['title']] = {
                    'id': str(len(agrupacion['grupos_consultas']) + 1),
                    'consultas': [id_consulta]}
            else:
                agrupacion['grupos_consultas'][consulta.metadatos['title']]["consultas"] \
                    .append(id_consulta)
            for columna in consulta.datos.datos_por_observacion.columns:
                if columna not in agrupacion['variables']:
                    agrupacion['variables'].append(columna)
        with open(fichero, 'w', encoding='utf-8') as fichero_actividad:
            yaml.dump(agrupacion, fichero_actividad, allow_unicode=True)
        self.logger.info('Fichero de configuración de la actividad creado y guardado')

        self.logger.info('Uniendo datos por titulo')
        for grupo, informacion_grupo in agrupacion['grupos_consultas'].items():

            self.logger.info('titulo: %s', grupo)

            for consulta in informacion_grupo['consultas']:
                self.consultas[consulta].datos.extender_con_disjuntos(agrupacion['variables'])
            columnas_grupo = [self.consultas[consulta].datos.datos_por_observacion.columns for consulta in
                              informacion_grupo['consultas']]
            self.comprobar_columnas_grupo_actividad(columnas_grupo, grupo)
            union_datos_sin_extender = pd.concat(
                [self.consultas[consulta].datos.datos_por_observacion for consulta in
                 informacion_grupo['consultas']])

            directorio_sin_extender = os.path.join(directorio, 'original')
            if not os.path.exists(directorio_sin_extender):
                os.makedirs(directorio_sin_extender)
            union_datos_sin_extender.to_csv(
                os.path.join(directorio_sin_extender, informacion_grupo['id'] + '.csv'),
                index=False)
            self.logger.info('proceso finalizado. Datos guardados')

            directorio_extension_disjuntos = os.path.join(directorio, 'extension_disjuntos')
            if not os.path.exists(directorio_extension_disjuntos):
                os.makedirs(directorio_extension_disjuntos)

            union_datos_extendidos = pd.concat(
                [self.consultas[consulta].datos.datos_por_observacion_extension_disjuntos for consulta in
                 informacion_grupo['consultas']])

            directorio_extension_disjuntos = os.path.join(directorio, 'extension_disjuntos')
            if not os.path.exists(directorio_extension_disjuntos):
                os.makedirs(directorio_extension_disjuntos)
            union_datos_extendidos.to_csv(
                os.path.join(directorio_extension_disjuntos, informacion_grupo['id'] + '.csv'),
                index=False)
        self.logger.info('Datos por titulo unidos')

    def comprobar_columnas_grupo_actividad(self, columnas_grupo, grupo):
        self.logger.info('Comprobando columnas del grupo de consultas')
        columnas_existentes = set(columna for columnas in columnas_grupo for columna in columnas)

        for columnas in columnas_grupo:
            if set(columnas) != columnas_existentes:
                self.logger.warning('Las columnas no coinciden dentro del grupo: %s', grupo)
                self.logger.warning('%s', set(columnas))
                self.logger.warning('%s', columnas_existentes)

        self.logger.info('Comprobación finalizada')
