import os
import sys
import logging
import yaml
import pandas as pd

from iecasdmx.ieca.consulta import Consulta

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Actividad:
    """Una actividad es definida por una lista de consultas a través de su ID en el fichero
    de configuración **'actividades.yaml'**.

    Esta clase ejecutará las consultas y se encargará de hacer las transformaciones pertinentes al
    grupo completo para su correcta modelización en el estandard SDMX.


    Args:
        configuracion_global (:class:`Diccionario`): Configuración común a todas las ejecuciones que se realicen.
        configuracion_actividad (:class:`Diccionario`): Configuración común para toda la actividad.
        plantilla_configuracion_actividad (:class:`Diccionario`): Configuración por defecto de la actividad.
            Este fichero de configuración extiende a :attr:`~.configuracion_actividad` con las configuraciones que no
            están explicitamente recogidas en este.
        actividad (:class:`Cadena de Texto`): Nombre de la actividad.

    Attributes:
        consultas (:obj:`Diccionario` de :class:`iecasdmx.consulta.Consulta`): Diccionario que contiene las consultas
         con los datos y metadatos, cuya clave serán los :attr:`iecasdmx.consulta.Consulta.id_consulta`
         correspondientes.
    """

    def __init__(self, configuracion_global, configuracion_actividad, plantilla_configuracion_actividad, actividad):
        self.configuracion_global = configuracion_global
        self.configuracion_actividad = {**plantilla_configuracion_actividad, **configuracion_actividad}
        self.actividad = actividad

        self.consultas = {}
        self.configuracion = {}

        self.logger = logging.getLogger(f'{self.__class__.__name__} [{actividad}]')
        self.logger.info('Inicializando actividad completa')

    def generar_consultas(self):
        """Inicializa y ejecuta las consultas a la API de BADEA dentro del diccionario :attr:`~.consultas`.

        """

        for consulta in self.configuracion_actividad['consultas']:
            try:
                consulta = Consulta(consulta, self.configuracion_global, self.configuracion_actividad,
                                    self.actividad)

                self.consultas[consulta.id_consulta] = consulta
            except Exception as e:
                raise e
            consulta.ejecutar()

    def ejecutar(self):
        """Aplica las funciones configuradas en el fichero de configuración **'actividades.yaml'** bajo
        la clave **acciones_actividad_completa**.
        """
        self.logger.info('Ejecutando actividad')
        for accion in self.configuracion_actividad['acciones_actividad_completa'].keys():
            if self.configuracion_actividad['acciones_actividad_completa'][accion]:
                getattr(self, accion)()
        self.logger.info('Ejecución finalizada')

    def agrupar_consultas_SDMX(self):
        """Tras realizar las consultas, esta función genera un fichero de configuración en  el sistema de información \
        con respecto a la actividad completa, agrupando las consultas de la actividad por su titulo. De esta forma un \
        grupo de consultas formaran un fichero de datos .CSV que contiene a todas.
        Los datos se guardarán siguiendo dos procedimientos:
            - **EXTENSION_DISJUNTOS**: Estructura de datos (DSD) común para toda la actividad donde las columnas \
            faltantes son añadidas y rellenas con la variable **_Z**
            - **ORIGINAL**: Estructura de datos (DSD) por cada grupo de consulta. Si las consultas con el mismo titulo \
            tienen dimensiones distintas, se creara una estructura común para todas las consultas y se mostrara una \
            advertencia por consola.

        """
        directorio = os.path.join(self.configuracion_global['directorio_datos_SDMX'], self.actividad)
        fichero = os.path.join(directorio, 'configuracion.yaml')
        print(self.configuracion_actividad.keys())
        if not os.path.exists(directorio):
            os.makedirs(directorio)

        self.logger.info('Creando fichero de configuración de la actividad')
        self.configuracion = {'NOMBRE_DSD': 'DSD_' + self.actividad,'categoria':self.configuracion_actividad['categoria'], 'grupos_consultas': {}, 'variables': []}
        for id_consulta, consulta in self.consultas.items():
            if consulta.metadatos['title'] not in self.configuracion['grupos_consultas']:
                self.configuracion['grupos_consultas'][consulta.metadatos['title']] = {
                    'id': str(len(self.configuracion['grupos_consultas']) + 1),
                    'consultas': [id_consulta]}
            else:
                self.configuracion['grupos_consultas'][consulta.metadatos['title']]["consultas"] \
                    .append(id_consulta)
            for columna in consulta.datos.datos_por_observacion.columns:
                if columna not in self.configuracion['variables']:
                    self.configuracion['variables'].append(columna)
        with open(fichero, 'w', encoding='utf-8') as fichero_actividad:
            yaml.dump(self.configuracion, fichero_actividad, allow_unicode=True, sort_keys=False)
        self.logger.info('Fichero de configuración de la actividad creado y guardado')

        self.logger.info('Uniendo datos por titulo')
        for grupo, informacion_grupo in self.configuracion['grupos_consultas'].items():

            self.logger.info('titulo: %s', grupo)

            for consulta in informacion_grupo['consultas']:
                self.consultas[consulta].datos.extender_con_disjuntos(self.configuracion['variables'])
                self.consultas[consulta].datos.datos_por_observacion_extension_disjuntos.to_csv(os.path.join(directorio,consulta+'.csv',),sep=';',index=False)
            columnas_grupo = [self.consultas[consulta].datos.datos_por_observacion.columns for consulta in
                              informacion_grupo['consultas']]
            self.comprobar_dimensiones_grupo_actividad(columnas_grupo, grupo)
            union_datos_sin_extender = pd.concat(
                [self.consultas[consulta].datos.datos_por_observacion for consulta in
                 informacion_grupo['consultas']])

            directorio_sin_extender = os.path.join(directorio, 'original')
            if not os.path.exists(directorio_sin_extender):
                os.makedirs(directorio_sin_extender)
            union_datos_sin_extender.to_csv(
                os.path.join(directorio_sin_extender, informacion_grupo['id'] + '.csv'), sep=';',
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
                os.path.join(directorio_extension_disjuntos, informacion_grupo['id'] + '.csv'), sep=';',
                index=False)
        self.logger.info('Datos por titulo unidos')

    def comprobar_dimensiones_grupo_actividad(self, columnas_grupo, grupo):
        """Comprueba el modelado por titulos en BADEA y muestra por pantalla advertencias sobre las dimensiones
        para facilitar su depuración.

        Args:
            columnas_grupo (:obj:`Lista` de :class:`DataFrame.columns`): Listado de columnas de cada consulta del grupo.
            grupo (:class:`Cadena de Texto`): Titulo del grupo

        """
        self.logger.info('Comprobando dimensiones del grupo de consultas')
        columnas_existentes = set(columna for columnas in columnas_grupo for columna in columnas)

        for columnas in columnas_grupo:
            if set(columnas) != columnas_existentes:
                self.logger.warning('Las dimensiones no coinciden dentro del grupo: %s', grupo)
                self.logger.warning('%s', set(columnas))
                self.logger.warning('%s', columnas_existentes)

        self.logger.info('Comprobación finalizada')
