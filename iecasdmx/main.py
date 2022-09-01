import os

import yaml

from iecasdmx.ieca.actividad import Actividad
from iecasdmx.sdmx.sdmx import SDMX
import deepl



if __name__ == "__main__":
    with open("iecasdmx/configuracion/global.yaml", 'r', encoding='utf-8') as configuracion_global, \
            open("iecasdmx/configuracion/ejecucion.yaml", 'r', encoding='utf-8') as configuracion_ejecucion, \
            open("iecasdmx/configuracion/actividades.yaml", 'r', encoding='utf-8') as configuracion_actividades, \
            open("iecasdmx/configuracion/plantilla_actividad.yaml", 'r',
                 encoding='utf-8') as plantilla_configuracion_actividad, \
            open("sistema_informacion/mapas/conceptos_codelist.yaml", 'r',
             encoding='utf-8') as mapa_conceptos_codelist, \
            open("sistema_informacion/traducciones.yaml", 'r',
             encoding='utf-8') as traducciones:
        configuracion_global = yaml.safe_load(configuracion_global)
        configuracion_ejecucion = yaml.safe_load(configuracion_ejecucion)
        configuracion_actividades = yaml.safe_load(configuracion_actividades)
        configuracion_plantilla_actividad = yaml.safe_load(plantilla_configuracion_actividad)
        mapa_conceptos_codelist = yaml.safe_load(mapa_conceptos_codelist)
        traducciones = yaml.safe_load(traducciones)
        traductor = deepl.Translator('92766a66-fa2a-b1c6-d7dd-ec0750322229:fx')

    for nombre_actividad in configuracion_ejecucion['actividades']:
        actividad = Actividad(configuracion_global, configuracion_actividades[nombre_actividad],
                              configuracion_plantilla_actividad, nombre_actividad)
        actividad.generar_consultas()
        # actividad.ejecutar()




