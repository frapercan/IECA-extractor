import yaml

import iecasdmx
from iecasdmx.actividad import Actividad


def test_consulta_positiva():
    with open("tests/global.yaml", 'r', encoding='utf-8') as configuracion_global, \
            open("tests/ejecucion.yaml", 'r', encoding='utf-8') as configuracion_ejecucion, \
            open("iecasdmx/configuracion/actividades.yaml", 'r', encoding='utf-8') as configuracion_actividades, \
            open("iecasdmx/configuracion/plantilla_actividad.yaml", 'r',
                 encoding='utf-8') as plantilla_configuracion_actividad:
        configuracion_global = yaml.safe_load(configuracion_global)
        configuracion_ejecucion = yaml.safe_load(configuracion_ejecucion)
        configuracion_actividades = yaml.safe_load(configuracion_actividades)
        configuracion_plantilla_actividad = yaml.safe_load(plantilla_configuracion_actividad)

    for nombre_actividad in configuracion_ejecucion['actividades']:
        actividad = Actividad(configuracion_global, configuracion_actividades[nombre_actividad],
                              configuracion_plantilla_actividad, nombre_actividad)
        actividad.generar_consultas()
        actividad.ejecutar()
