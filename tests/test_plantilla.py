import yaml

import iecasdmx

def test_consulta_positiva():
    with open("iecasdmx/configuracion.yaml", 'r') as fichero_configuracion:
        configuracion = yaml.safe_load(fichero_configuracion)
        iecasdmx.Consulta(49325,configuracion)
