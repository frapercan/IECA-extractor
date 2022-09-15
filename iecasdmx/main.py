import copy
import os
import sys
import time

import pandas as pd
import yaml

from iecasdmx.ieca.actividad import Actividad
from mdmpyclient.mdm import MDM
import deepl

import logging

logger = logging.getLogger("deepl")
logger.setLevel(logging.WARNING)

if __name__ == "__main__":
    with open("iecasdmx/configuracion/global.yaml", 'r', encoding='utf-8') as configuracion_global, \
            open("iecasdmx/configuracion/ejecucion.yaml", 'r', encoding='utf-8') as configuracion_ejecucion, \
            open("iecasdmx/configuracion/actividades.yaml", 'r', encoding='utf-8') as configuracion_actividades, \
            open("iecasdmx/configuracion/plantilla_actividad.yaml", 'r',
                 encoding='utf-8') as plantilla_configuracion_actividad, \
            open("sistema_informacion/mapas/conceptos_codelist.yaml", 'r',
                 encoding='utf-8') as mapa_conceptos_codelist_file, \
            open("sistema_informacion/traducciones.yaml", 'r',
                 encoding='utf-8') as traducciones:

        configuracion_global = yaml.safe_load(configuracion_global)
        configuracion_ejecucion = yaml.safe_load(configuracion_ejecucion)
        configuracion_actividades = yaml.safe_load(configuracion_actividades)
        configuracion_plantilla_actividad = yaml.safe_load(plantilla_configuracion_actividad)
        mapa_conceptos_codelist = yaml.safe_load(mapa_conceptos_codelist_file)
        traducciones = yaml.safe_load(traducciones)
        traductor = deepl.Translator('92766a66-fa2a-b1c6-d7dd-ec0750322229:fx')

        agencia = configuracion_global['nodeId']

        controller = MDM(configuracion_global, traductor)




        category_scheme = controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0']
        if configuracion_global['reset_ddb']:
            controller.ddb_reset()
            category_scheme.import_dcs()
            category_scheme.init_categories()
            category_scheme.set_permissions()


        for nombre_actividad in configuracion_ejecucion['actividades']:
            actividad = Actividad(configuracion_global, configuracion_actividades[nombre_actividad],
                                  configuracion_plantilla_actividad, nombre_actividad)
            actividad.generar_consultas()
            actividad.ejecutar()

            cache = yaml.safe_load(open('traducciones.yaml'))

            # Conversión de Jerarquia a Codelist y Esquemas de conceptos
            for consulta in actividad.consultas.values():
                for jerarquia in consulta.jerarquias:
                    informacion = mapa_conceptos_codelist[jerarquia.nombre]

                    nombre = informacion['nombre']
                    descripcion = informacion['descripcion']

                    id_codelist = informacion['codelist']['id']
                    agencia_codelist = informacion['codelist']['agency']
                    version_codelist = informacion['codelist']['version']

                    agencia_concept_scheme = informacion['concept_scheme']['agency']
                    id_concept_scheme = informacion['concept_scheme']['id']
                    version_concept_scheme = informacion['concept_scheme']['version']
                    nomre_concept_scheme_str = id_concept_scheme.replace('CS_', '')[
                                                   0].upper() + id_concept_scheme.replace('CS_', '')[1:].lower()
                    nombre_concept_scheme = {'es': nomre_concept_scheme_str}

                    concepto = informacion['concept_scheme']['concepto']

                    controller.codelists.put(agencia_codelist, id_codelist, version_codelist, nombre, descripcion)
                    codelist = controller.codelists.data[agencia_codelist][id_codelist][version_codelist]
                    codelist.init_codes()
                    codelist.add_codes(jerarquia.datos_sdmx)
                    codelist.put()

                    controller.concept_schemes.put(agencia_concept_scheme, id_concept_scheme, version_concept_scheme,
                                                   nombre_concept_scheme, nombre_concept_scheme)
                    concept_scheme = controller.concept_schemes.data[agencia_concept_scheme][id_concept_scheme][
                        version_concept_scheme]
                    concept_scheme.init_concepts()

                    concept_scheme.add_concept(concepto, None, descripcion['es'], None)
                    concept_scheme.put()
                mapa_indicadores = pd.read_csv(
                    os.path.join(configuracion_global['directorio_mapas_dimensiones'], 'INDICATOR'))

                # Actualización de las medidas
                try:
                    codelist_medidas = controller.codelists.data[agencia]['CL_UNIT']['1.0']
                except:
                    controller.codelists.put(agencia, 'CL_UNIT', '1.0', {'es': 'Unidades de Medida (Indicadores)',
                                                                         'en': 'Measurement units (Indicators)'},
                                             {'es': 'Unidades de Medida (Indicadores)',
                                              'en': 'Measurement units (Indicators)'})
                    codelist_medidas = controller.codelists.data[agencia]['CL_UNIT']['1.0']
                codelist_medidas.init_codes()
                for consulta in actividad.consultas.values():
                    for medida in consulta.medidas:
                        id_medida = mapa_indicadores[mapa_indicadores['SOURCE'] == medida['des']]['TARGET'].values[0]
                        if id_medida not in codelist_medidas.codes['id']:
                            codelist_medidas.add_code(id_medida, None, medida['des'], None)
                    codelist_medidas.put()

            ## DSD CREACION
            id_dsd = 'DSD_' + nombre_actividad
            agencia_dsd = 'ESC01'
            version_dsd = '1.0'
            nombre_dsd = {'es': actividad.configuracion_actividad['subcategoria']}
            descripcion = None

            variables = copy.deepcopy(actividad.configuracion['variables'])
            try:
                variables.remove('TEMPORAL')
            except:
                pass
            try:
                variables.remove('INDICATOR')
            except:
                pass
            try:
                variables.remove('OBS_VALUE')
            except:
                pass
            try:
                variables.remove('OBS_STATUS')
            except:
                pass
            try:
                variables.remove('FREQ')
            except:
                pass

            dimensiones = {variable: mapa_conceptos_codelist[variable] for variable in variables}
            try:
                dsd = controller.dsds.data[agencia_dsd][id_dsd][version_dsd]
            except:
                controller.dsds.put(agencia_dsd, id_dsd, version_dsd, nombre_dsd, descripcion, dimensiones)
                dsds = controller.dsds.get(init_data=False)
                dsd = dsds[agencia_dsd][id_dsd][version_dsd]

            # Creación de categoría para la actividad
            category_scheme.init_categories()
            category_scheme.add_category(nombre_actividad, actividad.configuracion_actividad['categoria'],
                                         actividad.configuracion_actividad['subcategoria'], None)
            category_scheme.put()

            # Creación del cubo para la actividad
            for consulta in actividad.consultas.values():
                categories = category_scheme.categories
                id_cube_cat = \
                    categories[categories['id'] == nombre_actividad]['id_cube_cat'].values[0]
                id_cubo = controller.cubes.put(consulta.id_consulta, id_cube_cat, id_dsd,
                                               consulta.metadatos['subtitle'],
                                               dimensiones)

                variables = copy.deepcopy(actividad.configuracion['variables'])
                mapa = copy.deepcopy(actividad.configuracion['variables'])
                mapa = ['TIME_PERIOD' if variable == 'TEMPORAL' else variable for variable in mapa]

                controller.mappings.put(variables,mapa,id_cubo,consulta.id_consulta)
