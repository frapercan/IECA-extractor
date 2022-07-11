
import requests

from iecasdmx.funciones import traducir_cadena, aglutinar_jerarquias_desde_consultas_por_dimension, \
    traducir_dataframe_por_variables, montar_medidas
from iecasdmx.sdmx.codelist import Codelist


class SDMX:
    def __init__(self, actividad, configuracion_global, mapa_conceptos_codelist, traducciones, traductor):
        self.actividad = actividad
        self.configuracion_global = configuracion_global
        self.mapa_conceptos_codelist = mapa_conceptos_codelist
        self.session = self.autentificar()
        self.traducciones = traducciones
        self.traductor = traductor
        self.codelists = Codelist(self.session, self.configuracion_global, traductor)
        # self.concept_schemes = self.get_concept_schemes()
        # self.dsds = self.get_dsds()
        # self.category_schemes = self.get_category_schemes()

    def gestionar_codelists(self):
        codelists = []
        jerarquias = []
        for consulta in self.actividad.consultas.values():
            for jerarquia in consulta.jerarquias:
                jerarquias.append(jerarquia)
        print(self.actividad.configuracion['variables'])
        for variable in self.actividad.configuracion['variables']:
            if variable == 'OBS_VALUE':
                continue
            agencia, id, version = self.inicializar_codelist(variable)


            print(variable)
            if variable in self.configuracion_global['dimension_temporal']:
                continue

            if variable == 'OBS_STATUS':
                continue

            if variable == self.configuracion_global['dimension_medida']:
                lista_codigo = montar_medidas(self.configuracion_global['directorio_mapas_dimensiones'])
                lista_codigo_multilenguaje = traducir_dataframe_por_variables(lista_codigo, ['NAME'],
                                                                              'es', 'en', self.traductor,
                                                                              self.configuracion_global[
                                                                                  'fichero_traducciones'])
                self.codelists.anadir_elementos(lista_codigo_multilenguaje, agencia, id, version)
                continue
            else:
                lista_codigo = aglutinar_jerarquias_desde_consultas_por_dimension(jerarquias, variable)
                lista_codigo_multilenguaje = traducir_dataframe_por_variables(lista_codigo, ['NAME', 'DESCRIPTION'],
                                                                              'es', 'en', self.traductor,
                                                                              self.configuracion_global[
                                                                                  'fichero_traducciones'])
                self.codelists.anadir_elementos(lista_codigo_multilenguaje, agencia, id, version)
                continue




    def obtener_descripcion(self, variable):
        for consulta in self.actividad.consultas.values():
            for jerarquia in consulta.jerarquias:
                if variable == jerarquia.metadatos['alias'][2:-2]:
                    return jerarquia.metadatos['des']

    def autentificar(self):
        headers = {}
        headers['nodeId'] = self.configuracion_global['nodeId']

        session = requests.session()
        session.headers = headers
        response = session.post(
            f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/api/Security/Authenticate/',
            json={'username': 'admin'})
        session.headers['Authorization'] = f'bearer {response.json()["token"]}'
        return session

    def recuperar_codelists(self):
        response = self.session.get(f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/codelist')
        codelists_json = response.json()['data']['codelists']
        codelists = {}
        for codelist in codelists_json:
            agencia = codelist['agencyID']
            version = codelist['version']
            codelist_id = codelist['id']

            if agencia not in codelists.keys():
                codelists[agencia] = {}

            if codelist_id not in codelists[agencia].keys():
                codelists[agencia][codelist_id] = {}
            codelists[agencia][codelist_id][version] = '/'.join([agencia, codelist_id, version])
        return codelists

    def get_concept_schemes(self):
        response = self.session.get(f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/conceptScheme')
        concept_scehmes_json = response.json()['data']['conceptSchemes']

        concept_schemes = {}
        for concept_scheme in concept_scehmes_json:
            concept_schemes[concept_scheme['links'][1]['href'].split('ConceptScheme/')[1]] = concept_scheme

        return concept_schemes

    def get_dsds(self):
        response = self.session.get(f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/dsd')

        try:
            dsds_json = response.json()['data']['dataStructures']
        except:
            dsds_json = {}
        dsds = {}
        for dsd in dsds_json:
            dsds[dsd['links'][1]['href'].split('DataStructure/')[1]] = dsd

        return dsds

    def get_category_schemes(self):
        response = self.session.get(
            f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/categoryScheme')
        category_schemes_json = response.json()['data']['categorySchemes']
        category_schemes = {}
        for category_scheme in category_schemes_json:
            category_schemes[category_scheme['links'][1]['href'].split('CategoryScheme/')[1]] = category_scheme

        return category_schemes

    def inicializar_codelist(self, variable):
        agencia,id,version = None,None,None
        if self.mapa_conceptos_codelist[variable]['codelist']:
            agencia, id, version = self.mapa_conceptos_codelist[variable]['codelist'].values()
            try:
                self.codelists.codelists[agencia][id][str(version)]
            except KeyError as e:

                if 'descripcion' in self.mapa_conceptos_codelist[variable].keys():
                    descripcion = self.mapa_conceptos_codelist[variable]['descripcion']
                else:
                    descripcion = self.obtener_descripcion(variable)
                descripcion_multilenguaje = traducir_cadena(descripcion, 'es', 'en', self.traductor,
                                                            self.configuracion_global['fichero_traducciones'])

                self.codelists.anadir_codelist(agencia, id, version, descripcion_multilenguaje)
        return agencia, id, version

    def alta_cubos(self):
        print(self.actividad.configuracion)
