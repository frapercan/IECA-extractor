import pandas as pd
import deepl


class Codelist:
    def __init__(self, session, configuracion_global, traductor):
        self.session = session
        self.configuracion_global = configuracion_global
        self.traductor = traductor
        self.codelists = self.recuperar_codelists()

    def recuperar_codelists(self):
        response = self.session.get(f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/codelist')
        try:
            codelists_json = response.json()['data']['codelists']
        except:
            codelists_json = {}
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

    def anadir_codelist(self, agencia, id, version, descripcion):

        json = {'data': {'codelists': [{'agencyID': agencia, 'id': id,'isFinal': 'true', 'names': descripcion, 'version': str(version)}]},
                'meta': {}}

        try:

            codelist = \
                self.session.post(f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/createArtefacts',
                                  json=json)
        except Exception as e:
            raise e

    def comprueba_codelist(self):
        json = {'agencyId': self.agencia, 'id': self.id, 'version': self.version, 'lang': 'es', 'pageNum': '1',
                'pageSize': '999999'}
        codelist = \
            self.session.post(f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/NOSQL/codelist',
                              json=json).json()['data']['codelists'][0]['codes']

    def get_items(self, agencia, id, version):
        json = {'agencyId': agencia, 'id': id, 'version': version, 'lang': 'en', 'pageNum': '1',
                'pageSize': '999999'}
        lenguajes = self.configuracion_global['lenguajes']

        codelist = \
            self.session.post(f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/NOSQL/codelist',
                              json=json).json()['data']['codelists']


        codelist_lenguajes_contenedor = {}
        for lenguaje in lenguajes:
            codelist_lenguajes_contenedor[lenguaje] = []

        for lenguaje in lenguajes:
            for item in codelist:
                try:
                    nombre = item['names'][lenguaje]
                except:
                    nombre = None
                try:
                    descripcion = item['descriptions'][lenguaje]
                except:
                    descripcion = None
                codelist_lenguajes_contenedor[lenguaje].append(
                    [item['id'], nombre, descripcion,
                     item['parent'] if 'parent' in item.keys() else None,
                     item['annotations'][0]['text']])
            codelist_lenguajes_contenedor[lenguaje] = pd.DataFrame(codelist_lenguajes_contenedor[lenguaje],
                                                                   columns=['ID', 'NAME', 'DESCRIPTION', 'PARENTCODE',
                                                                            'ORDER'])
        # if self.configuracion_global['traducir']:
        #     for lenguaje_fuente, lenguaje_destino in zip(lenguajes, lenguajes[::-1]):
        #         print(lenguaje_fuente, lenguaje_destino)
        #         codelist_fuente = codelist_lenguajes_contenedor[lenguaje_fuente]
        #         codelist_destino = codelist_lenguajes_contenedor[lenguaje_destino]
        #         print(codelist_destino)
        #
        #         indices_items_no_traducidos = codelist_destino[codelist_destino['NAME'].isnull()].index
        #         print('len', len(indices_items_no_traducidos), indices_items_no_traducidos)
        #         if len(indices_items_no_traducidos):
        #             print('tradducion', codelist_fuente['NAME'][indices_items_no_traducidos].values)
        #             traducciones = self.traductor.translate_text(
        #                 codelist_fuente['NAME'][indices_items_no_traducidos].values,
        #                 target_lang=lenguaje_destino if lenguaje_destino != 'en' else 'EN-US')
        #             for traduccion in traducciones:
        #                 print(traduccion)
        #             codelist_destino['NAME'][indices_items_no_traducidos] = traducciones
        #             codelist_lenguajes_contenedor[lenguaje_destino] = codelist_destino
        # return codelist_lenguajes_contenedor

    def extender(self, df, agencia, id, version):
        for lenguaje in self.configuracion_global['lenguajes']:
            csv = df[lenguaje].to_csv(index=False, sep=';')
            custom_data = str({"type": "codelist",
                               "identity": {"ID": id, "Agency": agencia, "Version": version, 'isFinal': 'true'},
                               "lang": lenguaje, "firstRowHeader": 'true',
                               "columns": {"id": 0, "name": 1, "description": 2, "parent": 3, "order": 4,
                                           "fullName": -1, "isDefault": -1}, "textSeparator": ";",
                               "textDelimiter": 'null'}).encode(encoding='UTF-8')
            files = {'file': (
                'USELESS.csv', csv, 'application/vnd.ms-excel', {}),
                'CustomData': (None, custom_data)}
            try:
                response = self.session.post(
                    f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/CheckImportedFileCsvItem',
                    files=files)

            except Exception as e:
                raise e

            response = response.json()
            response['identity']['ID'] = self.id
            response['identity']['Agency'] = self.agencia
            response['identity']['Version'] = self.version

            try:
                response = self.session.post(
                    f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/importFileCsvItem',
                    json=response)

            except Exception as e:
                raise e

    def anadir_elementos(self, elementos, agencia, id, version):


        for lenguaje in self.configuracion_global['lenguajes']:
            csv = elementos[lenguaje].to_csv(index=False, sep=';')
            custom_data = str({"type": "codelist",
                               "identity": {"ID": id, "Agency": agencia, "Version": version},
                               "lang": lenguaje, "firstRowHeader": 'true',
                               "columns": {"id": 0, "name": 1, "description": 2, "parent": 3, "order": 4,
                                           "fullName": -1, "isDefault": -1}, "textSeparator": ";",
                               "textDelimiter": 'null'}).encode(encoding='UTF-8')
            files = {'file': (
                'USELESS.csv', csv, 'application/vnd.ms-excel', {}),
                'CustomData': (None, custom_data)}
            try:
                response = self.session.post(
                    f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/CheckImportedFileCsvItem',
                    files=files)
                print(response.text)
            except Exception as e:
                print(e)
                raise e
            response = response.json()
            response['identity']['ID'] = id
            response['identity']['Agency'] = agencia
            response['identity']['Version'] = version

            try:
                response = self.session.post(
                    f'{self.configuracion_global["direccion_API_SDMX"]}/sdmx/ws/NODE_API/importFileCsvItem',
                    json=response)
                print(response.text)

            except Exception as e:
                print(e)
                raise e
