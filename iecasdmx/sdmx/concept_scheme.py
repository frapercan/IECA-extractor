import pandas as pd
import deepl


class ConceptScheme:
    def __init__(self, link, session, configuracion_global, traductor):
        self.agencia, self.id, self.version = link.split('/')
        self.session = session
        self.configuracion_global = configuracion_global
        self.traductor = traductor