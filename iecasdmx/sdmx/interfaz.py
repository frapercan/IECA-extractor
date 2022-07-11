from iecasdmx import Codelist


class InterfazSDMX:
    def __init__(self, actividad,session):
        self.actividad = actividad

    def gestionar_codelists(self):
        for variable in self.actividad.configuracion['variables']:
            existe = Codelist()

