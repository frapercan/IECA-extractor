import yaml

from iecasdmx.consulta import Consulta

if __name__ == "__main__":
    with open("configuracion.yaml", 'r', encoding='utf-8') as fichero_configuracion:
        configuracion = yaml.safe_load(fichero_configuracion)
        # consultas = [8121, 8122, 8123, 8124, 18405, 18404]
        consultas = [8121]
        for consulta in consultas:
            consulta = Consulta(consulta, configuracion)
            for jerarquia in consulta.jerarquias:
                jerarquia.guardar_datos()
            consulta.datos.crear_plantilla_mapa()
            consulta.datos.mapear_valores()
            consulta.datos.guardar_datos_procesados()
