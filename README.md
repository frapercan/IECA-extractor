# Conversión de datos del Instituto de Estadística y Cartografía Andaluz a SDMX

[![PyPI version](https://badge.fury.io/py/iecasdmx.svg)](https://badge.fury.io/py/iecasdmx)
[![codecov](https://codecov.io/gh/frapercan/IECA2SDMX/branch/develop/graph/badge.svg?token=GbJ3V9jEa7)](https://codecov.io/gh/frapercan/IECA2SDMX)
[![Python Tox](https://github.com/frapercan/IECA2SDMX/actions/workflows/tox.yml/badge.svg)](https://github.com/frapercan/IECA2SDMX/actions/workflows/tox.yml)
[![Documentation Status](https://readthedocs.org/projects/ieca2sdmx/badge/?version=latest)](https://ieca2sdmx.readthedocs.io/en/latest/?badge=latest)

Herramienta interna de transformación de datos desde la [API del IECA](https://www.juntadeandalucia.es/institutodeestadisticaycartografia/badea/apidoc) hacia el estandar SDMX, 
haciendo uso de las [herramientas SDMX del Instituto Nacional de Estadística Italiano (ISTAT Toolkit)](https://sdmxistattoolkit.github.io/).

![indexa](imagenes/indexa-logo.png)
![ieca](imagenes/ieca-logo.png)


## Despliegue

En un entorno con Python instalado, intsalar los requisitos de dependencias.

    pip3 install -r requirements.txt

## Ejecución
Con el directorio de trabajo en la raiz del proyecto ejecutar el fichero main.py

    IECA2SDMX
    └── iecasdmx
        └── main.py                    # Fichero de ejecución

## Documentación
[IECA2SDMX](https://ieca2sdmx.readthedocs.io/en/latest/)


# Información para desarrolladores del repositorio
## Ejecutar Integración continua en local

Tox es una herramienta de automatización para python, se puede instalar con pip:
    
    pip install tox
    
Sus comandos son los siguientes:

### Ejecutar Tests, Lint y Compilar la documentación

    tox

### Ejecutar tests

    tox -e py38

### Ejecutar Lint

    tox -e lint

### Compilar la documentación

    tox -e docs

Para compilar la documentación se hace uso del paquete make, se debe instalar en caso de no tenerlo presente en el entorno de trabajo.

## Integración continua
Github está configurado con dos distintas comprobaciones.

### Tox

Se ejecutará cada vez que se haga una Pull request y realizara el comando Tox completo. Indicandote si todo a ido bien.

### Publicación del paquete en Python

Se ejecutará cada vez que se haga una Pull Request a la rama "Main".

Solamente pasará si el paquete no existe previamente en el repositorio de paquetes, por lo tanto cuando estemos seguros de que todo está finalizado deberemos hacer uso de los comandos:

    bumpversion minor 

Dentro de la version actual del paquete, incrementa en 1 la subversion

    bumversion major (Incrementa la version)
    
 Incrementa la version del paquete


