import glob

from bs4 import BeautifulSoup

directorio_metadatos_regex = 'utiles/metadatos/*/*.html'


for file in glob.glob(directorio_metadatos_regex):
    print(file)
    with open(file,'r+') as fichero:
        html = BeautifulSoup(fichero, 'html.parser')

        # Borrar Target
        for tr in html.find_all("tr"):
            if 'FULL_TEST' in tr.text:
                tr.decompose()
        # Borrar seccion multilenguaje
        for div in html.find_all("div",class_="col-1"):
            if 'Italiano' in div.text:
                div.decompose()
        # Cambiar imagen
        html.find('img', class_="Cl-Header-Img-Catalog")['src'] = '../../logo.png'

        fichero.truncate(0)
        fichero.write(str(html))

    # Cambiar color de fondo de algunos elementos
    fin = open(file, "rt")
    # read file contents to string
    data = fin.read()
    # replace all occurrences of the required string
    data = data.replace('da0d14','007932')
    # close the input file
    fin.close()
    # open the input file in write mode
    fin = open(file, "wt")
    # overrite the input file with the resulting data
    fin.write(data)
    # close the file
    fin.close()

