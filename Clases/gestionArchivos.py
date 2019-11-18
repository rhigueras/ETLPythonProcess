"""
Clase responsable de la gestión de archivos. Realizará la descarga del fichero rar
descomprimirá el contenido en una carpeta y obtendrá una lista de ficheros con
extension xml/XML
"""

import os
import urllib.request

from pyunpack import Archive


class GestionArchivos():

    RUTA_ENTRADA = None
    RUTA_SALIDA_CSV = None
    RUTA_FICHEROS_PROCESADOS = None

    NOMBRE_FICHERO_DESCARGADO = "descarga.rar"

    ficheros_xml = list()

    def __init__(self):
        print ("GestionarArchivos creado ... ")

    def getFilesXMLFromOrigin(self, RUTA):
        """ Esta función guarda en la lista recibida la ubicación de todos los
        ficheros que se encuentran en la RUTA que recibe por parámetro """

        self.ficheros_xml.clear()
        diccionario = None
        self.ficheros_xml = list()
        for root, dir, files in os.walk(RUTA):
            for file in files:
                if file.endswith(".xml") | file.endswith(".XML"):
                    diccionario = {"RUTA_ENTRADA": root + "\\",
                                    "FICHERO": file
                                    }
                    self.ficheros_xml.append(diccionario)

        return self.ficheros_xml

    def crear_ruta_salida_si_no_existe(self, RUTA):
        if (not os.path.exists(RUTA)):
            os.makedirs(RUTA)

    def downloadPedidosRarURL(self, URL_PEDIDOS_DOWNLOAD, RUTA_DOWNLOAD):
        print("Beginning file download with urllib2 ...")

        self.crear_ruta_salida_si_no_existe(RUTA_DOWNLOAD)

        urllib.request.urlretrieve(URL_PEDIDOS_DOWNLOAD, RUTA_DOWNLOAD + self.NOMBRE_FICHERO_DESCARGADO)

    def unRarFileDownload(self,RUTA_DOWNLOAD, RUTA_DESCARGA):
        self.crear_ruta_salida_si_no_existe(RUTA_DESCARGA)
        Archive(RUTA_DOWNLOAD + self.NOMBRE_FICHERO_DESCARGADO).extractall(RUTA_DESCARGA)