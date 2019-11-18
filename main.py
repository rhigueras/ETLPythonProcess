""" Fichero principal que orquesta el proceso. Crea la conexion a la BBDD, descarga y descomprime los ficheros
y parsea los ficheros descomprimidos. """

import json
from datetime import datetime

from Clases.connection import Connection
from Clases.gestionArchivos import GestionArchivos
from Clases.logTrazabilidad import LogTrazabilidad
from Clases.parserXML import ParserXML

""" 
Iniciar el proceso de parsear la lista de ficheros XML que se encuentra en la lista "ficheros_xml".
Se conecta a la BBDD y va insertando en la tabla de logs todo lo que va sucediendo ademas de 
calcular tiempos e insertar la informacion de los ficheros XML
"""
def obtenerFecha():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Definir el entorno en el que conectarnos a los valores definidos en config.json
ENTORNO = "DESARROLLO"

# Obtener las variables de acceso
with open('./config.json', 'r') as file:
    config = json.load(file)

conexion = Connection(USER=config[ENTORNO]['USER'],
                      PASS=config[ENTORNO]['PASS'],
                      HOST=config[ENTORNO]['HOST'],
                      DATABASE=config[ENTORNO]['DATABASE'])

# Crear la trazabilidad para ir registrando en la tabla de control lo que va sucediendo
logTrazabilidad = LogTrazabilidad(conexion=conexion, ETL_MAESTRO="PARSEAR XML PRUEBAS")

fecha_process = obtenerFecha()
logTrazabilidad.iniStatusActivity(FICHERO="N/A",
                                  DESCRIPCION="PROCESO COMPLETO",
                                  FECHA=fecha_process,
                                  STATUS=0)

gestionArchivos = GestionArchivos()

fecha_down = obtenerFecha()
logTrazabilidad.iniStatusActivity(FICHERO="N/A",
                                  DESCRIPCION="DESCARGA & DESCOMPRESION",
                                  FECHA=fecha_down,
                                  STATUS=0)

gestionArchivos.downloadPedidosRarURL(URL_PEDIDOS_DOWNLOAD=config[ENTORNO]['URL_PEDIDOS_DOWNLOAD'],
                                      RUTA_DOWNLOAD=config[ENTORNO]['RUTA_DOWNLOAD'])

gestionArchivos.unRarFileDownload(RUTA_DOWNLOAD=config[ENTORNO]['RUTA_DOWNLOAD'],
                                  RUTA_DESCARGA=config[ENTORNO]['RUTA_XML'])

logTrazabilidad.updateStatusActivity(FICHERO="N/A",
                                     DESCRIPCION="DESCARGA & DESCOMPRESION",
                                     FECHA=fecha_down,
                                     STATUS=1)

# Obtener una lista de todos los ficheros XML que hay dentro de la ruta que se le envía por parámetro
ficheros_xml = gestionArchivos.getFilesXMLFromOrigin(RUTA=config[ENTORNO]['RUTA_XML'])
parserXML = ParserXML()
for ficheroEntrada in ficheros_xml:

    RUTA_ENTRADA = ficheroEntrada['RUTA_ENTRADA']
    FICHERO = ficheroEntrada['FICHERO']

    fecha_activity = obtenerFecha()

    try:
        logTrazabilidad.iniStatusActivity(FICHERO=FICHERO,
                                          DESCRIPCION="Parsear XML",
                                          FECHA=fecha_activity,
                                          STATUS=0)

        print("Parseando el fichero " + FICHERO)

        parserXML.parsearXML(FICHERO=FICHERO,
                             RUTA_ENTRADA=RUTA_ENTRADA)
        parserXML.insertRowsToBBDD(conexion)

    except:
        logTrazabilidad.updateStatusActivity(FICHERO=FICHERO,
                                             DESCRIPCION="Parsear XML",
                                             FECHA=fecha_activity,
                                             STATUS=-1)
    else:
        logTrazabilidad.updateStatusActivity(FICHERO=FICHERO,
                                             DESCRIPCION="Parsear XML",
                                             FECHA=fecha_activity,
                                             STATUS=1)

print("Proceso finalizado")

logTrazabilidad.updateStatusActivity(FICHERO="N/A",
                                  DESCRIPCION="PROCESO COMPLETO",
                                  FECHA=fecha_process,
                                  STATUS=1)