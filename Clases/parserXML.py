""" Parseará el fichero XML que se indica. Obtiene toda la información del
fichero, de los pedidos y de los detalles de éstos. Se le indicará qué
fichero XML parseará y posteriormente insertará los registros en la BBDD"""

from datetime import datetime
from xml.etree import ElementTree as ET

import pandas as pd


class ParserXML():
    """ Inicializar las listas que contendrán los registros de las tablas/CSV"""
    list_header = list()
    list_pedidos = list()
    list_pedidos_detalles = list()

    pd_list_header = None
    pd_list_pedidos = None
    pd_list_pedidos_detalles = None

    FICHERO = None
    RUTA_ENTRADA = None

    def __init__(self):
        print("ParserXML creado ...")

    def parsearXML(self, FICHERO, RUTA_ENTRADA):
        """ Cargar el fichero XML de entrada en la variable xroot """

        self.FICHERO = FICHERO
        self.RUTA_ENTRADA = RUTA_ENTRADA

        FICHERO_PARSER = RUTA_ENTRADA + FICHERO

        xtree = ET.parse(FICHERO_PARSER)
        xroot = xtree.getroot()

        """ definición de las funciones """
        def getValue(valor):
            """ Esta función es para recibir el valor de los objetos del XML
            en caso de que no sea None"""

            if (valor is not None):
                return valor.text
            else:
                None

        def funcion_list_header(self,xroot):
            """ Esta funcion extrae del XML los datos de la cabecera del fichero """

            self.list_header.clear()
            fecha_desde = xroot.find('header/fecha_desde').text
            fecha_hasta = xroot.find('header/fecha_hasta').text
            pagina = xroot.find('header/pagina').text
            DateInsert = str(datetime.now())

            # Crear un diccionario con los valores
            diccionario = {"fecha_desde": fecha_desde,
                           "fecha_hasta": fecha_hasta,
                           "pagina": pagina,
                           "DateInsert": DateInsert
                            }
            # Añadir este diccionario a la lista. Sólo tiene un registro
            self.list_header.append(diccionario)

        def funcion_list_pedidos(self, xroot):
            """ Guarda en una lista todos los registros que están en la etiqueta 'pedido' """

            self.list_pedidos.clear()
            for node in xroot.findall('pedido'):
                pedido_id = node.attrib.get('id')
                cliente_id = node.find('cliente_id')
                cliente = node.find('cliente')
                fecha = node.find('fecha')
                descuento_euro = node.find('descuento_euro')
                DateInsert = str(datetime.now())

                self.list_pedidos.append({"pedido_id": pedido_id,
                                          "cliente_id": getValue(cliente_id),
                                          "cliente": getValue(cliente),
                                          "fecha": getValue(fecha),
                                          "descuento_euro": getValue(descuento_euro),
                                          "DateInsert": DateInsert
                                          })

        def funcion_list_pedidos_detalles(self, xroot):

            self.list_pedidos_detalles.clear()
            for node in xroot.findall('pedido'):
                pedido_id = node.attrib.get('id')
                cliente_id = node.find('cliente_id')
                cliente = node.find('cliente')
                fecha = node.find('fecha')
                descuento_euro = node.find('descuento_euro')
                DateInsert = str(datetime.now())

                for node_2 in node.findall('detalles/linea'):
                    producto_id = node_2.attrib.get('producto_id')
                    color = node_2.find('color')
                    precio_euro = node_2.find('precio_euro')
                    unidades = node_2.find('unidades')

                    self.list_pedidos_detalles.append({"pedido_id": pedido_id,
                                                       "cliente_id": getValue(cliente_id),
                                                       "cliente": getValue(cliente),
                                                       "fecha": getValue(fecha),
                                                       "descuento_euro": getValue(descuento_euro),
                                                       "DateInsert": DateInsert,
                                                       "producto_id": producto_id,
                                                       "color": getValue(color),
                                                       "precio_euro": getValue(precio_euro),
                                                       "unidades": getValue(unidades)
                                                       })

        # Parsear el fichero XML para obtener los valores de la listas
        funcion_list_header(self, xroot)
        funcion_list_pedidos(self, xroot)
        funcion_list_pedidos_detalles(self, xroot)

        # Crear los dataframes a partir de las listas de diccionarios. Se trabaja mejor con pandas

        self.pd_list_header = pd.DataFrame(self.list_header)
        self.pd_list_pedidos = pd.DataFrame(self.list_pedidos)
        self.pd_list_pedidos_detalles = pd.DataFrame(self.list_pedidos_detalles)

    def insertRowsToBBDD(self, conexion):

        self.conexion = conexion

        def insert_list_header(self):
            sql_insert = "insert into header (pagina, fecha_desde, fecha_hasta, DateInsert) values (%s, %s, %s, %s)"

            params_array = (self.list_header[0]["pagina"],
                            self.list_header[0]["fecha_desde"],
                            self.list_header[0]["fecha_hasta"],
                            self.list_header[0]["DateInsert"]
                            )

            self.conexion.execQuery(query_params=sql_insert, params=params_array)

        def insert_list_pedidos(self):
            sql_insert = " insert into pedidos (id, cliente_id, cliente, fecha, descuento_euro, DateInsert) values (%s, %s, %s, %s, %s, %s)"

            params_array = []
            for pedido in self.list_pedidos:
                params_array.append((pedido["pedido_id"],
                                     pedido["cliente_id"],
                                     pedido["cliente"],
                                     pedido["fecha"],
                                     pedido["descuento_euro"],
                                     pedido["DateInsert"],
                                     ))

            self.conexion.execQueryArray(query_params=sql_insert, paramsArray=params_array)

        def insert_list_pedidos_detalles(self):
            sql_insert = " insert into pedidos_detalles (id, cliente_id, cliente, fecha, descuento_euro, DateInsert, producto_id, color, precio_euro, unidades) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "

            params_array = []
            for pedido in self.list_pedidos_detalles:
                params_array.append((pedido["pedido_id"],
                                     pedido["cliente_id"],
                                     pedido["cliente"],
                                     pedido["fecha"],
                                     pedido["descuento_euro"],
                                     pedido["DateInsert"],
                                     pedido["producto_id"],
                                     pedido["color"],
                                     pedido["precio_euro"],
                                     pedido["unidades"]
                                     ))

            self.conexion.execQueryArray(query_params=sql_insert, paramsArray=params_array)

        insert_list_header(self)
        insert_list_pedidos(self)
        insert_list_pedidos_detalles(self)

        self.conexion.commit()
