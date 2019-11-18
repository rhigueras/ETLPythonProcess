Uno de nuestros clientes nos proporciona información sobre pedidos en formato XML porque no nos puede dar acceso a la Base de datos, 
esta es una práctica muy común ya que dar acceso de información sensible a terceros es un poco peligroso.

Bien, para acceder a los datos nos ofrecen la siguiente URL: http://datamanagement.es/Recursos/pedidos.rar

El contenido de este fichero son 3 ficheros .XML y el proceso en python se encarga de:

    Descargar el rar
    Descomprimir el contenido en una carpeta
    Parsear los ficheros XML e insertar los datos en una base de datos

Además de todo esto, registra en una tabla de control todo lo que va sucediendo para poder consultar qué ficheros se han procesado, 
sí se han procesado bien, cuánto han tardado y cuando se han procesado.

Práctica realizada via datamanagement.es
