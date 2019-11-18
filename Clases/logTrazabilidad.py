""" Se encargará de registrar en una tabla de control lo que esté sucediendo dentro del código.
Debe recibir como argumento la conexion a la BBDD """

from datetime import datetime

class LogTrazabilidad():

    IDPROCESS = 0
    conexion = None
    ETL_MAESTRO = None

    def __init__(self, conexion, ETL_MAESTRO):
        self.IDPROCESS = self.getIdProcess()
        self.conexion = conexion
        self.ETL_MAESTRO = ETL_MAESTRO
        print("Log trazabilidad: " + self.IDPROCESS)

    def getIdProcess(self):
        return datetime.now().strftime("%H%M%S")

    def iniStatusActivity(self, FICHERO, DESCRIPCION, FECHA, STATUS, CANT_REGISTROS=None):
        """ Insertar un nuevo registro en la tabla de control """

        sql_insert = " insert into ctl_activity_process (id_process, etl_master, descri_activity, fichero, status, "
        sql_insert += " start_date, cant_row) values (%s, %s, %s, %s, %s, %s, %s) "
        params = (self.IDPROCESS, self.ETL_MAESTRO, DESCRIPCION, FICHERO, STATUS, FECHA, CANT_REGISTROS)
        self.conexion.execQuery(sql_insert, params)
        self.conexion.commit()

    def updateStatusActivity(self, FICHERO, DESCRIPCION, FECHA, STATUS):
        """ Modificar el estado y la fecha de finalizacion del registro insertado """

        sql_update = " update ctl_activity_process set status = %s, end_date = %s where id_process = %s and descri_activity = %s and fichero = %s and start_date = %s "

        params = (STATUS, datetime.now(), self.IDPROCESS, DESCRIPCION, FICHERO, FECHA)
        self.conexion.execQuery(sql_update, params)
        self.conexion.commit()




