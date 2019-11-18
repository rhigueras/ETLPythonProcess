"""fichero responsable de la comunicación de la base de datos"""
import mysql.connector
from mysql.connector import errorcode

class Connection():

    USER = None
    PASS = None
    HOST = None
    DATABASE = None

    conn = None

    def __init__(self, USER, PASS, HOST, DATABASE):

        self.USER = USER
        self.PASS = PASS
        self.HOST = HOST
        self.DATABASE = DATABASE

        try:
            cnx = cnx = mysql.connector.connect(user=self.USER,
                                                password=self.PASS,
                                                host=self.HOST,
                                                database=self.DATABASE)

            cnx.autocommit = False

            print("Conectado a BBDD")

            self.conn = cnx

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with yout username or password.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist.")
            else:
                print(err)

    def execQuery(self, query_params, params):
        cursor = self.conn.cursor()
        cursor.execute(query_params, params)

    def execQueryArray(self, query_params, paramsArray):
        cursor = self.conn.cursor()
        cursor.executemany(query_params, paramsArray)

    def commit(self):
        """Para confirmar los inserts se tiene que terminar con un commit"""
        self.conn.commit()

    def execQuerySimple(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()

