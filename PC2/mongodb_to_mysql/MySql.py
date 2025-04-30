
import mysql.connector

HOST = "194.210.86.10"
USER = "aluno"
PASSWD = "aluno"
DATABASE = "maze"

class MySql:

    def __init__( self ):

        self.mydb = mysql.connector.connect(
            host = HOST,
            user = USER,
            passwd = PASSWD,
            database = DATABASE
        )
        self.cursor = self.mydb.cursor()
        print("Connected to MySQL from cloud!")


    def get_corridors( self ):

        self.cursor.execute( "SELECT * FROM corridor" )

        corridor_list = self.cursor.fetchall()

        corridor_filtered = []

        for corridor in corridor_list:

            corridor_filtered.append( ( corridor[0], corridor[1] ) )

        return corridor_filtered
