import time
import mysql.connector

class MySql:


    def __init__(self, player_id) -> None:

        self.refresh_connection()

        # self.cursor.execute("delete from MedicaoPassagem")
        # self.cursor.fetchone()
        # time.sleep( 10 )

        self.cursor.execute("SELECT MAX(idJogo) as latest_game FROM MedicaoPassagem order by idJogo DESC")
        self.idJogo = self.cursor.fetchone()[ 0 ]
        print( self.idJogo )

    def get_last_20_medicoes( self ):

        # self.cursor.execute("SELECT * FROM MedicaoPassagem order by idMedicao DESC limit 3" )
        self.cursor.execute("SELECT * FROM MedicaoPassagem  order by idMedicao DESC limit 20" )
        data = self.cursor.fetchall()
        self.refresh_connection()
        return data

    def get_medicoes_by_room( self, room ):

        self.refresh_connection()
        self.cursor.execute("SELECT * FROM MedicaoPassagem where salaDestino = " + str( room ) + " order by idMedicao DESC limit 20" )
        data = self.cursor.fetchall()
        print( room, data )
        self.cursor.close()
        self.conn.close()
        return data


    def refresh_connection( self ):

        self.conn = mysql.connector.connect(
            host="mysql",
            port= 3306,
            user= "labuser",
            password= "labpass",
            database= "mydb"
        )

        # Create a cursor object
        self.cursor = self.conn.cursor()
