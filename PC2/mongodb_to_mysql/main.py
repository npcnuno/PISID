
from threading import Thread
from time import sleep
from MongoDB import MongoDB
from Watcher import Watcher, generate_watchers_setup_data
from MQTT import MQTT
from MySql import MySql

def save_resume_token( token ):

    with open( "resume_token.txt", "w" ) as file:

        file.write( str( token ) )

def add_new_doc(mongo):

    print("Adding new docs...")
    while True:

        mongo.db.test.insert_many( [ { "from": 1, "to": 2 }, { "from": 1, "to": 3 }, { "from": 2, "to": 4 } ] )

        sleep( .005 )

def main():

    mySQL = MySql()
    corridors = mySQL.get_corridors()
    watchers_setup_data = generate_watchers_setup_data( corridors )
    mqtt = MQTT()

    mongoDB = MongoDB()

    for watcher_setup_data in watchers_setup_data:

        watcher = Watcher()
        watcher.setup( mongoDB, mqtt, watcher_setup_data[ 0 ], watcher_setup_data[ 1 ] )
        
        Thread( target=watcher.publish ).start()
        Thread( target=watcher.watch ).start()

    sleep( 5 )

    add_new_doc( mongoDB )

if __name__ == "__main__":

    main()
