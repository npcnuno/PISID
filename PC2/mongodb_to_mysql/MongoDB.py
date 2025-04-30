
from pymongo import MongoClient
import json


DATABASE_NAME = "PISID_MongoDB_PC1"
URI = "mongodb://localhost:27017,localhost:27018,localhost:27019"


class MongoDB:

    client = MongoClient( URI, replicaSet="rs0" )


    def __init__( self ):

        self.test_connection()

        self.db = self.client[ DATABASE_NAME ]

    def test_connection( self ):

        self.client.admin.command( "ping" )
        print( "Successfully connected to MongoDB!" )

    def get_watcher( self, resume_token: dict | None, pipeline: list ):

        return self.db.watch( pipeline, resume_after=resume_token ) if resume_token else self.db.watch( pipeline )

    # def set_watchers( self ):
    #     # resume = self.get_resume_token()
    #
    #     pipeline = [
    #         {"$match": {
    #             "operationType": "insert",
    #             "fullDocument.name": "Marinho"
    #         }}
    #     ]
    #
    #     # self.watchers.append( self.db.watch( pipeline, resume_after=resume ) )
    #     self.watchers.append( self.db.watch( pipeline ) )
    #
    # def get_resume_token( self ):
    #
    #
    #     with open( "resume_token.txt", "r" ) as file:
    #
    #         resume_token = file.read()
    #
    #     resume_token = resume_token.replace( "'", '"' )
    #
    #     return json.loads( resume_token )
