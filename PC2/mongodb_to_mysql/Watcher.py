
from threading import Lock
from time import sleep
from MongoDB import MongoDB
from MQTT import MQTT


def generate_watchers_setup_data( available_corridors_combinations: list ):
    
    watchers_pipelines = []

    corridors_combinations_pipeline = []
    for corridor in available_corridors_combinations:

        corridors_combinations_pipeline.append(
            {
                "fullDocument.from": corridor[ 0 ],
                "fullDocument.to": corridor[ 1 ]
            }
        )

    watchers_pipelines.append([

        [{
            "$match": {
                "operationType": "insert",
                "$or": corridors_combinations_pipeline
            }
        }],
        "corridors_33_PISID"

    ])

    return watchers_pipelines


class Watcher:

    pipeline = []
    data_to_publish = []
    lock = Lock()

    def setup( self, mongoDB: MongoDB, mqtt: MQTT, pipeline: list, id: str ):

        self.mqtt = mqtt
        self.pipeline = pipeline
        self.id = id
        self.watcher = mongoDB.get_watcher( None, self.pipeline )

    def publish( self ):

        while True:

            with self.lock:

                if len( self.data_to_publish ) != 0:

                    self.mqtt.publish( self.id, str( self.data_to_publish ) )
                    self.data_to_publish = []

            sleep( .25 )

    def watch( self ):

        print("Watcher with id:", self.id, "started!")

        for _ in self.watcher:

            with self.lock:

                self.data_to_publish.append( _[ 'fullDocument' ] )

                print( _[ 'fullDocument' ] )
