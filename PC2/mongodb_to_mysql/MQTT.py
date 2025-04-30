
from paho.mqtt import client

BROKER = "test.mosquitto.org"
# BROKER = "broker.mqtt-dashboard.com"
PORT = 1883
CLIENT_ID = "MQTT-33-PISID"

class MQTT:

    def __init__( self ):

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)

        
        self.mqtt = client.Client( CLIENT_ID )
        self.mqtt.on_connect = on_connect
        self.mqtt.connect( BROKER, PORT )
        self.mqtt.loop_start()

    def publish( self, topic: str, payload: str ):

        self.mqtt.publish( topic, payload=payload, qos=2, retain=False )
