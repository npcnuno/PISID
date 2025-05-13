
import paho.mqtt.client as mqtt

class Decision:

    def __init__( self, decision_topic_name: str, player_id: str ) -> None:

        self.decision_topic_name = decision_topic_name
        self.player_id = player_id

        self.setup_mqtt()

    def setup_mqtt( self ) -> None:

        self.mqtt = mqtt.Client()
        self.mqtt.connect( "test.mosquitto.org", 1883)

    def close_all_doors( self ) -> None:

        message = f"{{Type: CloseAllDoor, Player: {self.player_id}}}"
        self.mqtt.publish(self.decision_topic_name, message)

    def open_all_doors( self ) -> None:

        message = f"{{Type: OpenAllDoor, Player: {self.player_id}}}"
        self.mqtt.publish(self.decision_topic_name, message)


    def send_score( self, room ) -> None:

        message = f"{{Type: Score, Player: {self.player_id}, Room: {room}}}"
        self.mqtt.publish(self.decision_topic_name, message)
