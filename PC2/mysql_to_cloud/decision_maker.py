import time
import logging
import paho.mqtt.client as mqtt
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DecisionMaker:
    def __init__(self, graph, player_id, cloud_pool, mqtt_broker, mqtt_topic):
        self.graph = graph
        self.player_id = player_id
        self.cloud_pool = cloud_pool
        self.mqtt_broker = mqtt_broker
        self.mqtt_topic = mqtt_topic
        self.running = True
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.connect(mqtt_broker)
        self.mqtt_client.loop_start()
        self._load_setup_variables()
        logging.info("DecisionMaker initialized")

    def _load_setup_variables(self):
        conn = self.cloud_pool.get_connection()
        try:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT normalnoise, noisevartoleration FROM setupmaze LIMIT 1")
                result = cursor.fetchone()
                if result:
                    self.normalnoise = result['normalnoise']
                    self.noisevartoleration = result['noisevartoleration']
                else:
                    self.normalnoise = 19
                    self.noisevartoleration = 2.5
                    logging.warning("No setup data found, using default values")
        except Exception as e:
            logging.error(f"Error loading setup variables: {e}")
            self.normalnoise = 19
            self.noisevartoleration = 2.5
        finally:
            conn.close()

    def start(self):
        logging.info("Starting DecisionMaker loop")
        while self.running:
            self._send_close_all_doors()
            time.sleep(5)  # Wait 5 seconds with doors closed
            for room_id in range(0, 9):  # Assuming rooms are numbered 0 to 8
                self._check_room(room_id)
            self._send_open_all_doors()
            time.sleep(2)  # Wait 2 seconds before repeating

    def _check_room(self, room_id):
        state = self.graph.get_room_state(room_id)
        if state['odds'] == state['evens'] and state['odds'] != 0:
            self._send_close_all_doors()
            time.sleep(5)
            logging.info(f"Room {room_id} has balanced marsamis: odds={state['odds']}, evens={state['evens']}")
            self._send_score(room_id)

    def _send_close_all_doors(self):
        command = f"{{Type: CloseAllDoor, Player:{self.player_id}}}"
        self.mqtt_client.publish(self.mqtt_topic, command)
        logging.info("Sent close all doors command")

    def _send_open_all_doors(self):
        command = f"{{Type: OpenAllDoor, Player:{self.player_id}}}"
        self.mqtt_client.publish(self.mqtt_topic, command)
        logging.info("Sent open all doors command")

    def _send_score(self, room_id):
        command = f"{{Type: Score, Player:{self.player_id}, Room: {room_id}}}"
        self.mqtt_client.publish(self.mqtt_topic, command)
        logging.info(f"Sent score command for room {room_id}")

    def stop(self):
        self.running = False
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        logging.info("DecisionMaker stopped")