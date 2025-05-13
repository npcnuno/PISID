import threading
import time
import logging
import paho.mqtt.client as mqtt
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DecisionMaker:
    def __init__(self, graph, sound_condition, current_sound, decision_condition, player_id, cloud_pool, mqtt_broker, mqtt_topic, marsami_tracking):
        self.graph = graph
        self.sound_condition = sound_condition
        self.current_sound = current_sound
        self.decision_condition = decision_condition
        self.player_id = player_id
        self.cloud_pool = cloud_pool
        self.mqtt_broker = mqtt_broker
        self.mqtt_topic = mqtt_topic
        self.marsami_tracking = {m: {'room': None, 'active': True} for m in marsami_tracking}
        self.running = True
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.connect(mqtt_broker)
        self.mqtt_client.loop_start()
        logging.info("DecisionMaker initialized")

    def start(self):
        logging.info("Starting DecisionMaker loop")
        while self.running:
            with self.decision_condition:
                self._manage_doors()
                self.decision_condition.wait(timeout=0.25)

    def _manage_doors(self):
        logging.info("Managing doors")
        for room_id in self.graph.rooms:
            state = self.graph.get_room_state(room_id)
            odds = state['odds']
            evens = state['evens']
            difference = abs(odds - evens)
            if difference == 1:
                needed_type = 'even' if odds > evens else 'odd'
                if self._can_balance_room(room_id, needed_type):
                    self._close_doors_to_room(room_id)
                    threading.Thread(target=self._monitor_room_after_closure, args=(room_id,), daemon=True).start()

    def _can_balance_room(self, room_id, needed_type):
        logging.info(f"Checking if room {room_id} can be balanced with {needed_type}")
        for neighbor in self.graph.adjacency.get(room_id, []):
            neighbor_state = self.graph.get_room_state(neighbor)
            if needed_type == 'even' and neighbor_state['evens'] > 0:
                return True
            elif needed_type == 'odd' and neighbor_state['odds'] > 0:
                return True
        return False

    def _close_doors_to_room(self, room_id):
        logging.info(f"Closing doors to room {room_id}")
        for neighbor in self.graph.adjacency.get(room_id, []):
            self._send_door_command(room_id, neighbor, False)
            self.graph.set_door_state(room_id, neighbor, False)
        for room in self.graph.rooms:
            if room_id in self.graph.adjacency.get(room, []):
                self._send_door_command(room, room_id, False)
                self.graph.set_door_state(room, room_id, False)

    def _monitor_room_after_closure(self, room_id):
        trapped_marsamis = [m for m, data in self.marsami_tracking.items() if data['room'] == room_id and data['active']]
        time.sleep(0.6)
        with self.decision_condition:
            for marsami in trapped_marsamis:
                current_room = self.marsami_tracking[marsami]['room']
                if current_room != room_id:
                    logging.info(f"Marsami {marsami} moved from {room_id} to {current_room}. Doors not closed in time.")
                    self._open_doors_to_room(room_id)
                    return
            logging.info(f"Room {room_id} successfully trapped Marsamis.")
            self._send_score(room_id, "positive")
            self._open_doors_to_room(room_id)

    def _open_doors_to_room(self, room_id):
        logging.info(f"Opening doors to room {room_id}")
        for neighbor in self.graph.adjacency.get(room_id, []):
            self._send_door_command(room_id, neighbor, True)
            self.graph.set_door_state(room_id, neighbor, True)
        for room in self.graph.rooms:
            if room_id in self.graph.adjacency.get(room, []):
                self._send_door_command(room, room_id, True)
                self.graph.set_door_state(room, room_id, True)

    def _send_door_command(self, room1, room2, state):
        command = f"Door {room1} {room2} {'open' if state else 'close'}"
        self.mqtt_client.publish(self.mqtt_topic, command)
        logging.info(f"Sent command: {command}")

    def _send_score(self, room_id, score_type):
        command = f"Score {room_id} {score_type}"
        self.mqtt_client.publish(self.mqtt_topic, command)
        logging.info(f"Sent score command: {command}")

    def update_marsami_position(self, marsami, room, status):
        logging.info(f"Updating marsami {marsami} position to {room} with status {status}")
        with self.decision_condition:
            self.marsami_tracking[marsami] = {'room': room, 'active': status == 'active'}

    def mark_marsami_inactive(self, marsami):
        logging.info(f"Marking marsami {marsami} as inactive")
        with self.decision_condition:
            if marsami in self.marsami_tracking:
                self.marsami_tracking[marsami]['active'] = False

    def stop(self):
        self.running = False
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        logging.info("DecisionMaker stopped")
