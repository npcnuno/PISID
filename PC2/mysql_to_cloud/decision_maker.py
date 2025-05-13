import threading
import time
import logging
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DecisionMaker:
    STABILITY_THRESHOLD = 2.0  # seconds

    def __init__(self, graph, sound_condition, current_sound, decision_condition, player_id, cloud_pool, mqtt_broker, mqtt_topic, marsami_tracking):
        self.graph = graph
        self.sound_condition = sound_condition
        self.current_sound = current_sound
        self.decision_condition = decision_condition
        self.player_id = player_id
        self.cloud_pool = cloud_pool
        self.mqtt_broker = mqtt_broker
        self.mqtt_topic = mqtt_topic
        self.marsami_tracking = {m: {'room': None, 'active': True, 'last_move_time': None, 'stuck': False} for m in marsami_tracking}        
        self.running = True
        self.all_marsamis_in_maze = False
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.connect(mqtt_broker)
        self.mqtt_client.loop_start()
        self.abnormal_sound_handling = threading.Event()
        self.score_signal = threading.Event()
        self._load_setup_variables()
        logging.info("DecisionMaker initialized")

    def _load_setup_variables(self):
        with self.decision_condition:
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
                self.noisevartoleration = 19.5
            finally:
                conn.close()

    def start(self):
        self.door_thread = threading.Thread(target=self.run_door_manager)
        self.score_thread = threading.Thread(target=self.run_scoring)
        self.noise_thread = threading.Thread(target=self.monitor_sound)
        self.door_thread.daemon = True
        self.score_thread.daemon = True
        self.noise_thread.daemon = True
        self.door_thread.start()
        self.score_thread.start()
        self.noise_thread.start()
        logging.info("DecisionMaker threads started")

    def run_door_manager(self):
        while self.running:
            time.sleep(0.5)
            with self.decision_condition:
                if not self.abnormal_sound_handling.is_set():
                    self._manage_doors()

    def run_scoring(self):
        while self.running:
            self.score_signal.wait(timeout=1)  # Check every second if no signal
            if not self.running:
                break
            with self.decision_condition:
                if self.all_marsamis_stuck():
                    self._final_scoring()
                else:
                    self._check_scoring()
            self.score_signal.clear()

    def monitor_sound(self):
        while self.running:
            with self.sound_condition:
                self.sound_condition.wait(timeout=1)
                if not self.running:
                    break
                current_sound = self.current_sound[0]
                if current_sound is not None:
                    lower_bound = 1.5 + float(self.normalnoise) - float(self.noisevartoleration)
                    upper_bound = float(self.normalnoise) + float(self.noisevartoleration) - 1.5
                    if not (lower_bound <= current_sound <= upper_bound):
                        self._handle_abnormal_sound()
                        self.score_signal.set()
    
    def all_marsamis_stuck(self):
        with self.decision_condition:
            active_marsamis = [m for m, data in self.marsami_tracking.items() if data['active']]
            if not active_marsamis:
                return False  # No active Marsamis
            return all(self.marsami_tracking[m]['stuck'] for m in active_marsamis)

    def mark_marsami_stuck(self, marsami):
        with self.decision_condition:
            if marsami in self.marsami_tracking:
                self.marsami_tracking[marsami]['stuck'] = True
                logging.info(f"Marked marsami {marsami} as stuck")
                self.score_signal.set()  # Trigger scoring check

    def _final_scoring(self):
        with self.decision_condition:
            for room_id in self.graph.rooms:
                state = self.graph.get_room_state(room_id)
                if state['odds'] == state['evens'] and state['odds'] > 0:
                    self._send_score(room_id, "positive")
                    logging.info(f"Final scoring for room {room_id}")
            if self.all_marsamis_stuck():
                logging.info("All Marsamis are stuck. Game has ended.")
                self.stop()  # Optionally stop the DecisionMaker

    def _handle_abnormal_sound(self):
        self.abnormal_sound_handling.set()
        self._send_close_all_doors()
        time.sleep(10)
        self._send_open_all_doors()
        self.abnormal_sound_handling.clear()

    def _all_marsamis_in_valid_rooms(self):
        return all(data['room'] in self.graph.rooms for m, data in self.marsami_tracking.items() if data['active'])

    def _manage_doors(self):
        if not self.all_marsamis_in_maze:
            if self._all_marsamis_in_valid_rooms():
                self.all_marsamis_in_maze = True
                logging.info("All Marsamis are now in valid rooms. Starting door management.")
            else:
                logging.info("Waiting for all Marsamis to enter valid rooms.")
                return

        for room_id in self.graph.rooms:
            state = self.graph.get_room_state(room_id)
            deficit = state['odds'] - state['evens']
            for neighbor in self.graph.adjacency.get(room_id, []):
                neighbor_state = self.graph.get_room_state(neighbor)
                neighbor_deficit = neighbor_state['odds'] - neighbor_state['evens']
                if (deficit > 0 and neighbor_deficit < 0) or (deficit < 0 and neighbor_deficit > 0):
                    self._send_door_command(room_id, neighbor, True)
                    self.graph.set_door_state(room_id, neighbor, True)
                else:
                    self._send_door_command(room_id, neighbor, False)
                    self.graph.set_door_state(room_id, neighbor, False)

            # Signal scoring if room is balanced and stable
            if state['odds'] == state['evens'] and state['odds'] > 0:
                last_move_time = self.graph.last_movement_time.get(room_id)
                if last_move_time and (datetime.now() - last_move_time).total_seconds() > self.STABILITY_THRESHOLD:
                    self.score_signal.set()

    def _check_scoring(self):
        current_time = datetime.now()
        for room_id in self.graph.rooms:
            state = self.graph.get_room_state(room_id)
            if state['odds'] == state['evens'] and state['odds'] > 0:
                last_move_time = self.graph.last_movement_time.get(room_id)
                if last_move_time and (current_time - last_move_time).total_seconds() > self.STABILITY_THRESHOLD:
                    self._close_doors_to_room(room_id)
                    threading.Thread(target=self._monitor_room_after_closure, args=(room_id,), daemon=True).start()

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

    def _close_doors_to_room(self, room_id):
        logging.info(f"Closing doors to room {room_id}")
        for neighbor in self.graph.adjacency.get(room_id, []):
            self._send_door_command(room_id, neighbor, False)
            self.graph.set_door_state(room_id, neighbor, False)
        for room in self.graph.rooms:
            if room_id in self.graph.adjacency.get(room, []):
                self._send_door_command(room, room_id, False)
                self.graph.set_door_state(room, room_id, False)

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
        action = 'OpenDoor' if state else 'CloseDoor'
        command = f"{{Type: {action}, Player:{self.player_id}, RoomOrigin: {room1}, RoomDestiny: {room2}}}"
        self.mqtt_client.publish(self.mqtt_topic, command)
        logging.info(f"Sent command: {command}")

    def _send_score(self, room_id, score_type):
        command = f"{{Type: Score, Player:{self.player_id}, Room: {room_id}}}"
        self.mqtt_client.publish(self.mqtt_topic, command)
        logging.info(f"Sent score command: {command}")

    def _send_close_all_doors(self):
        command = f"{{Type: CloseAllDoor, Player:{self.player_id}}}"
        self.mqtt_client.publish(self.mqtt_topic, command)
        logging.info("Sent close all doors command")

    def _send_open_all_doors(self):
        command = f"{{Type: OpenAllDoor, Player:{self.player_id}}}"
        self.mqtt_client.publish(self.mqtt_topic, command)
        logging.info("Sent open all doors command")

    def update_marsami_position(self, marsami, room, status):
        with self.decision_condition:
            self.marsami_tracking[marsami] = {
                'room': room,
                'active': status == 'active',
                'last_move_time': datetime.now()
            }
            if status == 'active' and room not in self.graph.rooms:
                logging.warning(f"Active Marsami {marsami} is in invalid room {room}")

    def mark_marsami_inactive(self, marsami):
        with self.decision_condition:
            if marsami in self.marsami_tracking:
                self.marsami_tracking[marsami]['active'] = False
                logging.info(f"Marked marsami {marsami} as inactive")

    def stop(self):
        self.running = False
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        logging.info("DecisionMaker stopped")