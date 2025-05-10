import json
import time
import threading
from collections import defaultdict
import paho.mqtt.client as mqtt
import logging
from datetime import datetime, timezone  # Added for UTC handling

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DecisionMaker:
    def __init__(self, graph, sound_condition, current_sound, decision_condition, player_id, cloud_pool, MQTT_BROKER_TO_SEND_COMMANDS, MQTT_TOPIC_TO_SEND_COMMANDS, event_delays):
        self.graph = graph
        self.sound_condition = sound_condition
        self.current_sound = current_sound
        self.decision_condition = decision_condition
        self.player_id = int(player_id)
        self.cloud_pool = cloud_pool
        self.mqtt = mqtt.Client()
        self.command_broker = MQTT_BROKER_TO_SEND_COMMANDS
        self.command_topic = MQTT_TOPIC_TO_SEND_COMMANDS
        self.marsami_tracking = defaultdict(lambda: {'start_time': None, 'room': None, 'status': None, 'last_move_time': None})
        self.score_attempts = defaultdict(int)
        self.event_delays = event_delays
        self.event_thread = None
        self.sound_thread = None
        self.score_thread = None
        self.running = True
        self.setup_lock = threading.Lock()
        self.abnormal_sound_handling = threading.Event()
        self.mov_event = threading.Event()
        self.game_started = False
        self.numberofmarsamis = 0
        self.room_locks = defaultdict(bool)

        self._load_setup_variables()
        self._connect_mqtt_with_retry()
        
        self._send_close_all_doors()
        self._initialize_graph_with_marsamis()
        time.sleep(0.2)

    def _load_setup_variables(self):
        with self.setup_lock:
            conn = self.cloud_pool.get_connection()
            try:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("SELECT normalnoise, noisevartoleration, timemarsamilive, numbermarsamis FROM setupmaze LIMIT 1")
                    result = cursor.fetchone()
                    if result:
                        self.normalnoise = result['normalnoise']
                        self.noisevartoleration = result['noisevartoleration']
                        self.timemarsamilive = result['timemarsamilive']
                        self.numberofmarsamis = result['numbermarsamis']
                    else:
                        self.normalnoise = 50
                        self.noisevartoleration = 10
                        self.timemarsamilive = 300
                        self.numberofmarsamis = 30
                        logging.warning("No setup data found, using default values")
            except Exception as e:
                logging.error(f"Error loading setup variables: {e}")
                self.normalnoise = 50
                self.noisevartoleration = 10
                self.timemarsamilive = 300
                self.numberofmarsamis = 30
            finally:
                conn.close()

    def _connect_mqtt_with_retry(self, max_retries=5, retry_delay=5):
        for attempt in range(max_retries):
            try:
                self.mqtt.connect(self.command_broker, 1883, 60)
                self.mqtt.loop_start()
                return
            except Exception as e:
                logging.error(f"Error connecting to MQTT broker: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise

    def start(self):
        self.event_thread = threading.Thread(target=self.balance_rooms)
        self.sound_thread = threading.Thread(target=self.monitor_sound)
        self.score_thread = threading.Thread(target=self.score_rooms)
        self.event_thread.daemon = True
        self.sound_thread.daemon = True
        self.score_thread.daemon = True
        self.event_thread.start()
        self.sound_thread.start()
        self.score_thread.start()

    def stop(self):
        self.running = False
        self.mqtt.loop_stop()
        self.mqtt.disconnect()

    def _initialize_graph_with_marsamis(self):
        pass  # Placeholder

    def update_marsami_position(self, marsami_id, new_room, status):
        current_time = time.time()
        with self.setup_lock:
            if self.marsami_tracking[marsami_id]['start_time'] is None:
                self.marsami_tracking[marsami_id]['start_time'] = current_time
            old_room = self.marsami_tracking[marsami_id]['room']
            self.marsami_tracking[marsami_id]['room'] = new_room
            self.marsami_tracking[marsami_id]['status'] = status
            if old_room != new_room:
                self.marsami_tracking[marsami_id]['last_move_time'] = current_time
                # Check if new_room was locked and is now unbalanced
                state = self.graph.get_room_state(new_room)
                if self.room_locks[new_room] and state['odds'] != state['evens']:
                    self.room_locks[new_room] = False
                    logging.info(f"Unlocked room {new_room} due to imbalance")
        if not self.game_started and self.check_all_marsami_initial():
            self.game_started = True
            self.start_game()
        self.mov_event.set()

    def check_all_marsami_initial(self):
        with self.setup_lock:
            return len(self.marsami_tracking) == self.numberofmarsamis
    
    def start_game(self):
        logging.info("Starting the game: all Marsamis are positioned.")
        logging.info(f"Number of Marsamis: {len(self.marsami_tracking)} / {self.numberofmarsamis}")
        self._send_close_all_doors()
        time.sleep(4)
        positive_delays = [d for d in self.event_delays if d >= 0]
        D = max(positive_delays) if positive_delays else 2.0
        logging.info(f"Waiting for {D} seconds before initial scoring")
        time.sleep(D)
        for room_id in self.graph.rooms:
            state = self.graph.get_room_state(room_id)
            if state['odds'] == state['evens'] and (state['odds'] > 0 or state['evens'] > 0):
                logging.info(f"Initial scoring on room {room_id}: odds={state['odds']}, evens={state['evens']}")
                self._send_score(room_id)
                self.score_attempts[room_id] += 1
                self.room_locks[room_id] = True
                logging.info(f"Locked room {room_id} after initial scoring")
        self._send_open_all_doors()

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

    def check_scoring(self):
        if not self.game_started:
            return
        positive_delays = [d for d in self.event_delays if d >= 0]
        D = max(positive_delays) if positive_delays else 2.0
        if D > 4.0:
            D = 4.0
        logging.info(f"Checking scoring with D={D}")
        current_time = time.time()
        for room_id in self.graph.rooms:
            state = self.graph.get_room_state(room_id)
            logging.info(f"Room {room_id}: odds={state['odds']}, evens={state['evens']}, attempts={self.score_attempts[room_id]}")
            is_balanced = state['odds'] == state['evens']
            if not (state['odds'] > 0 and state['evens'] > 0):
                is_balanced = False 
            has_marsamis = state['odds'] + state['evens'] > 0
            if is_balanced and self.score_attempts[room_id] < 3:
                if not has_marsamis:
                    logging.info(f"Skipping scoring room {room_id}: balanced with zero Marsamis")
                else:
                    last_movement_time = self.graph.last_movement_time.get(room_id)
                    if last_movement_time is None:
                        logging.info(f"Scoring room {room_id}: no movement recorded")
                        self._send_score(room_id)
                        self.score_attempts[room_id] += 1
                        self.room_locks[room_id] = True
                        logging.info(f"Locked room {room_id} after scoring")
                    else:
                        stability_time = current_time - last_movement_time.timestamp()
                        if stability_time > D:
                            logging.info(f"Scoring room {room_id}: stability_time={stability_time} > D={D}")
                            self._send_score(room_id)
                            self.score_attempts[room_id] += 1
                            self.room_locks[room_id] = True
                            logging.info(f"Locked room {room_id} after scoring")
                        else:
                            logging.info(f"Not scoring {room_id}: stability_time={stability_time} <= D={D}")
            else:
                if self.score_attempts[room_id] >= 3:
                    logging.info(f"Not scoring {room_id}: maximum attempts (3) reached")
                else:
                    logging.info(f"Not scoring {room_id}: not balanced (odds={state['odds']}, evens={state['evens']})")

    def score_rooms(self):
        while self.running:
            self.mov_event.wait()
            if not self.running:
                break
            logging.info("Score thread triggered by mov_event")
            self.check_scoring()
            self.mov_event.clear()

    def balance_rooms(self):
        while self.running:
            with self.decision_condition:
                self.decision_condition.wait(timeout=1)
                if not self.running:
                    break
            try:
                self._manage_marsami_lifecycles()
                self._manage_doors()
                self.check_scoring()
            except Exception as e:
                logging.error(f"Decision error: {e}")

    def _handle_abnormal_sound(self):
        self.abnormal_sound_handling.set()
        self._send_close_all_doors()
        time.sleep(5)
        self._send_open_all_doors()
        self.abnormal_sound_handling.clear()

    def _manage_marsami_lifecycles(self):
        current_time = time.time()
        for marsami_id, data in list(self.marsami_tracking.items()):
            if data['start_time']:
                if current_time - data['start_time'] > self.timemarsamilive:
                    self._kill_marsami(marsami_id, current_time, "exceeded lifetime")
                if data['last_move_time']:
                    time_since_move = current_time - data['last_move_time']
                    if time_since_move > 200:
                        self._kill_marsami(marsami_id, current_time, "stationary for over 200 seconds")
                elif data['status'] == 1 and not data['last_move_time']:
                    time_moving = current_time - data['start_time']
                    if time_moving > 200:
                        self._kill_marsami(marsami_id, current_time, "moving for over 200 seconds")

    def _kill_marsami(self, marsami_id, current_time, reason):
        with self.setup_lock:
            data = self.marsami_tracking.get(marsami_id, {})
            room_id = data.get('room')
            if room_id:
                state = self.graph.get_room_state(room_id)
                if marsami_id % 2 == 0:
                    state['evens'] = max(0, state['evens'] - 1)
                else:
                    state['odds'] = max(0, state['odds'] - 1)
                self.graph.update_room(room_id, state['odds'], state['evens'], state['points'])
                if self.room_locks[room_id] and state['odds'] != state['evens']:
                    self.room_locks[room_id] = False
                    logging.info(f"Unlocked room {room_id} due to imbalance after Marsami death")
            del self.marsami_tracking[marsami_id]
            logging.info(f"Killed marsami {marsami_id} due to {reason}")

    def _manage_doors(self):
        if self.abnormal_sound_handling.is_set():
            return
        desired_open_doors = set()
        rooms = list(self.graph.rooms.keys())
        for room_id in rooms:
            if self.room_locks[room_id]:
                continue
            state = self.graph.get_room_state(room_id)
            deficit = state['odds'] - state['evens']
            total_marsami = state['odds'] + state['evens']
            for neighbor in self.graph.adjacency.get(room_id, []):
                if self.room_locks[neighbor]:
                    continue
                neighbor_state = self.graph.get_room_state(neighbor)
                neighbor_deficit = neighbor_state['odds'] - neighbor_state['evens']
                neighbor_total = neighbor_state['odds'] + neighbor_state['evens']
                deficit_diff = abs(deficit - neighbor_deficit)
                if (deficit > 0 and neighbor_deficit < 0) or (deficit < 0 and neighbor_deficit > 0) or \
                   (deficit != 0 and neighbor_deficit == 0) or (deficit == 0 and neighbor_deficit != 0) or \
                   (total_marsami > 0 and neighbor_total < total_marsami and deficit_diff > 2):
                    door_key = (room_id, neighbor)
                    desired_open_doors.add(door_key)
        for door_key in self.graph.door_states:
            room1, room2 = door_key
            if self.room_locks[room1] or self.room_locks[room2]:
                current_state = self.graph.get_door_state(room1, room2)
                if current_state:
                    self._send_door_command(room1, room2, False)
                    self.graph.set_door_state(room1, room2, False)
                continue
            current_state = self.graph.get_door_state(room1, room2)
            if door_key in desired_open_doors:
                if not current_state:
                    self._send_door_command(room1, room2, True)
                    self.graph.set_door_state(room1, room2, True)
            else:
                if current_state:
                    self._send_door_command(room1, room2, False)
                    self.graph.set_door_state(room1, room2, False)

    def _send_score(self, room_id):
        message = f"{{Type: Score, Player: {self.player_id}, Room: {room_id}}}"
        logging.info(f"Sending score for room {room_id}: {message}")
        result = self.mqtt.publish(self.command_topic, message)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.error(f"Failed to send score: {mqtt.error_string(result.rc)}")

    def _send_door_command(self, room1, room2, open_state):
        type_str = "OpenDoor" if open_state else "CloseDoor"
        message = f"{{Type: {type_str}, Player: {self.player_id}, RoomOrigin: {room1}, RoomDestiny: {room2}}}"
        self.mqtt.publish(self.command_topic, message)

    def _send_open_all_doors(self):
        message = f"{{Type: OpenAllDoor, Player: {self.player_id}}}"
        self.mqtt.publish(self.command_topic, message)

    def _send_close_all_doors(self):
        message = f"{{Type: CloseAllDoor, Player: {self.player_id}}}"
        self.mqtt.publish(self.command_topic, message)

    def on_message(self, client, userdata, message):
        payload = message.payload.decode()
        logging.info(f"Received message: {payload}")
        data = json.loads(payload)
        if data['Type'] == 'mov':
            self.update_marsami_position(data['marsami_id'], data['new_room'], data['status'])
            self.mov_event.set()
