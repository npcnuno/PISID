import decimal
import json
import time
import threading
from collections import defaultdict
import paho.mqtt.client as mqtt
import logging

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
        self.score_attempts = defaultdict(int)  # Track scoring attempts per room
        self.event_delays = event_delays
        self.event_thread = None
        self.sound_thread = None
        self.score_thread = None
        self.running = True
        self.setup_lock = threading.Lock()
        self.abnormal_sound_handling = threading.Event()
        self.mov_event = threading.Event()
        
        self._load_setup_variables()
        self._connect_mqtt_with_retry()
        
        self._send_close_all_doors()
        for room in self.graph.rooms:
            self._send_score(room)
        time.sleep(0.2)

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

    def _load_setup_variables(self):
        with self.setup_lock:
            conn = self.cloud_pool.get_connection()
            try:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("SELECT normalnoise, noisevartoleration, timemarsamilive FROM setupmaze LIMIT 1")
                    result = cursor.fetchone()
                    if result:
                        self.normalnoise = result['normalnoise']
                        self.noisevartoleration = result['noisevartoleration']
                        self.timemarsamilive = result['timemarsamilive']
                    else:
                        self.normalnoise = 50
                        self.noisevartoleration = 10
                        self.timemarsamilive = 300
                        logging.warning("No setup data found, using default values")
            except Exception as e:
                logging.error(f"Error loading setup variables: {e}")
                self.normalnoise = 50
                self.noisevartoleration = 10
                self.timemarsamilive = 300
            finally:
                conn.close()

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
        if self.check_all_marsami_initial():
            self.start_game()
        self.mov_event.set()

    def check_all_marsami_initial(self):
        with self.setup_lock:
            return all(data['status'] == 0 for data in self.marsami_tracking.values())

    def start_game(self):
        self._send_close_all_doors()
        for room in self.graph.rooms:
            if any(data['room'] == room and data['status'] == 1 for data in self.marsami_tracking.values()):
                self._send_score(room)
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
        positive_delays = [d for d in self.event_delays if d >= 0]
        D = max(positive_delays) if positive_delays else 2.0
        logging.info(f"Checking scoring with D={D}")
        current_time = time.time()
        for room_id in self.graph.rooms:
            state = self.graph.get_room_state(room_id)
            logging.info(f"Room {room_id}: odds={state['odds']}, evens={state['evens']}, attempts={self.score_attempts[room_id]}")
            # Room is balanced if odds == evens or both are 0
            is_balanced = state['odds'] == state['evens'] or (state['odds'] == 0 and state['evens'] == 0)
            if is_balanced and self.score_attempts[room_id] < 3:
                if any(data['room'] == room_id and data['status'] == 1 for data in self.marsami_tracking.values()):
                    last_movement_time = self.graph.last_movement_time.get(room_id)
                    if last_movement_time:
                        stability_time = current_time - last_movement_time.timestamp()
                        if stability_time > D:
                            logging.info(f"Scoring room {room_id}: stability_time={stability_time} > D={D}")
                            self._send_score(room_id)
                            self.score_attempts[room_id] += 1
                        else:
                            logging.info(f"Not scoring {room_id}: stability_time={stability_time} <= D={D}")
                    else:
                        logging.info(f"Scoring room {room_id}: no last movement time")
                        self._send_score(room_id)
                        self.score_attempts[room_id] += 1
                else:
                    logging.info(f"Not scoring {room_id}: no active marsamis")
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
                # Check total lifetime
                if current_time - data['start_time'] > self.timemarsamilive:
                    self._kill_marsami(marsami_id, current_time, "exceeded lifetime")
                # Check moving time (time since last move)
                if data['last_move_time']:
                    time_since_move = current_time - data['last_move_time']
                    if time_since_move > 200:
                        self._kill_marsami(marsami_id, current_time, "stationary for over 200 seconds")
                # Check stationary time for active marsamis
                if data['status'] == 1 and data['last_move_time']:
                    time_stationary = current_time - data['last_move_time']
                    if time_stationary > 200:
                        self._kill_marsami(marsami_id, current_time, "stationary for over 200 seconds")
                # If no movement recorded, assume moving since start
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
            del self.marsami_tracking[marsami_id]
            logging.info(f"Killed marsami {marsami_id} due to {reason}")

    def _manage_doors(self):
        if self.abnormal_sound_handling.is_set():
            return
        desired_open_doors = set()
        rooms = list(self.graph.rooms.keys())
        for room_id in rooms:
            state = self.graph.get_room_state(room_id)
            deficit = state['odds'] - state['evens']
            total_marsami = state['odds'] + state['evens']
            for neighbor in self.graph.adjacency.get(room_id, []):
                neighbor_state = self.graph.get_room_state(neighbor)
                neighbor_deficit = neighbor_state['odds'] - neighbor_state['evens']
                neighbor_total = neighbor_state['odds'] + neighbor_state['evens']
                if (deficit > 0 and neighbor_deficit < 0) or (deficit < 0 and neighbor_deficit > 0) or \
                   (deficit != 0 and neighbor_deficit == 0) or (deficit == 0 and neighbor_deficit != 0) or \
                   (total_marsami > 0 and neighbor_total < total_marsami):
                    door_key = (room_id, neighbor)
                    desired_open_doors.add(door_key)
        for door_key in self.graph.door_states:
            room1, room2 = door_key
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
