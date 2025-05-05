import os
import time
import threading
import queue
import json
import paho.mqtt.client as mqtt
from mysql.connector import pooling
from datetime import datetime

# Global variables
global current_sound
current_sound = None
sound_lock = threading.Lock()

# Environment variables
PLAYER_ID = os.getenv("PLAYER_ID", "33")
FRESH_START = os.getenv("FRESH_START", "False").lower() == "true"
MQTT_BROKER_TO_SEND_COMMANDS = os.getenv("MQTT_BROKER_TO_SEND_COMMANDS", "broker.eqmx.io")
MQTT_TOPIC_TO_SEND_COMMANDS = os.getenv("MQTT_TOPIC_TO_SEND_COMMANDS", "pisid_mazeact")
MQTT_BROKER_TO_LISTEN_FOR_UPDATES = os.getenv("MQTT_BROKER_TO_LISTEN_FOR_UPDATES", "test.mosquitto.org")
MQTT_TOPICS_SUFIX = os.getenv("MQTT_TOPIC_SUFIX", "_confirmed")
MQTT_TOPIC_TO_LISTEN_FOR_UPDATES = os.getenv("MQTT_TOPIC_TO_LISTEN_FOR_UPDATES", {
    "mazesound": f"pisid_mazesound_{PLAYER_ID}{MQTT_TOPICS_SUFIX}",
    "mazemov": f"pisid_mazemov_{PLAYER_ID}{MQTT_TOPICS_SUFIX}"
})

DEFAULT_CLOUD_DB_CONFIG = {
    "host": "194.210.86.10",
    "user": "aluno",
    "password": "aluno",
    "database": "maze",
    "port": 3306
}

DEFAULT_LOCAL_DB_CONFIG = {
    "host": "mysql",
    "port": 3306,
    "user": "labuser",
    "password": "labpass",
    "database": "mydb"
}

def get_db_config(prefix, default_config):
    config = default_config.copy()
    for key in config:
        env_var = f"{prefix}_{key.upper()}"
        if env_var in os.environ:
            config[key] = os.environ[env_var]
    return config

# Load configurations from environment variables or use defaults
cloud_db_config = get_db_config("CLOUD_DB", DEFAULT_CLOUD_DB_CONFIG)
local_db_config = get_db_config("LOCAL_DB", DEFAULT_LOCAL_DB_CONFIG)

# Set up connection pools
cloud_pool = pooling.MySQLConnectionPool(pool_name="cloud_pool", pool_size=5, **cloud_db_config)
local_pool = pooling.MySQLConnectionPool(pool_name="local_pool", pool_size=5, **local_db_config)

class Graph:
    def __init__(self):
        self.rooms = {}
        self.adjacency = {}
        self.door_states = {}
        self.lock = threading.RLock()

    def add_room(self, room_id):
        with self.lock:
            if room_id not in self.rooms:
                self.rooms[room_id] = {'odds': 0, 'evens': 0, 'points': 0}
                self.adjacency[room_id] = []

    def add_corridor(self, room1, room2):
        with self.lock:
            self.adjacency[room1].append(room2)
            self.adjacency[room2].append(room1)
            key = frozenset({room1, room2})
            self.door_states[key] = False

    def update_room(self, room_id, odds, evens, points, marsami=None, is_new_room=False):
        with self.lock:
            if room_id in self.rooms:
                self.rooms[room_id].update({'odds': odds, 'evens': evens, 'points': points})
                if marsami is not None and is_new_room:
                    print(f"Marsami {marsami} moved to room {room_id} (Graph updated: odds={odds}, evens={evens})")

    def get_room_state(self, room_id):
        with self.lock:
            return self.rooms.get(room_id, {'odds': 0, 'evens': 0, 'points': 0})

    def get_door_state(self, room1, room2):
        with self.lock:
            key = frozenset({room1, room2})
            return self.door_states.get(key, False)

    def set_door_state(self, room1, room2, state):
        with self.lock:
            key = frozenset({room1, room2})
            if key in self.door_states:
                self.door_states[key] = state

class DatabaseManager:
    def __init__(self, graph, player_id, fresh_start):
        self.graph = graph
        self.player_id = player_id
        self.fresh_start = fresh_start
        self.game_id = None
        self.number_marsamis = None
        self.marsami_positions = {}
        self._load_maze_structure()
        self._load_initial_state()

    def _load_maze_structure(self):
        conn = cloud_pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT Rooma FROM corridor UNION SELECT DISTINCT Roomb FROM corridor")
                for (room_id,) in cursor.fetchall():
                    self.graph.add_room(room_id)
                cursor.execute("SELECT Rooma, Roomb FROM corridor")
                for room1, room2 in cursor.fetchall():
                    self.graph.add_corridor(room1, room2)
        except Exception as e:
            print(f"Error loading maze structure: {e}")
        finally:
            conn.close()

    def _fetch_latest_game_id(self):
        conn = local_pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(idJogo) as latest_game FROM MedicaoPassagem WHERE idJogo IN "
                              "(SELECT idJogo FROM Jogo WHERE player_id = %s)", (self.player_id,))
                result = cursor.fetchone()
                return result[0] if result and result[0] else None
        except Exception as e:
            print(f"Error fetching latest game ID: {e}")
            return None
        finally:
            conn.close()

    def _load_initial_state(self):
        if self.fresh_start:
            conn = cloud_pool.get_connection()
            try:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("SELECT numbermarsamis FROM setupmaze LIMIT 1")
                    result = cursor.fetchone()
                    self.number_marsamis = result['numbermarsamis'] if result else 0
            except Exception as e:
                print(f"Error loading setupmaze: {e}")
            finally:
                conn.close()
        else:
            self.game_id = self._fetch_latest_game_id()
            if not self.game_id:
                print("No existing game found, treating as fresh start.")
                self.fresh_start = True
                return

            conn = local_pool.get_connection()
            try:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("SELECT numbermarsamis FROM setupmaze LIMIT 1")
                    result = cursor.fetchone()
                    self.number_marsamis = result['numbermarsamis'] if result else 0

                    cursor.execute("""
                        SELECT m1.marsami, m1.salaDestino
                        FROM MedicaoPassagem m1
                        INNER JOIN (
                            SELECT marsami, MAX(hora) as max_hora
                            FROM MedicaoPassagem
                            WHERE idJogo = %s AND marsami BETWEEN 1 AND %s
                            GROUP BY marsami
                        ) m2 ON m1.marsami = m2.marsami AND m1.hora = m2.max_hora
                        WHERE m1.idJogo = %s
                    """, (self.game_id, self.number_marsamis, self.game_id))
                    latest_movements = cursor.fetchall()

                    self.marsami_positions = {movement['marsami']: movement['salaDestino'] for movement in latest_movements}

                    room_counts = {room_id: {'odds': 0, 'evens': 0} for room_id in self.graph.rooms}
                    for movement in latest_movements:
                        marsami = movement['marsami']
                        room_id = movement['salaDestino']
                        if marsami % 2 == 0:
                            room_counts[room_id]['evens'] += 1
                        else:
                            room_counts[room_id]['odds'] += 1
                    for room_id, counts in room_counts.items():
                        self.graph.update_room(room_id, counts['odds'], counts['evens'], 0)
            except Exception as e:
                print(f"Error loading initial state: {e}")
            finally:
                conn.close()

    def process_movement(self, mysqlID):
        conn = local_pool.get_connection()
        try:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT marsami, salaDestino, hora FROM MedicaoPassagem WHERE idMedicao = %s", (mysqlID,))
                result = cursor.fetchone()
                if result:
                    marsami = result['marsami']
                    new_room = result['salaDestino']
                    hora = result['hora']
                    with self.graph.lock:
                        old_room = self.marsami_positions.get(marsami)
                        if old_room:
                            old_state = self.graph.get_room_state(old_room)
                            if marsami % 2 == 0:
                                old_state['evens'] -= 1
                            else:
                                old_state['odds'] -= 1
                            self.graph.update_room(old_room, old_state['odds'], old_state['evens'], old_state['points'])
                            print(f"At {hora.strftime('%Y-%m-%d %H:%M:%S')}: Marsami {marsami} removed from room {old_room}")
                        self.marsami_positions[marsami] = new_room
                        new_state = self.graph.get_room_state(new_room)
                        if marsami % 2 == 0:
                            new_state['evens'] += 1
                        else:
                            new_state['odds'] += 1
                        self.graph.update_room(new_room, new_state['odds'], new_state['evens'], new_state['points'], marsami, True)
        except Exception as e:
            print(f"Error processing movement {mysqlID}: {e}")
        finally:
            conn.close()

    def update_room_states(self):
        while True:
            time.sleep(60)

class DecisionMaker:
    def __init__(self, graph):
        self.graph = graph
        self.mqtt = mqtt.Client()
        self.command_broker = MQTT_BROKER_TO_SEND_COMMANDS
        self.command_topic = MQTT_TOPIC_TO_SEND_COMMANDS
        self.last_sound = None

    def balance_rooms(self):
        while True:
            with sound_lock:
                if self.last_sound != current_sound:
                    print(f"Current sound in DecisionMaker: {current_sound}")
                    self.last_sound = current_sound
            try:
                desired_open_doors = set()
                rooms = list(self.graph.rooms.keys())
                for room_id in rooms:
                    state = self.graph.get_room_state(room_id)
                    deficit = state['odds'] - state['evens']
                    points = state['points']
                    if deficit == 0 and points < 3:
                        self._send_score(room_id)
                        continue
                    if deficit != 0:
                        for neighbor in self.graph.adjacency.get(room_id, []):
                            neighbor_state = self.graph.get_room_state(neighbor)
                            neighbor_deficit = neighbor_state['odds'] - neighbor_state['evens']
                            if deficit * neighbor_deficit < 0:
                                door_key = frozenset({room_id, neighbor})
                                desired_open_doors.add(door_key)

                for door_key in self.graph.door_states:
                    room1, room2 = door_key
                    current_state = self.graph.get_door_state(room1, room2)
                    if door_key in desired_open_doors:
                        if not current_state:
                            self._send_door_command(room1, room2, True)
                    else:
                        if current_state:
                            self._send_door_command(room1, room2, False)
                time.sleep(1)
            except Exception as e:
                print(f"Decision error: {e}")

    def _send_score(self, room_id):
        print("sent score")

    def _send_door_command(self, room1, room2, open_state):
        print(f"{'Opening' if open_state else 'Closing'} door between {room1} and {room2}")

class MazeController:
    def __init__(self, player_id, fresh_start):
        self.graph = Graph()
        self.db_manager = DatabaseManager(self.graph, player_id, fresh_start)
        self.decision_maker = DecisionMaker(self.graph)
        self.client = mqtt.Client()
        self.client.on_message = self._on_mqtt_message
        self.client.connect("test.mosquitto.org", 1883, 60)
        self.client.subscribe(f"pisid_mazemov_{player_id}_confirmed")
        self.client.subscribe(f"pisid_mazesound_{player_id}_confirmed")
        self.update_queue = queue.Queue()
        self.sound_queue = queue.Queue()

    def start(self):
        threading.Thread(target=self.db_manager.update_room_states, daemon=True).start()
        threading.Thread(target=self.decision_maker.balance_rooms, daemon=True).start()
        threading.Thread(target=self.update_consumer, daemon=True).start()
        threading.Thread(target=self.sound_consumer, daemon=True).start()
        self.client.loop_start()

    def _on_mqtt_message(self, client, userdata, msg):
        if msg.topic == f"pisid_mazemov_{self.db_manager.player_id}_confirmed":
            try:
                payload = json.loads(msg.payload.decode('utf-8'))
                mysqlID = payload.get("mysqlID")
                if mysqlID:
                    self.update_queue.put(mysqlID)
            except Exception as e:
                print(f"Error processing movement MQTT message: {e}")
        elif msg.topic == f"pisid_mazesound_{self.db_manager.player_id}_confirmed":
            try:
                payload = json.loads(msg.payload.decode('utf-8'))
                mysqlID = payload.get("mysqlID")
                if mysqlID:
                    self.sound_queue.put(mysqlID)
            except Exception as e:
                print(f"Error processing sound MQTT message: {e}")

    def update_consumer(self):
        while True:
            mysqlID = self.update_queue.get()
            self.db_manager.process_movement(mysqlID)
            self.update_queue.task_done()

    def sound_consumer(self):
        while True:
            mysqlID = self.sound_queue.get()
            self.process_sound_event(mysqlID)
            self.sound_queue.task_done()

    def process_sound_event(self, mysqlID):
        conn = local_pool.get_connection()
        try:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT sound, hora FROM Sound WHERE idSound = %s LIMIT 1", (mysqlID,))
                result = cursor.fetchone()
                if result:
                    global current_sound
                    with sound_lock:
                        current_sound = result['sound']
                        print(f"At {result['hora'].strftime('%Y-%m-%d %H:%M:%S')}: Current sound updated to {current_sound}")
                else:
                    print(f"No sound event found for idSound {mysqlID}")
        except Exception as e:
            print(f"Error processing sound event {mysqlID}: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    controller = MazeController(PLAYER_ID, FRESH_START)
    controller.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
