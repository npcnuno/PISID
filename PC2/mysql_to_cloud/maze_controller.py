import os
import time
import threading
import queue
import json
import paho.mqtt.client as mqtt
from mysql.connector import pooling, Error as MySQLError
import logging
from graph import Graph 
from database_manager import DatabaseManager
from decision_maker import DecisionMaker
from flask import Flask, jsonify, render_template_string
from datetime import datetime,timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

class MazeController:
    def __init__(self):
        self.PLAYER_ID = os.getenv("PLAYER_ID", 33)
        self.FRESH_START = os.getenv("FRESH_START", "False").lower() == "true"
        self.MQTT_BROKER_TO_SEND_COMMANDS = os.getenv("MQTT_BROKER_TO_SEND_COMMANDS", "test.mosquitto.org")
        self.MQTT_TOPIC_TO_SEND_COMMANDS = os.getenv("MQTT_TOPIC_TO_SEND_COMMANDS", "pisid_mazeact")
        self.MQTT_BROKER_TO_LISTEN_FOR_UPDATES = os.getenv("MQTT_BROKER_TO_LISTEN_FOR_UPDATES", "test.mosquitto.org")
        self.MQTT_TOPICS_SUFIX = os.getenv("MQTT_TOPIC_SUFIX", "_confirmed")
        self.MQTT_TOPIC_TO_LISTEN_FOR_UPDATES = {
            "mazesound": f"pisid_mazesound_{self.PLAYER_ID}{self.MQTT_TOPICS_SUFIX}",
            "mazemov": f"pisid_mazemov_{self.PLAYER_ID}{self.MQTT_TOPICS_SUFIX}"
        }

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

        self.cloud_db_config = self.get_db_config("CLOUD_DB", DEFAULT_CLOUD_DB_CONFIG)
        self.local_db_config = self.get_db_config("LOCAL_DB", DEFAULT_LOCAL_DB_CONFIG)

        self.cloud_pool = self.create_db_pool("cloud_pool", self.cloud_db_config)
        self.local_pool = self.create_db_pool("local_pool", self.local_db_config)

        self.event_delays = []
        self.event_delays_lock = threading.Lock()

        self.graph = Graph()
        self.current_sound = [None]
        self.sound_condition = threading.Condition()
        self.decision_condition = threading.Condition()
        self.decision_maker = DecisionMaker(
            self.graph, 
            self.sound_condition, 
            self.current_sound, 
            self.decision_condition, 
            self.PLAYER_ID,
            self.cloud_pool,
            self.MQTT_BROKER_TO_SEND_COMMANDS, 
            self.MQTT_TOPIC_TO_SEND_COMMANDS,
            self.event_delays
        )
        self.db_manager = DatabaseManager(
            self.graph, 
            self.PLAYER_ID, 
            self.FRESH_START, 
            self.cloud_pool, 
            self.local_pool, 
            self.decision_maker,
            self.event_delays,
            self.event_delays_lock
        )
        self.client = mqtt.Client()
        self.client.on_message = self._on_mqtt_message
        self.client.on_connect = self._on_mqtt_connect
        self.client.on_disconnect = self._on_mqtt_disconnect
        self.update_queue = queue.Queue()
        self.sound_queue = queue.Queue()
        self.running = True

        # Flask routes
        @app.route('/')
        def index():
            return render_template_string('''
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Marsamis in Rooms</title>
                    <style>
                        .room { margin: 10px; padding: 10px; border: 1px solid #ccc; }
                        .odds { color: red; }
                        .evens { color: blue; }
                    </style>
                </head>
                <body>
                    <h1>Marsamis in Rooms</h1>
                    <div id="rooms"></div>
                    <script>
                        function updateRooms() {
                            fetch('/data')
                                .then(response => response.json())
                                .then(data => {
                                    const roomsDiv = document.getElementById('rooms');
                                    roomsDiv.innerHTML = '';
                                    data.forEach(room => {
                                        const roomDiv = document.createElement('div');
                                        roomDiv.className = 'room';
                                        roomDiv.innerHTML = `
                                            <h2>Room ${room.id}</h2>
                                            <p class="odds">Odds: ${room.odds}</p>
                                            <p class="evens">Evens: ${room.evens}</p>
                                        `;
                                        roomsDiv.appendChild(roomDiv);
                                    });
                                });
                        }
                        setInterval(updateRooms, 1000);
                        updateRooms();
                    </script>
                </body>
                </html>
            ''')

        @app.route('/data')
        def data():
            with self.graph.lock:
                room_states = {room_id: self.graph.get_room_state(room_id) for room_id in sorted(self.graph.rooms.keys())}
            return jsonify([{'id': room_id, 'odds': state['odds'], 'evens': state['evens']} for room_id, state in room_states.items()])

    def get_db_config(self, prefix, default_config):
        config = default_config.copy()
        for key in config:
            env_var = f"{prefix}_{key.upper()}"
            if env_var in os.environ:
                config[key] = os.environ[env_var]
        return config

    def create_db_pool(self, pool_name, db_config, max_retries=5, retry_delay=5):
        for attempt in range(max_retries):
            try:
                pool = pooling.MySQLConnectionPool(pool_name=pool_name, pool_size=5, **db_config)
                logger.info(f"Successfully created database pool: {pool_name}")
                return pool
            except MySQLError as e:
                logger.error(f"Error creating database pool {pool_name}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    raise

    def start(self):
        try:
            self.client.connect(self.MQTT_BROKER_TO_LISTEN_FOR_UPDATES, 1883, 60)
            self.client.subscribe(self.MQTT_TOPIC_TO_LISTEN_FOR_UPDATES["mazemov"])
            self.client.subscribe(self.MQTT_TOPIC_TO_LISTEN_FOR_UPDATES["mazesound"])
            self.client.loop_start()
        except Exception as e:
            logger.error(f"Error connecting to MQTT broker: {e}")
            return

        threading.Thread(target=self.decision_maker.start, daemon=True).start()
        threading.Thread(target=self.movement_consumer, daemon=True).start()
        threading.Thread(target=self.sound_consumer, daemon=True).start()

        # Start Flask server in a separate thread
        threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5003), daemon=True).start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Shutting down...")
            self.stop()

    def stop(self):
        self.running = False
        self.decision_maker.stop()
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("MazeController shut down gracefully")

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("Connected to MQTT broker")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def _on_mqtt_disconnect(self, client, userdata, rc):
        if rc != 0 and self.running:
            logger.warning("Unexpected disconnection from MQTT broker")

    def _on_mqtt_message(self, client, userdata, msg):
        try:
            if msg.topic == self.MQTT_TOPIC_TO_LISTEN_FOR_UPDATES["mazemov"]:
                payload = json.loads(msg.payload.decode('utf-8'))
                mysqlID = payload.get("mysqlID")
                if mysqlID:
                    self.update_queue.put(mysqlID)
            elif msg.topic == self.MQTT_TOPIC_TO_LISTEN_FOR_UPDATES["mazesound"]:
                payload = json.loads(msg.payload.decode('utf-8'))
                mysqlID = payload.get("mysqlID")
                if mysqlID:
                    self.sound_queue.put(mysqlID)
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}")

    def movement_consumer(self):
        while self.running:
            try:
                mysqlID = self.update_queue.get(timeout=1)
                self.db_manager.process_movement(mysqlID)
                with self.decision_condition:
                    self.decision_condition.notify_all()
                self.update_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in movement_consumer: {e}")

    def sound_consumer(self):
        while self.running:
            try:
                mysqlID = self.sound_queue.get(timeout=1)
                self.process_sound_event(mysqlID)
                with self.decision_condition:
                    self.decision_condition.notify_all()
                self.sound_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in sound_consumer: {e}")

    def process_sound_event(self, mysqlID):
        try:
            conn = self.local_pool.get_connection()
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT sound, hora FROM Sound WHERE idSound = %s LIMIT 1", (mysqlID,))
                result = cursor.fetchone()
                if result:
                    current_time = datetime.now() + timedelta(minutes=60)
                    event_time = result['hora']
                    delay = current_time - event_time
                    delay = delay.total_seconds()
                    if delay > 0:
                        with self.event_delays_lock:
                            self.event_delays.append(delay)
                            if len(self.event_delays) > 100:
                                self.event_delays.pop(0)
                    else:
                        logger.warning(f"Negative delay detected for sound event {mysqlID}: {delay} || {current_time} - {event_time} ")
                    with self.sound_condition:
                        self.current_sound[0] = result['sound']
                        logger.info(f"At {result['hora'].strftime('%Y-%m-%d %H:%M:%S')}: Current sound updated to {self.current_sound[0]}")
                        self.sound_condition.notify_all()
                else:
                    logger.warning(f"No sound event found for idSound {mysqlID}")
        except MySQLError as e:
            logger.error(f"Database error in process_sound_event: {e}")
        except Exception as e:
            logger.error(f"Error processing sound event {mysqlID}: {e}")
        finally:
            if 'conn' in locals():
                conn.close()

if __name__ == "__main__":
    controller = MazeController()
    controller.start()
