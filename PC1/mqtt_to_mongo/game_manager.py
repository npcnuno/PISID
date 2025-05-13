import os
import json
import subprocess
import threading
import queue
import time
from datetime import datetime
from paho.mqtt import client
from pymongo import MongoClient, errors
from bson.objectid import ObjectId
import logging

class GameManager:
    def __init__(self):
        # Logging Configuration
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # MongoDB Configuration
        self.MONGO_USER = os.getenv('MONGO_USER', 'admin')
        self.MONGO_PASS = os.getenv('MONGO_PASS', 'adminpass')
        self.MONGO_DB = os.getenv('MONGO_DB', 'game_monitoring')
        self.MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')
        self.MONGO_URI = os.getenv('MONGO_URI', (
            f"mongodb://{self.MONGO_USER}:{self.MONGO_PASS}@mongo1:27017,mongo2:27017,mongo3:27017/"
            f"{self.MONGO_DB}?replicaSet=my-mongo-set&authSource={self.MONGO_AUTH_SOURCE}&"
            f"w=1&journal=true&retryWrites=true&"
            f"connectTimeoutMS=5000&socketTimeoutMS=5000&serverSelectionTimeoutMS=5000&"
            f"readPreference=primaryPreferred"
        ))
        self.MQTT_GAME_MANAGER_TOPIC = os.getenv("MQTT_TOPIC", "pisid_maze/mazeManager")
        self.MQTT_GAME_MANAGER_BROKER = os.getenv("MQTT_GAME_MANAGER_BROKER", "test.mosquitto.org")
        self.MQTT_GAME_MANAGER_TOPIC_ACK = os.getenv("MQTT_GAME_MANAGER_TOPIC_ACK", "pisid_maze/mazeManager_ack")
        self.MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
        self.CHECK_INTERVAL = 5  # Seconds to check process status
        self.MQTT_QOS = int(os.getenv("MQTT_QOS", "2"))

        # Process management
        self.processes = {}  # {player_id: {"raw": {"proc": Process, "out_q": Queue, "err_q": Queue}, "proc": {...}, "send": {...}, "archive": {...}}}
        self.lock = threading.Lock()

        # Initialize MongoDB and MQTT clients
        self.mongo_client = None
        self.db = None
        self.game_sessions_col = None
        self.mqtt_client = None

    def connect_to_mongodb(self, retry_count=5, retry_delay=5):
        for attempt in range(retry_count):
            try:
                self.logger.info(f"Attempting to connect to MongoDB: {self.MONGO_URI}")
                self.mongo_client = MongoClient(
                    self.MONGO_URI,
                    maxPoolSize=20, minPoolSize=1,
                    connectTimeoutMS=5000, socketTimeoutMS=5000,
                    serverSelectionTimeoutMS=5000, retryWrites=True,
                    authMechanism='SCRAM-SHA-256'
                )
                self.mongo_client.admin.command('ping')
                self.logger.info("Connected to MongoDB replica set")
                self.db = self.mongo_client[self.MONGO_DB]
                self.game_sessions_col = self.db["game_sessions"]
                return self.mongo_client
            except errors.PyMongoError as e:
                self.logger.error(f"Connection failed (attempt {attempt+1}/{retry_count}): {e}")
                if attempt < retry_count - 1:
                    time.sleep(retry_delay)
        self.logger.error("Failed to connect to MongoDB")
        raise SystemExit(1)

    def log_subprocess_output(self, proc, out_queue, err_queue, player_id, proc_name):
        def read_output(pipe, q, level, prefix):
            for line in iter(pipe.readline, ''):
                q.put((level, f"{prefix}: {line.strip()}"))
            pipe.close()

        out_thread = threading.Thread(
            target=read_output,
            args=(proc.stdout, out_queue, logging.INFO, f"{player_id}/{proc_name}/stdout"),
            daemon=True
        )
        err_thread = threading.Thread(
            target=read_output,
            args=(proc.stderr, err_queue, logging.INFO, f"{player_id}/{proc_name}/stderr"),
            daemon=True
        )
        out_thread.start()
        err_thread.start()

        while True:
            while True:
                try:
                    level, message = out_queue.get_nowait()
                    self.logger.log(level, message)
                    print(message)
                    out_queue.task_done()
                except queue.Empty:
                    break

            while True:
                try:
                    level, message = err_queue.get_nowait()
                    self.logger.log(level, message)
                    print(message)
                    err_queue.task_done()
                except queue.Empty:
                    break

            if proc.poll() is not None:
                break
            time.sleep(0.1)

    def start_scripts(self, player_id, mqtt_config):
        existing_session = self.game_sessions_col.find_one({"player_id": player_id, "status": "active"})
        if existing_session:
            self.logger.warning(f"Player {player_id} already has an active session, cannot start a new one")
            self.mqtt_client.publish(self.MQTT_GAME_MANAGER_TOPIC_ACK, f'{{"player_id": "{player_id}", "status": "in_use"}}')
            return
        session_doc = {
            "player_id": player_id,
            "status": "active",
            "start_time": datetime.now(),
            "mqtt_config": mqtt_config
        }
        result = self.game_sessions_col.insert_one(session_doc)
        session_id = str(result.inserted_id)
        self.start_processes(player_id, session_id, mqtt_config)

    def start_processes(self, player_id, session_id, mqtt_config):
        with self.lock:
            if player_id in self.processes:
                self.logger.info(f"Processes already running for player {player_id}")
                return
            env = os.environ.copy()
            env["PLAYER_ID"] = str(player_id)
            env["SESSION_ID"] = session_id
            env["MQTT_BROKER"] = mqtt_config["broker_url"]
            env["MQTT_PORT"] = str(mqtt_config["port"])
            env["TOPICS_CONFIG"] = json.dumps(mqtt_config["topics"])
            env["MONGO_URI"] = self.MONGO_URI

            raw_out_q = queue.Queue()
            raw_err_q = queue.Queue()
            proc_out_q = queue.Queue()
            proc_err_q = queue.Queue()
            send_out_q = queue.Queue()
            send_err_q = queue.Queue()
            archive_out_q = queue.Queue()
            archive_err_q = queue.Queue()

            raw_proc = subprocess.Popen(
                ["python", "raw_messages.py"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
            )
            proc_proc = subprocess.Popen(
                ["python", "message_processor.py"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
            )
            send_proc = subprocess.Popen(
                ["python", "send_messages.py"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
            )
            archive_proc = subprocess.Popen(
                ["python", "archive_messages.py"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
            )

            self.processes[player_id] = {
                "raw": {"proc": raw_proc, "out_q": raw_out_q, "err_q": raw_err_q},
                "proc": {"proc": proc_proc, "out_q": proc_out_q, "err_q": proc_err_q},
                "send": {"proc": send_proc, "out_q": send_out_q, "err_q": send_err_q},
                "archive": {"proc": archive_proc, "out_q": archive_out_q, "err_q": archive_err_q},
                "session_id": session_id
            }

            threading.Thread(target=self.log_subprocess_output, args=(raw_proc, raw_out_q, raw_err_q, player_id, "raw"), daemon=True).start()
            threading.Thread(target=self.log_subprocess_output, args=(proc_proc, proc_out_q, proc_err_q, player_id, "proc"), daemon=True).start()
            threading.Thread(target=self.log_subprocess_output, args=(send_proc, send_out_q, send_err_q, player_id, "send"), daemon=True).start()
            threading.Thread(target=self.log_subprocess_output, args=(archive_proc, archive_out_q, archive_err_q, player_id, "archive"), daemon=True).start()
            self.logger.info(f"Started scripts for player {player_id} with session {session_id}")
            self.mqtt_client.publish(self.MQTT_GAME_MANAGER_TOPIC_ACK, f'{{"player_id": "{player_id}", "status": "started"}}')

    def stop_scripts(self, player_id):
        active_session = self.game_sessions_col.find_one({"player_id": player_id, "status": "active"})
        with self.lock:
            if player_id not in self.processes:
                self.logger.info(f"No running scripts for player {player_id}")
                return
            if not active_session:
                self.logger.info(f"No active session found for player {player_id}, cleaning up processes")
                if player_id in self.processes:
                    procs = self.processes[player_id]
                    for proc_name, proc_data in procs.items():
                        if proc_name == "session_id":
                            continue
                        proc = proc_data["proc"]
                        if proc.poll() is None:
                            proc.terminate()
                            try:
                                proc.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                self.logger.warning(f"Forced kill of {proc_name} for player {player_id}")
                        self.logger.info(f"Stopped {proc_name} for player {player_id}")
                    del self.processes[player_id]
                return
            session_id = self.processes[player_id]["session_id"]
            procs = self.processes[player_id]
            for proc_name, proc_data in procs.items():
                if proc_name == "session_id":
                    continue
                proc = proc_data["proc"]
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        self.logger.warning(f"Forced kill of {proc_name} for player {player_id}")
                self.logger.info(f"Stopped {proc_name} for player {player_id}")
            del self.processes[player_id]
        self.game_sessions_col.update_one(
            {"_id": ObjectId(session_id)},
            {"$set": {"status": "closed", "end_time": datetime.now()}}
        )
        self.logger.info(f"Closed session {session_id} for player {player_id} in MongoDB")
        self.mqtt_client.publish(self.MQTT_GAME_MANAGER_TOPIC_ACK, f'{{"player_id": "{player_id}", "status": "closed"}}')

    def monitor_processes(self):
        restart_counts = {}
        max_restarts = 10
        while True:
            with self.lock:
                for player_id, procs in list(self.processes.items()):
                    for proc_name, proc_data in procs.items():
                        if proc_name == "session_id":
                            continue
                        proc = proc_data["proc"]
                        if proc.poll() is not None:
                            key = f"{player_id}_{proc_name}"
                            restart_counts[key] = restart_counts.get(key, 0) + 1
                            self.logger.error(f"{proc_name} for player {player_id} crashed with code {proc.poll()}")
                            if restart_counts[key] > max_restarts:
                                self.logger.error(f"Max restarts exceeded for {proc_name} for player {player_id}")
                                session_id = procs["session_id"]
                                del self.processes[player_id]
                                self.game_sessions_col.update_one(
                                    {"_id": ObjectId(session_id)},
                                    {"$set": {"status": "closed", "end_time": datetime.now()}}
                                )
                                continue
                            session = self.game_sessions_col.find_one({"player_id": player_id, "status": "active"})
                            if session:
                                mqtt_config = session.get("mqtt_config")
                                session_id = str(session["_id"])
                                env = os.environ.copy()
                                env["PLAYER_ID"] = str(player_id)
                                env["SESSION_ID"] = session_id
                                env["MQTT_BROKER"] = mqtt_config["broker_url"]
                                env["MQTT_PORT"] = str(mqtt_config["port"])
                                env["TOPICS_CONFIG"] = json.dumps(mqtt_config["topics"])
                                env["MONGO_URI"] = self.MONGO_URI
                                script_map = {"raw": "raw_messages.py", "proc": "message_processor.py", "send": "send_messages.py", "archive": "archive_messages.py"}
                                script_name = script_map[proc_name]
                                out_q = queue.Queue()
                                err_q = queue.Queue()
                                new_proc = subprocess.Popen(
                                    ["python", script_name], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
                                )
                                procs[proc_name] = {"proc": new_proc, "out_q": out_q, "err_q": err_q}
                                threading.Thread(target=self.log_subprocess_output, args=(new_proc, out_q, err_q, player_id, proc_name), daemon=True).start()
                                self.logger.info(f"Restarted {proc_name} for player {player_id}")
            time.sleep(self.CHECK_INTERVAL)

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            action = payload.get("action")
            player_id = payload.get("player_id")
            mqtt_config = payload.get("mqtt_config")
            if action == "start" and player_id and mqtt_config:
                self.start_scripts(player_id, mqtt_config)
            elif action == "stop" and player_id:
                self.stop_scripts(player_id)
            else:
                self.logger.warning(f"Invalid message: {payload}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def load_active_sessions(self):
        try:
            active_sessions = self.game_sessions_col.find({"status": "active"})
            for session in active_sessions:
                player_id = session["player_id"]
                session_id = str(session["_id"])
                mqtt_config = session["mqtt_config"]
                self.start_processes(player_id, session_id, mqtt_config)
                self.logger.info(f"Resumed active session for player {player_id}")
        except errors.PyMongoError as e:
            self.logger.error(f"Failed to load active sessions: {e}")
            raise

    def connect_mqtt(self, topic, broker,port, qos):
        mqtt_client = client.Client()
        def on_connect(c, u, f, rc):
            self.logger.info(f"Connected with result code {rc}")
            c.subscribe(topic, qos=qos)
        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = self.on_message
        mqtt_client.connect(broker, port)
        return mqtt_client

    

    def run(self):
        self.connect_to_mongodb()
        self.mqtt_client = self.connect_mqtt(self.MQTT_GAME_MANAGER_TOPIC,self.MQTT_GAME_MANAGER_BROKER, self.MQTT_PORT, self.MQTT_QOS)
        self.load_active_sessions()
        threading.Thread(target=self.monitor_processes, daemon=True).start()
        self.mqtt_client.loop_forever()

if __name__ == "__main__":
    game_manager = GameManager()
    game_manager.run()
