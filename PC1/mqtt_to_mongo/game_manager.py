import os
import json
import subprocess
import threading
import queue
import time
from datetime import datetime
from paho.mqtt import client as mqtt_client
from pymongo import MongoClient, errors
from bson.objectid import ObjectId
import logging

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB Configuration
MONGO_USER = os.getenv('MONGO_USER', 'admin')
MONGO_PASS = os.getenv('MONGO_PASS', 'adminpass')
MONGO_DB = os.getenv('MONGO_DB', 'game_monitoring')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')
MONGO_URI = os.getenv('MONGO_URI', (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@mongo1:27017,mongo2:27017,mongo3:27017/"
    f"{MONGO_DB}?replicaSet=my-mongo-set&authSource={MONGO_AUTH_SOURCE}&"
    f"w=1&journal=true&retryWrites=true&"
    f"connectTimeoutMS=5000&socketTimeoutMS=5000&serverSelectionTimeoutMS=5000&"
    f"readPreference=primaryPreferred"
))

CHECK_INTERVAL = 5  # Seconds to check process status

# Process management
processes = {}  # {player_id: {"raw": {"proc": Process, "out_q": Queue, "err_q": Queue}, "proc": {...}, "send": {...}, "archive": {...}}}
lock = threading.Lock()

def connect_to_mongodb(retry_count=5, retry_delay=5):
    global mongo_client, db, game_sessions_col
    for attempt in range(retry_count):
        try:
            logger.info(f"Attempting to connect to MongoDB: {MONGO_URI}")
            mongo_client = MongoClient(
                MONGO_URI,
                maxPoolSize=20, minPoolSize=1,
                connectTimeoutMS=5000, socketTimeoutMS=5000,
                serverSelectionTimeoutMS=5000, retryWrites=True,
                authMechanism='SCRAM-SHA-256'
            )
            mongo_client.admin.command('ping')
            logger.info("Connected to MongoDB replica set")
            db = mongo_client[MONGO_DB]
            game_sessions_col = db["game_sessions"]
            return mongo_client
        except errors.PyMongoError as e:
            logger.error(f"Connection failed (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
    logger.error("Failed to connect to MongoDB")
    raise SystemExit(1)

def log_subprocess_output(proc, out_queue, err_queue, player_id, proc_name):
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
                logger.log(level, message)
                print(message)
                out_queue.task_done()
            except queue.Empty:
                break

        while True:
            try:
                level, message = err_queue.get_nowait()
                logger.log(level, message)
                print(message)
                err_queue.task_done()
            except queue.Empty:
                break

        if proc.poll() is not None:
            break
        time.sleep(0.1)

def start_scripts(player_id, mqtt_config):
    existing_session = game_sessions_col.find_one({"player_id": player_id, "status": "active"})
    if existing_session:
        logger.warning(f"Player {player_id} already has an active session, cannot start a new one")
        return
    session_doc = {
        "player_id": player_id,
        "status": "active",
        "start_time": datetime.now(),
        "mqtt_config": mqtt_config
    }
    result = game_sessions_col.insert_one(session_doc)
    session_id = str(result.inserted_id)
    start_processes(player_id, session_id, mqtt_config)

def start_processes(player_id, session_id, mqtt_config):
    with lock:
        if player_id in processes:
            logger.info(f"Processes already running for player {player_id}")
            return
        env = os.environ.copy()
        env["PLAYER_ID"] = str(player_id)
        env["SESSION_ID"] = session_id
        env["MQTT_BROKER"] = mqtt_config["broker_url"]
        env["MQTT_PORT"] = str(mqtt_config["port"])
        env["TOPICS_CONFIG"] = json.dumps(mqtt_config["topics"])
        env["MONGO_URI"] = MONGO_URI
        
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
        
        processes[player_id] = {
            "raw": {"proc": raw_proc, "out_q": raw_out_q, "err_q": raw_err_q},
            "proc": {"proc": proc_proc, "out_q": proc_out_q, "err_q": proc_err_q},
            "send": {"proc": send_proc, "out_q": send_out_q, "err_q": send_err_q},
            "archive": {"proc": archive_proc, "out_q": archive_out_q, "err_q": archive_err_q},
            "session_id": session_id
        }
        
        threading.Thread(target=log_subprocess_output, args=(raw_proc, raw_out_q, raw_err_q, player_id, "raw"), daemon=True).start()
        threading.Thread(target=log_subprocess_output, args=(proc_proc, proc_out_q, proc_err_q, player_id, "proc"), daemon=True).start()
        threading.Thread(target=log_subprocess_output, args=(send_proc, send_out_q, send_err_q, player_id, "send"), daemon=True).start()
        threading.Thread(target=log_subprocess_output, args=(archive_proc, archive_out_q, archive_err_q, player_id, "archive"), daemon=True).start()
        logger.info(f"Started scripts for player {player_id} with session {session_id}")

def stop_scripts(player_id):
    active_session = game_sessions_col.find_one({"player_id": player_id, "status": "active"})
    with lock:
        if player_id not in processes:
            logger.info(f"No running scripts for player {player_id}")
            return
        if not active_session:
            logger.info(f"No active session found for player {player_id}, cleaning up processes")
            if player_id in processes:
                procs = processes[player_id]
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
                            logger.warning(f"Forced kill of {proc_name} for player {player_id}")
                    logger.info(f"Stopped {proc_name} for player {player_id}")
                del processes[player_id]
            return
        session_id = processes[player_id]["session_id"]
        procs = processes[player_id]
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
                    logger.warning(f"Forced kill of {proc_name} for player {player_id}")
            logger.info(f"Stopped {proc_name} for player {player_id}")
        del processes[player_id]
    game_sessions_col.update_one(
        {"_id": ObjectId(session_id)},
        {"$set": {"status": "closed", "end_time": datetime.now()}}
    )
    logger.info(f"Closed session {session_id} for player {player_id} in MongoDB")

def monitor_processes():
    restart_counts = {}
    max_restarts = 10
    while True:
        with lock:
            for player_id, procs in list(processes.items()):
                for proc_name, proc_data in procs.items():
                    if proc_name == "session_id":
                        continue
                    proc = proc_data["proc"]
                    if proc.poll() is not None:
                        key = f"{player_id}_{proc_name}"
                        restart_counts[key] = restart_counts.get(key, 0) + 1
                        logger.error(f"{proc_name} for player {player_id} crashed with code {proc.poll()}")
                        if restart_counts[key] > max_restarts:
                            logger.error(f"Max restarts exceeded for {proc_name} for player {player_id}")
                            session_id = procs["session_id"]
                            del processes[player_id]
                            game_sessions_col.update_one(
                                {"_id": ObjectId(session_id)},
                                {"$set": {"status": "closed", "end_time": datetime.now()}}
                            )
                            continue
                        session = game_sessions_col.find_one({"player_id": player_id, "status": "active"})
                        if session:
                            mqtt_config = session.get("mqtt_config")
                            session_id = str(session["_id"])
                            env = os.environ.copy()
                            env["PLAYER_ID"] = str(player_id)
                            env["SESSION_ID"] = session_id
                            env["MQTT_BROKER"] = mqtt_config["broker_url"]
                            env["MQTT_PORT"] = str(mqtt_config["port"])
                            env["TOPICS_CONFIG"] = json.dumps(mqtt_config["topics"])
                            env["MONGO_URI"] = MONGO_URI
                            script_map = {"raw": "raw_messages.py", "proc": "message_processor.py", "send": "send_messages.py", "archive": "archive_messages.py"}
                            script_name = script_map[proc_name]
                            out_q = queue.Queue()
                            err_q = queue.Queue()
                            new_proc = subprocess.Popen(
                                ["python", script_name], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
                            )
                            procs[proc_name] = {"proc": new_proc, "out_q": out_q, "err_q": err_q}
                            threading.Thread(target=log_subprocess_output, args=(new_proc, out_q, err_q, player_id, proc_name), daemon=True).start()
                            logger.info(f"Restarted {proc_name} for player {player_id}")
        time.sleep(CHECK_INTERVAL)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        action = payload.get("action")
        player_id = payload.get("player_id")
        mqtt_config = payload.get("mqtt_config")
        if action == "start" and player_id and mqtt_config:
            start_scripts(player_id, mqtt_config)
        elif action == "stop" and player_id:
            stop_scripts(player_id)
        else:
            logger.warning(f"Invalid message: {payload}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def load_active_sessions():
    try:
        active_sessions = game_sessions_col.find({"status": "active"})
        for session in active_sessions:
            player_id = session["player_id"]
            session_id = str(session["_id"])
            mqtt_config = session["mqtt_config"]
            start_processes(player_id, session_id, mqtt_config)
            logger.info(f"Resumed active session for player {player_id}")
    except errors.PyMongoError as e:
        logger.error(f"Failed to load active sessions: {e}")
        raise

def connect_mqtt():
    client = mqtt_client.Client(client_id="game_manager")
    def on_connect(c, u, f, rc):
        logger.info(f"Connected with result code {rc}")
        c.subscribe("pisid_maze/mazeManager/#", qos=2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("test.mosquitto.org", 1883)
    return client

def main():
    connect_to_mongodb()
    load_active_sessions()
    threading.Thread(target=monitor_processes, daemon=True).start()
    client = connect_mqtt()
    client.loop_forever()

if __name__ == "__main__":
    main()
