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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# MongoDB Configuration
MONGO_USER = os.getenv('MONGO_USER', 'admin')
MONGO_PASS = os.getenv('MONGO_PASS', 'adminpass')
MONGO_DB = os.getenv('MONGO_DB', 'game_monitoring')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')

# Use the MONGO_URI from environment variables if available, otherwise build it
MONGO_URI = os.getenv('MONGO_URI', (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@mongo1:27017,mongo2:27017,mongo3:27017/"
    f"{MONGO_DB}?replicaSet=my-mongo-set&authSource={MONGO_AUTH_SOURCE}&"
    f"w=1&journal=true&retryWrites=true&"
    f"connectTimeoutMS=5000&socketTimeoutMS=5000&serverSelectionTimeoutMS=5000&"
    f"readPreference=primaryPreferred"
))

CHECK_INTERVAL = 5  # Seconds to check process status

# Process management
processes = {}  # {player_id: {"raw": {"proc": Process, "out_q": Queue, "err_q": Queue}, "proc": {...}, "send": {...}}}
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
            # Test the connection
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
    """Purpose: Reads and logs subprocess stdout and stderr in real-time for monitoring and debugging.
    Execution Flow:
    1. Define a nested function read_output to continuously read lines from a pipe (stdout or stderr).
    2. In read_output, iterate over pipe lines, queue each line with a logging level and prefix, and close the pipe when done.
    3. Create two daemon threads: one for stdout and one for stderr, passing the respective pipes, queues, and prefixes.
    4. Start both threads to begin reading output concurrently.
    5. Enter an infinite loop to process queued messages from out_queue and err_queue.
    6. For each queue, attempt to get a message within a 1-second timeout.
    7. If a message is retrieved, log it at the specified level (INFO) and print it to stdout for Docker logs.
    8. Mark the task as done in the queue.
    9. If the queue is empty (timeout), check if the process has terminated (proc.poll() is not None); if so, exit the loop."""
    def read_output(pipe, q, level, prefix):
        for line in iter(pipe.readline, ''):
            q.put((level, f"{prefix}: {line.strip()}"))
        pipe.close()
    out_thread = threading.Thread(target=read_output, args=(proc.stdout, out_queue, logging.INFO, f"{player_id}/{proc_name}/stdout"), daemon=True)
    err_thread = threading.Thread(target=read_output, args=(proc.stderr, err_queue, logging.INFO, f"{player_id}/{proc_name}/stderr"), daemon=True)
    out_thread.start()
    err_thread.start()
    while True:
        try:
            level, message = out_queue.get(timeout=1)
            logger.log(level, message)
            print(message)
            out_queue.task_done()
        except queue.Empty:
            pass
        try:
            level, message = err_queue.get(timeout=1)
            logger.log(level, message)
            print(message)
            err_queue.task_done()
        except queue.Empty:
            if proc.poll() is not None:
                break

def start_scripts(player_id, mqtt_config):
    """Purpose: Initiates a new game session for a player by starting subprocesses if no active session exists.
    Execution Flow:
    1. Query MongoDB to check for an existing active session for the given player_id.
    2. If an active session is found, log a warning and return without starting new scripts.
    3. Create a session document with player_id, 'active' status, current timestamp, and mqtt_config.
    4. Insert the session document into game_sessions_col and retrieve the generated session_id.
    5. Call start_processes with player_id, session_id, and mqtt_config to launch subprocesses."""
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
    """Purpose: Launches subprocesses for raw message handling, processing, and sending for a player session.
    Execution Flow:
    1. Acquire a lock to ensure thread-safe access to the processes dictionary.
    2. Check if processes are already running for the player_id; if so, log and return.
    3. Copy the current environment and set variables (PLAYER_ID, SESSION_ID, MQTT details, MONGO_URI).
    4. Create queues for stdout and stderr for each subprocess (raw, proc, send).
    5. Launch three subprocesses (raw_messages.py, message_processor.py, send_messages.py) with Popen, passing the environment.
    6. Store process details in the processes dictionary with player_id as the key.
    7. Start threads to log output for each subprocess using log_subprocess_output.
    8. Log the start of scripts for the player and session."""
    with lock:
        if player_id in processes:
            logger.info(f"Processes already running for player {player_id}")
            return
        env = os.environ.copy()
        env["PLAYER_ID"] = str(player_id)
        env["SESSION_ID"] = session_id
        env["MQTT_BROKER"] = mqtt_config["broker_url"]
        env["MQTT_PORT"] = str(mqtt_config["port"])
        env["TOPICS"] = json.dumps(mqtt_config["topics"])
        env["MONGO_URI"] = MONGO_URI
        
        raw_out_q = queue.Queue()
        raw_err_q = queue.Queue()
        proc_out_q = queue.Queue()
        proc_err_q = queue.Queue()
        send_out_q = queue.Queue()
        send_err_q = queue.Queue()
        
        raw_proc = subprocess.Popen(
            ["python", "raw_messages.py"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
        )
        proc_proc = subprocess.Popen(
            ["python", "message_processor.py"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
        )
        send_proc = subprocess.Popen(
            ["python", "send_messages.py"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
        )
        
        processes[player_id] = {
            "raw": {"proc": raw_proc, "out_q": raw_out_q, "err_q": raw_err_q},
            "proc": {"proc": proc_proc, "out_q": proc_out_q, "err_q": proc_err_q},
            "send": {"proc": send_proc, "out_q": send_out_q, "err_q": send_err_q},
            "session_id": session_id
        }
        
        threading.Thread(target=log_subprocess_output, args=(raw_proc, raw_out_q, raw_err_q, player_id, "raw"), daemon=True).start()
        threading.Thread(target=log_subprocess_output, args=(proc_proc, proc_out_q, proc_err_q, player_id, "proc"), daemon=True).start()
        threading.Thread(target=log_subprocess_output, args=(send_proc, send_out_q, send_err_q, player_id, "send"), daemon=True).start()
        logger.info(f"Started scripts for player {player_id} with session {session_id}")

def stop_scripts(player_id):
    """Purpose: Stops subprocesses for a player's active session and updates the session status in MongoDB.
    Execution Flow:
    1. Query MongoDB for an active session for the player_id.
    2. Acquire a lock to safely access the processes dictionary.
    3. If no processes are running for the player_id, log and return.
    4. If no active session exists but processes are running, terminate them and remove from processes.
    5. Otherwise, get the session_id and iterate through processes (raw, proc, send).
    6. For each process, terminate it if still running, wait up to 5 seconds, and kill if necessary.
    7. Log each process stop and remove the player_id entry from processes.
    8. Update the session in MongoDB to 'closed' with the current end_time.
    9. Log the session closure."""
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
    """Purpose: Monitors subprocesses and restarts crashed ones up to a maximum restart limit.
    Execution Flow:
    1. Initialize a restart_counts dictionary to track restarts per process.
    2. Enter an infinite loop to periodically check processes every CHECK_INTERVAL seconds.
    3. Acquire a lock to safely iterate over processes.
    4. For each player_id and process, check if it has terminated (poll() is not None).
    5. If terminated, increment restart count and log the crash with exit code.
    6. If restart count exceeds max_restarts (10), close the session and remove processes.
    7. Otherwise, retrieve session details, prepare environment, and restart the crashed process.
    8. Update processes dictionary and start a logging thread for the new process."""
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
                            env["TOPICS"] = json.dumps(mqtt_config["topics"])
                            env["MONGO_URI"] = MONGO_URI
                            script_map = {"raw": "raw_messages.py", "proc": "message_processor.py", "send": "send_messages.py"}
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
    """Purpose: Handles incoming MQTT messages to start or stop player sessions.
    Execution Flow:
    1. Decode the MQTT message payload and parse it as JSON.
    2. Extract 'action', 'player_id', and 'mqtt_config' from the payload.
    3. If action is 'start' and required fields are present, call start_scripts.
    4. If action is 'stop' and player_id is present, call stop_scripts.
    5. Otherwise, log a warning for an invalid message."""
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
    """Purpose: Loads and resumes active sessions from MongoDB on startup.
    Execution Flow:
    1. Query MongoDB for all sessions with 'active' status.
    2. For each active session, extract player_id, session_id, and mqtt_config.
    3. Call start_processes to resume subprocesses for the session.
    4. Log the resumption of each session.
    5. On MongoDB error, log the failure and raise an exception to halt startup."""
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
    """Purpose: Initializes and connects an MQTT client for game management commands.
    Execution Flow:
    1. Create an MQTT client with a fixed client_id 'game_manager'.
    2. Define on_connect callback to log connection status and subscribe to 'pisid_maze/mazeManager/#' with QoS 1.
    3. Set on_message callback to the on_message function.
    4. Connect to the MQTT broker at test.mosquitto.org:1883.
    5. Return the connected client."""
    client = mqtt_client.Client(client_id="game_manager")
    def on_connect(c, u, f, rc):
        logger.info(f"Connected with result code {rc}")
        c.subscribe("pisid_maze/mazeManager/#", qos=1)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("test.mosquitto.org", 1883)
    return client

def main():
    """Purpose: Orchestrates the startup and continuous operation of the game manager.
    Execution Flow:
    1. Call connect_to_mongodb to establish a database connection.
    2. Call load_active_sessions to resume any existing active sessions.
    3. Copy the environment and launch archive_messages.py as a subprocess for background archiving.
    4. Start a daemon thread to monitor subprocesses for crashes.
    5. Call connect_mqtt to set up the MQTT client.
    6. Enter an infinite loop with client.loop_forever() to process MQTT messages."""
    connect_to_mongodb()
    load_active_sessions()
    env = os.environ.copy()
    subprocess.Popen(["python", "archive_messages.py"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    threading.Thread(target=monitor_processes, daemon=True).start()
    client = connect_mqtt()
    client.loop_forever()

if __name__ == "__main__":
    main()
