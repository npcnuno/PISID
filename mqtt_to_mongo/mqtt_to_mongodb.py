from paho.mqtt import client as mqtt_client
import hashlib
import logging
from datetime import datetime
import json
import re
from threading import Thread, Lock, active_count
import queue
import time
import os
from pydantic import BaseModel, ValidationError, conint, confloat
from collections import defaultdict
import pytz
import threading

try:
    from blist import blist
    FAST_DICT = lambda: defaultdict(lambda: None, blist())
except ImportError:
    FAST_DICT = lambda: defaultdict(lambda: None)

active_session_id = None

# Configuration Settings
player_id = int(os.getenv('PLAYER_ID', '33'))
MQTT_BROKER = os.getenv('MQTT_BROKER', 'broker.mqtt-dashboard.com')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
TOPICS = [
    f"pisid_mazeNovo_{player_id}",
    f"{os.getenv('MOVEMENT_TOPIC', f'pisid_mazemov_{player_id}')}",
    f"{os.getenv('SOUND_TOPIC', f'pisid_mazesound_{player_id}')}"
]

MONGO_USER = os.getenv('MONGO_USER', 'admin')
MONGO_PASS = os.getenv('MONGO_PASS', 'adminpass')
MONGO_DB = os.getenv('MONGO_DB', 'game_monitoring')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')
MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@"
    f"mongo1:27017,mongo2:27017,mongo3:27017/"
    f"{MONGO_DB}?replicaSet=my-mongo-set&"
    f"authSource={MONGO_AUTH_SOURCE}&w=1&journal=false&"
    f"retryWrites=true&connectTimeoutMS=5000&socketTimeoutMS=5000&"
    f"serverSelectionTimeoutMS=5000&readPreference=primaryPreferred"
)

BASE_TTL = 10
CACHE_MAX_SIZE = 1000
MIN_THREADS = 2
MAX_THREADS = 10
SCALE_UP_THRESHOLD = 50
SCALE_DOWN_THRESHOLD = 5
CHECK_INTERVAL = 5
THRESHOLD_DELAY = 2

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database Setup
from pymongo import MongoClient, errors
mongo_client = None

def connect_to_mongodb(retry_count=5, retry_delay=5):
    global mongo_client, db
    for attempt in range(retry_count):
        try:
            mongo_client = MongoClient(
                MONGO_URI,
                maxPoolSize=1, minPoolSize=1,
                connectTimeoutMS=5000, socketTimeoutMS=5000,
                serverSelectionTimeoutMS=5000, retryWrites=True,
                authMechanism='SCRAM-SHA-256'
            )
            mongo_client.admin.command('ping')
            logger.info("Connected to MongoDB replica set")
            db = mongo_client[MONGO_DB]
            return True
        except errors.PyMongoError as e:
            logger.error(f"Connection failed (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
    logger.error("Failed to connect to MongoDB")
    raise SystemExit(1)

connect_to_mongodb()
game_sessions_col = db["game_sessions"]
raw_messages_col = db["raw_messages"]
move_messages_col = db["move_messages"]
sound_messages_col = db["sound_messages"]
failed_messages_col = db["failed_messages"]

try:
    move_messages_col.create_index([("timestamp", 1)], background=True)
    sound_messages_col.create_index([("timestamp", 1)], background=True)
    logger.info("Created minimal indexes")
except errors.PyMongoError as e:
    logger.warning(f"Index creation failed: {e}")

# Message Queues and Thread Management
mazemov_queue = queue.Queue()
mazesound_queue = queue.Queue()
mazemov_threads = []
mazesound_threads = []
thread_lock = Lock()

class FastTTLCache:
    def __init__(self, maxsize, base_ttl):
        self.maxsize = maxsize
        self.base_ttl = base_ttl
        self.cache = FAST_DICT()  # {mid: message_hash}
        self.timestamps = FAST_DICT()  # {mid: timestamp}
        self.worst_latency = 0
        self.lock = threading.Lock()

    def update_ttl(self, latency):
        with self.lock:
            self.worst_latency = max(self.worst_latency, latency)
            self._cleanup()

    def get_ttl(self):
        return self.base_ttl + (self.worst_latency / 1000)

    def _cleanup(self):
        current_time = time.time()
        ttl = self.get_ttl()
        expired = [key for key, ts in self.timestamps.items() if current_time - ts > ttl]
        for key in expired:
            self.cache.pop(key, None)
            self.timestamps.pop(key, None)

    def is_duplicate(self, mid, message_hash):
        with self.lock:
            self._cleanup()
            if mid in self.cache:
                stored_hash = self.cache[mid]
                return stored_hash == message_hash  # True if hashes match (duplicate)
            return False

    def add(self, mid, message_hash):
        with self.lock:
            self._cleanup()
            if len(self.cache) >= self.maxsize:
                oldest_key = min(self.timestamps.items(), key=lambda x: x[1])[0]
                self.cache.pop(oldest_key, None)
                self.timestamps.pop(oldest_key, None)
            self.cache[mid] = message_hash
            self.timestamps[mid] = time.time()

message_cache = FastTTLCache(maxsize=CACHE_MAX_SIZE, base_ttl=BASE_TTL)
cache_lock = Lock()
session_lock = Lock()

# Data Models
class MazemovMessage(BaseModel):
    Player: conint(ge=1)
    Marsami: int
    RoomOrigin: int
    RoomDestiny: int
    Status: conint(ge=0, le=2)

class MazesoundMessage(BaseModel):
    Player: int
    Hour: datetime
    Sound: confloat(ge=0)

# Payload Parsing Functions
def parse_key_value_pair(part: str) -> tuple:
    match = re.match(r'\s*(\w+)\s*:\s*(?:(["\'])(.*?)\2|([^,\s]+))\s*', part)
    if match:
        key = match.group(1)
        value = match.group(3) if match.group(2) else match.group(4)
        try:
            return key, int(value)
        except ValueError:
            try:
                return key, float(value)
            except ValueError:
                return key, value
    return None, None

def parse_payload(payload: str) -> dict:
    payload = payload.strip()[1:-1]
    parsed_dict = {}
    for part in payload.split(','):
        key, value = parse_key_value_pair(part)
        if key:
            parsed_dict[key] = value
    if 'Hour' in parsed_dict:
        hour_str = str(parsed_dict['Hour']).replace(',', '.')
        try:
            parsed_dict['Hour'] = datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S.%f").isoformat()
        except ValueError:
            parsed_dict['Hour'] = datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S").isoformat()
    return parsed_dict

# Session Management
def get_current_session():
    global active_session_id
    with session_lock:
        if active_session_id:
            return active_session_id
        session = game_sessions_col.find_one(
            {"player_id": player_id, "status": "active"},
            sort=[("start_time", -1)]
        )
        if session:
            active_session_id = session["_id"]
            return active_session_id
        session_doc = {
            "player_id": player_id,
            "status": "active",
            "created_at": datetime.now()
        }
        result = game_sessions_col.insert_one(session_doc)
        active_session_id = result.inserted_id
        return active_session_id

# Message Handlers
def handle_maze_novo():
    global active_session_id
    with session_lock:
        game_sessions_col.update_many(
            {"player_id": player_id, "status": "active"},
            {"$set": {"status": "completed", "end_time": datetime.now().isoformat()}}
        )
        session_doc = {
            "player_id": player_id,
            "status": "active",
            "created_at": datetime.now(),
            "movement_messages": [],
            "sound_messages": []
        }
        result = game_sessions_col.insert_one(session_doc)
        active_session_id = result.inserted_id
        logger.info(f"New session: {active_session_id}")

def handle_movement_message(session_id, payload_dict: dict, message_hash: str):
    validated = MazemovMessage(**payload_dict)
    doc = {
        "session_id": session_id,
        "player": validated.Player,
        "marsami": validated.Marsami,
        "room_origin": validated.RoomOrigin,
        "room_destiny": validated.RoomDestiny,
        "status": validated.Status,
        "timestamp": datetime.now(),
        "message_hash": message_hash,
        "sent": "false",
        "Already_moved": "false",
    }
    move_messages_col.insert_one(doc)

def handle_sound_message(session_id, payload_dict: dict, message_hash: str):
    validated = MazesoundMessage(**payload_dict)
    tz = pytz.timezone('UTC')
    send_time = datetime.fromisoformat(payload_dict['Hour']).replace(tzinfo=tz)
    process_time = datetime.now(tz)
    latency = (process_time - send_time).total_seconds() * 1000
    logger.critical(f"CURRENT LATENCY: {latency}ms")
    doc = {
        "session_id": session_id,
        "player": validated.Player,
        "sound_level": validated.Sound,
        "hour": validated.Hour,
        "timestamp": datetime.now(),
        "message_hash": message_hash,
        "sent": "false",
        "Already_moved": "false",
    }
    sound_messages_col.insert_one(doc)

# Worker Functions with Scaling
def worker_mazemov():
    while True:
        try:
            msg = mazemov_queue.get(timeout=1)
            start_time = time.time()
            try:
                payload = msg.payload.decode().strip()
                decode_time = time.time()
                logger.info(f"Message decode time: {(decode_time - start_time)*1000:.2f}ms")

                raw_messages_col.insert_one({
                    "topic": msg.topic,
                    "payload": payload,
                    "message_id": msg.mid,
                    "dup_flag": msg.dup,
                    "timestamp": datetime.now()
                })

                message_hash = hashlib.sha256(payload.encode()).hexdigest()
                msg_id = msg.mid

                with cache_lock:
                    is_duplicate = message_cache.is_duplicate(msg_id, message_hash)
                    if is_duplicate:
                        cache_check_time = time.time()
                        logger.info(f"Duplicate detected (MID: {msg_id}, DUP: {msg.dup}) time: {(cache_check_time - decode_time)*1000:.2f}ms")
                        failed_messages_col.insert_one({
                            "topic": msg.topic,
                            "payload": payload,
                            "error": f"Duplicate Message (DUP: {msg.dup})",
                            "hash": message_hash,
                            "message_id": msg_id,
                            "timestamp": datetime.now()
                        })
                        continue
                    # Add to cache if not a duplicate
                    message_cache.add(msg_id, message_hash)

                cache_time = time.time()
                logger.info(f"Cache operation time: {(cache_time - decode_time)*1000:.2f}ms")

                payload_dict = parse_payload(payload)
                parse_time = time.time()
                logger.info(f"Payload parse time: {(parse_time - cache_time)*1000:.2f}ms")

                session_id = get_current_session()
                session_time = time.time()
                logger.info(f"Session retrieval time: {(session_time - parse_time)*1000:.2f}ms")

                handle_movement_message(session_id, payload_dict, message_hash)
                process_time = time.time()
                logger.info(f"Message processing time: {(process_time - session_time)*1000:.2f}ms")

                total_time = (process_time - start_time) * 1000
                logger.info(f"Total mazemov message handling time: {total_time:.2f}ms")

            except Exception as e:
                error_time = time.time()
                logger.error(f"mazemov error after {(error_time - start_time)*1000:.2f}ms: {e}")
                failed_messages_col.insert_one({
                    "topic": msg.topic,
                    "payload": payload,
                    "error": str(e),
                    "timestamp": datetime.now()
                })
            finally:
                mazemov_queue.task_done()
        except queue.Empty:
            with thread_lock:
                if len(mazemov_threads) > MIN_THREADS and mazemov_queue.qsize() < SCALE_DOWN_THRESHOLD:
                    break

def worker_mazesound():
    while True:
        try:
            msg = mazesound_queue.get(timeout=1)
            start_time = time.time()
            try:
                payload = msg.payload.decode().strip()
                raw_messages_col.insert_one({
                    "topic": msg.topic,
                    "payload": payload,
                    "message_id": msg.mid,
                    "dup_flag": msg.dup,
                    "timestamp": datetime.now()
                })
                decode_time = time.time()
                logger.info(f"Message decode time: {(decode_time - start_time)*1000:.2f}ms")

                message_hash = hashlib.sha256(payload.encode()).hexdigest()
                msg_id = msg.mid

                with cache_lock:
                    is_duplicate = message_cache.is_duplicate(msg_id, message_hash)
                    if is_duplicate:
                        cache_check_time = time.time()
                        logger.info(f"Duplicate detected (MID: {msg_id}, DUP: {msg.dup}) time: {(cache_check_time - decode_time)*1000:.2f}ms")
                        failed_messages_col.insert_one({
                            "topic": msg.topic,
                            "payload": payload,
                            "error": f"Duplicate Message (DUP: {msg.dup})",
                            "hash": message_hash,
                            "message_id": msg_id,
                            "timestamp": datetime.now()
                        })
                        continue
                    # Add to cache if not a duplicate
                    message_cache.add(msg_id, message_hash)

                cache_time = time.time()
                logger.info(f"Cache operation time: {(cache_time - decode_time)*1000:.2f}ms")

                payload_dict = parse_payload(payload)
                parse_time = time.time()
                logger.info(f"Payload parse time: {(parse_time - cache_time)*1000:.2f}ms")

                tz = pytz.timezone('UTC')
                send_time = datetime.fromisoformat(payload_dict['Hour']).replace(tzinfo=tz)
                process_time = datetime.now(tz)
                latency = (process_time - send_time).total_seconds() * 1000

                with cache_lock:
                    message_cache.update_ttl(latency)
                    current_ttl = message_cache.get_ttl()
                    logger.debug(f"Updated TTL to {current_ttl:.2f}s based on latency {latency:.2f}ms")

                session_id = get_current_session()
                session_time = time.time()
                logger.info(f"Session retrieval time: {(session_time - parse_time)*1000:.2f}ms")

                handle_sound_message(session_id, payload_dict, message_hash)
                process_time = time.time()
                logger.info(f"Message processing time: {(process_time - session_time)*1000:.2f}ms")

                total_time = (process_time - start_time) * 1000
                logger.info(f"Total mazesound message handling time: {total_time:.2f}ms")

            except Exception as e:
                error_time = time.time()
                logger.error(f"mazesound error after {(error_time - start_time)*1000:.2f}ms: {e}")
                failed_messages_col.insert_one({
                    "topic": msg.topic,
                    "payload": payload,
                    "error": str(e),
                    "timestamp": datetime.now()
                })
            finally:
                mazesound_queue.task_done()
        except queue.Empty:
            with thread_lock:
                if len(mazesound_threads) > MIN_THREADS and mazesound_queue.qsize() < SCALE_DOWN_THRESHOLD:
                    break

# Scaling Function with Delay
def scale_workers(queue_obj, thread_list, worker_func, name):
    last_check_time = time.time()
    scale_up_start = None
    scale_down_start = None
    
    while True:
        try:
            queue_size = queue_obj.qsize()
            current_time = time.time()
            
            with thread_lock:
                current_threads = len(thread_list)
                
                if queue_size > SCALE_UP_THRESHOLD and current_threads < MAX_THREADS:
                    if scale_up_start is None:
                        scale_up_start = current_time
                    elif (current_time - scale_up_start) >= THRESHOLD_DELAY:
                        new_threads = min(MAX_THREADS - current_threads, (queue_size // SCALE_UP_THRESHOLD))
                        for _ in range(new_threads):
                            t = Thread(target=worker_func, daemon=True)
                            t.start()
                            thread_list.append(t)
                        logger.info(f"Scaled up {name} workers: {current_threads} -> {current_threads + new_threads} after {THRESHOLD_DELAY}s threshold")
                        scale_up_start = None
                else:
                    scale_up_start = None
                
                if queue_size < SCALE_DOWN_THRESHOLD and current_threads > MIN_THREADS:
                    if scale_down_start is None:
                        scale_down_start = current_time
                    elif (current_time - scale_down_start) >= THRESHOLD_DELAY:
                        excess_threads = min(current_threads - MIN_THREADS, (SCALE_DOWN_THRESHOLD - queue_size) // 2)
                        for _ in range(excess_threads):
                            if thread_list:
                                thread_list.pop()
                        logger.info(f"Scaled down {name} workers: {current_threads} -> {current_threads - excess_threads} after {THRESHOLD_DELAY}s threshold")
                        scale_down_start = None
                else:
                    scale_down_start = None

            time.sleep(max(0, CHECK_INTERVAL - (current_time - last_check_time)))
            last_check_time = time.time()
        except Exception as e:
            logger.error(f"Error in {name} scaler: {e}")
            time.sleep(CHECK_INTERVAL)

# MQTT Callbacks
def on_connect(client: mqtt_client.Client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        logger.info("Connected to MQTT broker (code: 0 - Success)")
        for topic in TOPICS:
            client.subscribe(topic, qos=1)
            logger.info(f"Subscribed to topic: {topic}")
    else:
        logger.error(f"Failed to connect to MQTT broker: {reason_code}")

def on_message(client: mqtt_client.Client, userdata, msg: mqtt_client.MQTTMessage):
    try:
        logger.debug(f"Received message on {msg.topic}, DUP: {msg.dup}, MID: {msg.mid}")
        if "mazeNovo" in msg.topic:
            handle_maze_novo()
        elif "mazemov" in msg.topic:
            mazemov_queue.put(msg)
        elif "mazesound" in msg.topic:
            mazesound_queue.put(msg)
    except Exception as e:
        logger.error(f"Message routing error: {e}")
        failed_messages_col.insert_one({
            "topic": msg.topic,
            "payload": msg.payload.decode(),
            "error": str(e),
            "timestamp": datetime.now()
        })

# Main Execution
def main():
    with thread_lock:
        for _ in range(MIN_THREADS):
            t1 = Thread(target=worker_mazemov, daemon=True)
            t1.start()
            mazemov_threads.append(t1)
            t2 = Thread(target=worker_mazesound, daemon=True)
            t2.start()
            mazesound_threads.append(t2)
    
    Thread(target=scale_workers, args=(mazemov_queue, mazemov_threads, worker_mazemov, "mazemov"), daemon=True).start()
    Thread(target=scale_workers, args=(mazesound_queue, mazesound_threads, worker_mazesound, "mazesound"), daemon=True).start()
    
    client = mqtt_client.Client(
        client_id=f"player_{player_id}_monitor",
        protocol=mqtt_client.MQTTv5,
        userdata=None
    )
    
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=5)

    while True:
        try:
            logger.info(f"Connecting to MQTT broker {MQTT_BROKER}:{MQTT_PORT}...")
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            logger.error(f"MQTT connection error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()
