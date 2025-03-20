from paho.mqtt import client as mqtt_client  # Import for type hints and clarity
import hashlib
import logging
from datetime import datetime
import json
import re
from threading import Thread, Lock
import queue
import time
import os
from pydantic import BaseModel, ValidationError, conint, confloat
from cachetools import TTLCache
from pymongo import MongoClient, errors, ReadPreference
import pytz
active_session_id = None

# --------------------------
# Configuration Settings
# --------------------------
player_id = int(os.getenv('PLAYER_ID', '33'))
MQTT_BROKER = os.getenv('MQTT_BROKER', 'broker.mqtt-dashboard.com')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
TOPICS = [
    f"pisid_mazeNovo_{player_id}",
    f"{os.getenv('MOVEMENT_TOPIC', f'pisid_mazemov_{player_id}')}",
    f"{os.getenv('SOUND_TOPIC', f'pisid_mazesound_{player_id}')}"
]

# MongoDB configuration optimized for streaming writes
MONGO_USER = os.getenv('MONGO_USER', 'admin')
MONGO_PASS = os.getenv('MONGO_PASS', 'adminpass')
MONGO_DB = os.getenv('MONGO_DB', 'game_monitoring')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')

# Optimized connection string for replica set
MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@"
    f"mongo1:27017,mongo2:27017,mongo3:27017/"
    f"{MONGO_DB}?replicaSet=my-mongo-set&"
    f"authSource={MONGO_AUTH_SOURCE}&"
    f"w=1&journal=false&"  # Write to primary only, no journaling for speed
    f"retryWrites=true&"   # Automatic retries for transient failures
    f"connectTimeoutMS=5000&socketTimeoutMS=5000&"  # Fast timeouts
    f"serverSelectionTimeoutMS=5000&"  # Quick server selection
    f"readPreference=primaryPreferred"  # Prefer primary for reads
)

CACHE_TTL = 0.3  # 300ms TTL for duplicate detection
CACHE_MAX_SIZE = 3  # Max cached messages per topic

# --------------------------
# Logging Configuration
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --------------------------
# Database Setup with Streamlined Connection
# --------------------------
def connect_to_mongodb(retry_count=5, retry_delay=5):
    """Connect to MongoDB with minimal overhead"""
    global mongo_client, db
    
    for attempt in range(retry_count):
        try:
            mongo_client = MongoClient(
                MONGO_URI,
                maxPoolSize=1,  # Single connection for streaming
                minPoolSize=1,  # Always keep one connection
                connectTimeoutMS=5000,
                socketTimeoutMS=5000,
                serverSelectionTimeoutMS=5000,
                retryWrites=True,    
                authMechanism='SCRAM-SHA-256',
            )
            # Quick ping to verify connection
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

# Connect to MongoDB
connect_to_mongodb()

# Collections
game_sessions_col = db["game_sessions"]
raw_messages_col = db["raw_messages"]
move_messages_col = db["move_messages"]
sound_messages_col = db["sound_messages"]
failed_messages_col = db["failed_messages"]

# Create minimal indexes for performance
try:
    move_messages_col.create_index([("timestamp", 1)], background=True)
    sound_messages_col.create_index([("timestamp", 1)], background=True)
    logger.info("Created minimal indexes")
except errors.PyMongoError as e:
    logger.warning(f"Index creation failed: {e}")

# --------------------------
# Message Queues
# --------------------------
mazemov_queue = queue.Queue()
mazesound_queue = queue.Queue()

# --------------------------
# Cache Setup
# --------------------------
message_cache = {topic: None for topic in TOPICS}  # Store only the last message hash
cache_lock = Lock()
session_lock = Lock()

# --------------------------
# Data Models
# --------------------------
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

# --------------------------
# Payload Parsing Functions
# --------------------------
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

# --------------------------
# Session Management
# --------------------------
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

# --------------------------
# Message Handlers
# --------------------------
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
        "Already_moved":"false",
    }
    move_messages_col.insert_one(doc)

def handle_sound_message(session_id, payload_dict: dict, message_hash: str):
    validated = MazesoundMessage(**payload_dict)
    tz = pytz.timezone('UTC')
    send_time = datetime.fromisoformat(payload_dict['Hour']).replace(tzinfo=tz)
    process_time = datetime.now(tz)
    latency = (process_time - send_time).total_seconds()*1000
    logger.critical(f"CURRENT LATENCY:  {latency}")
    doc = {
        "session_id": session_id,
        "player": validated.Player,
        "sound_level": validated.Sound,
        "hour": validated.Hour,
        "timestamp": datetime.now(),
        "message_hash": message_hash,
        "sent": "false",
        "Already_moved":"false",

    }
    sound_messages_col.insert_one(doc)

# --------------------------
# Worker Functions with Timing Logs
# --------------------------
def worker_mazemov():
    while True:
        msg = mazemov_queue.get()
        start_time = time.time()
        try:
            payload = msg.payload.decode().strip()
            decode_time = time.time()
            logger.info(f"Message decode time: {(decode_time - start_time)*1000:.2f}ms")
            raw_messages_col.insert_one({
                        "topic": msg.topic,
                        "payload": payload,
                        "timestamp": datetime.now()
                    })
            payload_dict = parse_payload(payload)
            parse_time = time.time()
            logger.info(f"Payload parse time: {(parse_time - decode_time)*1000:.2f}ms")

            message_hash = hashlib.sha256(json.dumps(payload_dict, sort_keys=True).encode()).hexdigest()
            hash_time = time.time()
            logger.info(f"Hash calculation time: {(hash_time - parse_time)*1000:.2f}ms")

            with cache_lock:
                previous_hash = message_cache[msg.topic]
                message_cache[msg.topic] = message_hash
                if previous_hash == message_hash:
                    cache_check_time = time.time()
                    logger.info(f"Consecutive duplicate detected time: {(cache_check_time - hash_time)*1000:.2f}ms")
                    failed_messages_col.insert_one({
                        "topic": msg.topic,
                        "payload": payload_dict,
                        "error": "Duplicated Message",
                        "hash": message_hash,
                        "timestamp": datetime.now()
                    })
                    continue
            cache_time = time.time()
            logger.info(f"Cache operation time: {(cache_time - hash_time)*1000:.2f}ms")

            session_id = get_current_session()
            session_time = time.time()
            logger.info(f"Session retrieval time: {(session_time - cache_time)*1000:.2f}ms")

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

def worker_mazesound():
    while True:
        msg = mazesound_queue.get()
        start_time = time.time()
        try:
            payload = msg.payload.decode().strip()
            raw_messages_col.insert_one({
                        "topic": msg.topic,
                        "payload": payload,
                        "timestamp": datetime.now()
                    })
            decode_time = time.time()
            logger.info(f"Message decode time: {(decode_time - start_time)*1000:.2f}ms")

            payload_dict = parse_payload(payload)
            parse_time = time.time()
            logger.info(f"Payload parse time: {(parse_time - decode_time)*1000:.2f}ms")

            message_hash = hashlib.sha256(json.dumps(payload_dict, sort_keys=True).encode()).hexdigest()
            hash_time = time.time()
            logger.info(f"Hash calculation time: {(hash_time - parse_time)*1000:.2f}ms")

            with cache_lock:
                previous_hash = message_cache[msg.topic]
                message_cache[msg.topic] = message_hash
                if previous_hash == message_hash:
                    cache_check_time = time.time()
                    logger.info(f"Consecutive duplicate detected time: {(cache_check_time - hash_time)*1000:.2f}ms")
                    continue
            cache_time = time.time()
            logger.info(f"Cache operation time: {(cache_time - hash_time)*1000:.2f}ms")

            session_id = get_current_session()
            session_time = time.time()
            logger.info(f"Session retrieval time: {(session_time - cache_time)*1000:.2f}ms")

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
# --------------------------
# MQTT Callbacks
# --------------------------
def on_connect(client: mqtt_client.Client, userdata, flags, reason_code, properties=None):
    """
    Callback for when the client connects to the broker.
    Updated for Paho MQTT 2.0+ API.
    """
    if reason_code == 0:
        logger.info("Connected to MQTT broker (code: 0 - Success)")
        for topic in TOPICS:
            client.subscribe(topic, qos=0)
            logger.info(f"Subscribed to topic: {topic}")
    else:
        logger.error(f"Failed to connect to MQTT broker: {reason_code}")

def on_message(client: mqtt_client.Client, userdata, msg: mqtt_client.MQTTMessage):
    """
    Callback for when a message is received from the broker.
    Updated for Paho MQTT 2.0+ API.
    """
    try:
        payload = msg.payload.decode()
        if "mazemov" in msg.topic:
            mazemov_queue.put(msg)
        elif "mazesound" in msg.topic:
            mazesound_queue.put(msg)
    except Exception as e:
        logger.error(f"Message routing error: {e}")
        failed_messages_col.insert_one({
            "topic": msg.topic,
            "payload": payload,
            "error": str(e),
            "timestamp": datetime.now()
        })

# --------------------------
# Main Execution (Updated for API 2.0+)
# --------------------------
def main():
    # Start worker threads (unchanged)
    for _ in range(2):
        Thread(target=worker_mazemov, daemon=True).start()
        Thread(target=worker_mazesound, daemon=True).start()
    
    # Create MQTT client with updated API
    client = mqtt_client.Client(
        client_id=f"player_{player_id}_monitor",
        protocol=mqtt_client.MQTTv5,  
        userdata=None
    )
    
    # Assign callbacks
    client.on_connect = on_connect
    client.on_message = on_message

    # Configure connection options (optional, for better control)
    client.reconnect_delay_set(min_delay=1, max_delay=5)  # Automatic reconnection

    while True:
        try:
            logger.info(f"Connecting to MQTT broker {MQTT_BROKER}:{MQTT_PORT}...")
            client.connect(
                MQTT_BROKER,
                MQTT_PORT,
            )
            client.loop_forever()
        except Exception as e:
            logger.error(f"MQTT connection error: {e}")
            time.sleep(1)  # Retry after delay

if __name__ == "__main__":
    main()
