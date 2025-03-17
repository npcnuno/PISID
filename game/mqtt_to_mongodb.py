import paho.mqtt.client as mqtt
from pymongo import MongoClient
import hashlib
import logging
from datetime import datetime
import json
import re
from threading import Thread, Lock
import queue
import time
from pydantic import BaseModel, ValidationError, conint, confloat
from cachetools import TTLCache
from datetime import datetime
# Configuration
player_id = 33
MQTT_BROKER = "broker.mqtt-dashboard.com"
MQTT_PORT = 1883
TOPICS = [
    f"pisid_mazeNovo_{player_id}",
    f"pisid_mazemov_{player_id}",
    f"pisid_mazesound_{player_id}"
]
MONGO_URI = "mongodb://mongo:27017"
CACHE_TTL = 0.5
CACHE_MAX_SIZE = 100

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MongoDB setup
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["game_monitoring"]
game_sessions_col = db["game_sessions"]
raw_messages_col = db["raw_messages"]
move_messages_col = db["move_messages"]
sound_messages_col = db["sound_messages"]
failed_messages_col = db["failed_messages"]

# Ensure indexes
game_sessions_col.create_index([("player_id", 1), ("status", 1), ("start_time", -1)])
move_messages_col.create_index([("session_id", 1), ("timestamp", 1)])
sound_messages_col.create_index([("session_id", 1), ("timestamp", 1)])

# Queues for each topic
mazeNovo_queue = queue.Queue()
mazemov_queue = queue.Queue()
mazesound_queue = queue.Queue()

message_cache = {topic: TTLCache(maxsize=CACHE_MAX_SIZE, ttl=CACHE_TTL) for topic in TOPICS}
cache_lock = Lock()


def parse_key_value_pair(part):
    match = re.match(r'\s*(\w+)\s*:\s*(?:(["\'])(.*?)\2|([^,\s]+))\s*', part)
    if match:
        key = match.group(1)
        value = match.group(3) if match.group(2) else match.group(4)
        if value:
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass
        return key, value
    return None, None

def parse_payload(payload: str) -> dict:
    print(f"Raw payload: {payload}")
    try:
        payload = payload.strip()[1:-1]
        parts = payload.split(',')
        parsed_dict = {}
        for part in parts:
            key, value = parse_key_value_pair(part)
            if key:
                parsed_dict[key] = value
            else:
                raise ValueError("Invalid key-value pair")
    except Exception:
        raise ValueError("Invalid JSON payload")

    if 'Hour' in parsed_dict:
        hour_str = parsed_dict['Hour']
        if not isinstance(hour_str, str):
            raise ValueError("'Hour' must be a string")
        hour_str = hour_str.replace(',', '.')
        try:
            parsed_dict['Hour'] = datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S.%f").isoformat()
        except ValueError:
            try:
                parsed_dict['Hour'] = datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S").isoformat()
            except ValueError:
                raise ValueError("Invalid date format for 'Hour'. Expected: 'YYYY-MM-DD HH:MM:SS.SSSSSS'")

    return parsed_dict

class MazemovMessage(BaseModel):
    Player: conint(ge=player_id, le=player_id)
    Marsami: int
    RoomOrigin: int
    RoomDestiny: int
    Status: conint(ge=0, le=2)

class MazesoundMessage(BaseModel):
    Player: int
    Hour: datetime
    Sound: confloat(ge=0)


def handle_maze_novo(payload_dict: dict, message_hash: str):
    game_sessions_col.update_many(
        {"player_id": player_id, "status": "active"},
        {
            "$set": {
                "status": "completed",
                "end_time": datetime.now().isoformat()
            }
        }
    )
    session_doc = {
        "player_id": player_id,
        "start_time": payload_dict.get("StartTime", datetime.now().isoformat()),
        "status": "active",
        "created_at": datetime.now()
    }
    logger.info(session_doc) #FIXME: REMOVE LATER
    result = game_sessions_col.insert_one(session_doc)
    return result.inserted_id

def handle_movement_message(session_id, payload_dict: dict, message_hash: str):
    try:
        validated = MazemovMessage(**payload_dict)
    except ValidationError as e:
        logger.error(f"Invalid mazemov message: {e}")
        raise

    doc = {
        "session_id": session_id,
        "player": validated.Player,
        "marsami": validated.Marsami,
        "room_origin": validated.RoomOrigin,
        "room_destiny": validated.RoomDestiny,
        "status": validated.Status,
        "timestamp": datetime.now(),
        "message_hash": message_hash
    }
    move_messages_col.insert_one(doc)
    game_sessions_col.update_one(
        {"_id": session_id},
        {"$push": {"movement_messages": doc}}
    )

def handle_sound_message(session_id, payload_dict: dict, message_hash: str):
    try:
        validated = MazesoundMessage(**payload_dict)
    except ValidationError as e:
        logger.error(f"Invalid mazesound message: {e}")
        raise

    doc = {
        "session_id": session_id,
        "player": validated.Player,
        "sound_level": validated.Sound,
        "hour": validated.Hour,
        "timestamp": datetime.now(),
        "message_hash": message_hash
    }
    sound_messages_col.insert_one(doc)
    game_sessions_col.update_one(
        {"_id": session_id},
        {"$push": {"sound_messages": doc}}
    )

def get_current_session():
    session = game_sessions_col.find_one(
        {"player_id": player_id, "status": "active"},
        sort=[("start_time", -1)]
    )
    if not session:
        logger.warning("Creating new session")
        session_id = handle_maze_novo({}, "default_session")
        return session_id
    return session["_id"]

# Replace the single cache definition


# In worker_wrapper, update the duplicate check
def worker_wrapper(queue_name, handler):
    while True:
        msg = queue_name.get()
        try:
            payload = msg.payload.decode().strip()
            payload_dict = parse_payload(payload)
            message_hash = hashlib.sha256(
                json.dumps(payload_dict, sort_keys=True).encode()
            ).hexdigest()

            topic = msg.topic
            with cache_lock:
                topic_cache = message_cache[topic]
                if message_hash in topic_cache:
                    logger.info(f"Skipping duplicate in {topic}: {message_hash}")
                    failed_messages_col.insert_one({
                        "topic": topic,
                        "payload": payload_dict,
                        "error": "Duplicated Message",
                        "hash": message_hash,
                        "timestamp": datetime.now()
                    })
                    continue
                topic_cache[message_hash] = True

            if "mazeNovo" in msg.topic:
                handler(payload_dict, message_hash)
            else:
                session_id = get_current_session()
                handler(session_id, payload_dict, message_hash)

            logger.info(f"Processed {msg.topic}: {message_hash}")
        except Exception as e:
            logger.error(f"Error processing {msg.topic}: {e}")
            failed_messages_col.insert_one({
                "topic": msg.topic,
                "payload": payload,
                "error": str(e),
                "timestamp": datetime.now()
            })
        finally:
            queue_name.task_done()

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        raw_messages_col.insert_one({
            "topic": msg.topic,
            "payload": payload,
            "received_at": datetime.now()
        })

        if "mazeNovo" in msg.topic:
            mazeNovo_queue.put(msg)
        elif "mazemov" in msg.topic:
            mazemov_queue.put(msg)
        elif "mazesound" in msg.topic:
            mazesound_queue.put(msg)
    except Exception as e:
        logger.error(f"Message handling failed: {e}")

# Initialize worker threads
Thread(target=worker_wrapper, args=(mazeNovo_queue, handle_maze_novo), daemon=True).start()
Thread(target=worker_wrapper, args=(mazemov_queue, handle_movement_message), daemon=True).start()
Thread(target=worker_wrapper, args=(mazesound_queue, handle_sound_message), daemon=True).start()

def run():
    result = game_sessions_col.update_many(
        {"player_id": player_id, "status": "active"},
        {
            "$set": {
                "status": "completed",
                "end_time": datetime.now().isoformat()
            }
        }
    )
    logger.info(f"Closed {result.modified_count} active sessions on startup")
    client = mqtt.Client()
    client.on_message = on_message
    client.enable_logger(logger)
    client.reconnect_delay_set(min_delay=1, max_delay=120)

    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT)
            client.subscribe([(t, 1) for t in TOPICS])
            client.loop_forever()
        except Exception as e:
            logger.error(f"Connection failed: {e}. Retrying in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    run()
