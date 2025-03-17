import paho.mqtt.client as mqtt
from pymongo import MongoClient
import hashlib
import logging
from datetime import datetime
import json
import re
from threading import Lock
import time

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
CACHE_TTL = 300  # 5 minutes in seconds

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
failed_messages_col = db["failed_messages"]

# Collections for move and sound messages
move_messages_col = db["move_messages"]
sound_messages_col = db["sound_messages"]

# In-memory cache with TTL
class MessageCache:
    def __init__(self):
        self.cache = {}
        self.lock = Lock()
        self.timestamps = []
        
    def add(self, hash_value):
        with self.lock:
            self.cache[hash_value] = datetime.now()
            self.timestamps = [(h, t) for h, t in self.timestamps if (datetime.now() - t).seconds < CACHE_TTL]
            self.timestamps.append((hash_value, datetime.now()))
            
    def exists(self, hash_value):
        with self.lock:
            return hash_value in self.cache

message_cache = MessageCache()

def hash_message(payload):
    return hashlib.sha256(payload.encode()).hexdigest()

def parse_payload(payload):
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        fixed = re.sub(r'(\w+)\s*:\s*', r'"\1": ', payload)
        fixed = re.sub(r':\s*([^{}\[\],\s]+)(?=\s*[,}])', r': "\1"', fixed)
        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            result = {}
            pairs = re.findall(r'"(\w+)"\s*:\s*"?([^",}]+)"?', fixed)
            for key, value in pairs:
                value = value.strip()
                if value.isdigit():
                    result[key] = int(value)
                elif value.replace('.', '', 1).isdigit():
                    result[key] = float(value)
                else:
                    result[key] = value
            return result

def handle_maze_novo(payload_dict, message_hash):
    session_doc = {
        "player_id": player_id,
        "start_time": payload_dict.get("start_time", datetime.now().isoformat()),
        "status": "active",
        "maze_data": payload_dict,
        "created_at": datetime.now()
    }
    result = game_sessions_col.insert_one(session_doc)
    return result.inserted_id

def handle_movement_message(session_id, payload_dict, message_hash):
    doc = {
        "session_id": session_id,
        "player": int(payload_dict.get("player", player_id)),
        "marsami": int(payload_dict.get("marsami", 0)),
        "room_origin": int(payload_dict.get("roomorigin", 0)),
        "room_destiny": int(payload_dict.get("roomdestiny", 0)),
        "status": int(payload_dict.get("status", 0)),
        "timestamp": datetime.now(),
        "message_hash": message_hash
    }
    game_sessions_col.update_one(
        {"_id": session_id},
        {"$push": {"movement_messages": doc}}
    )
    move_messages_col.insert_one(doc)

def handle_sound_message(session_id, payload_dict, message_hash):
    doc = {
        "session_id": session_id,
        "player": int(payload_dict.get("player", player_id)),
        "sound_level": float(payload_dict.get("sound", 0)),
        "hour": payload_dict.get("hour", datetime.now().isoformat()),
        "timestamp": datetime.now(),
        "message_hash": message_hash
    }
    game_sessions_col.update_one(
        {"_id": session_id},
        {"$push": {"sound_messages": doc}}
    )
    sound_messages_col.insert_one(doc)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8").strip()
        message_hash = hash_message(payload)
        logger.info(f"Received message on {msg.topic}: {payload}")
        
        raw_messages_col.insert_one({
            "raw_payload": payload,
            "topic": msg.topic,
            "received_at": datetime.now(),
            "hash": message_hash
        })
        
        if message_cache.exists(message_hash):
            logger.info(f"Skipping duplicate message: {message_hash}")
            return
            
        payload_dict = parse_payload(payload)
        logger.debug(f"Parsed payload: {payload_dict}")
        
        current_session = game_sessions_col.find_one(
            {"player_id": player_id, "status": "active"},
            sort=[("start_time", -1)]
        )
        
        if "mazeNovo" in msg.topic:
            session_id = handle_maze_novo(payload_dict, message_hash)
            message_cache.add(message_hash)
            return
            
        if not current_session:
            logger.warning("No active session, creating default session")
            session_id = handle_maze_novo({}, message_hash)
        else:
            session_id = current_session["_id"]
            
        if "mazemov" in msg.topic:
            handle_movement_message(session_id, payload_dict, message_hash)
        elif "mazesound" in msg.topic:
            handle_sound_message(session_id, payload_dict, message_hash)
            
        message_cache.add(message_hash)
        
    except Exception as e:
        logger.error(f"Message processing failed: {str(e)}")
        raw_messages_col.insert_one({
            "error": str(e),
            "raw_payload": payload,
            "timestamp": datetime.now()
        })

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT Broker!")
        # Subscribe to all topics upon connection
        client.subscribe([(topic, 0) for topic in TOPICS])
    else:
        logger.error(f"Failed to connect, return code {rc}")

def on_subscribe(client, userdata, mid, granted_qos):
    for i, qos in enumerate(granted_qos):
        if qos == 128:
            logger.error(f"Subscription to {TOPICS[i]} failed")
        else:
            logger.info(f"Subscribed to {TOPICS[i]} with QoS {qos}")

def run():
    client = mqtt.Client()
    client.on_message = on_message
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.loop_forever()
    except Exception as e:
        logger.error(f"MQTT connection error: {e}")
        time.sleep(1)
        run()  # Reconnect on failure

if __name__ == "__main__":
    run()
