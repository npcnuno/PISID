import paho.mqtt.client as mqtt
from pymongo import MongoClient
import hashlib
import logging
from datetime import datetime
import time
import json
import re
from threading import Lock

# Configuration
PLAYER_ID = 33
MQTT_BROKER = "broker.mqtt-dashboard.com"
MQTT_PORT = 1883
TOPICS = [
    f"pisid_mazeNovo_{PLAYER_ID}",
    f"pisid_mazemov_{PLAYER_ID}",
    f"pisid_mazesound_{PLAYER_ID}"
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

# In-memory cache with TTL
class MessageCache:
    def __init__(self):
        self.cache = {}
        self.lock = Lock()
        self.timestamps = []
        
    def add(self, hash_value):
        with self.lock:
            self.cache[hash_value] = datetime.now()
            # Remove expired entries
            self.timestamps = [(h, t) for h, t in self.timestamps if (datetime.now() - t).seconds < CACHE_TTL]
            self.timestamps.append((hash_value, datetime.now()))
            
    def exists(self, hash_value):
        with self.lock:
            return hash_value in self.cache

message_cache = MessageCache()

def hash_message(payload):
    return hashlib.sha256(payload.encode()).hexdigest()

def parse_payload(payload):
    """Robust payload parser that handles malformed JSON"""
    try:
        # First attempt with standard JSON parsing
        return json.loads(payload)
    except json.JSONDecodeError:
        # Attempt to fix common formatting issues
        fixed = re.sub(r'(\w+)\s*:\s*', r'"\1": ', payload)
        fixed = re.sub(r':\s*([^{}\[\],\s]+)(?=\s*[,}])', r': "\1"', fixed)
        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            # Fallback to manual parsing
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
    """Handle maze initialization messages"""
    session_doc = {
        "player_id": PLAYER_ID,
        "start_time": payload_dict.get("start_time", datetime.now().isoformat()),
        "status": "active",
        "maze_data": payload_dict,
        "created_at": datetime.now()
    }
    result = game_sessions_col.insert_one(session_doc)
    return result.inserted_id

def handle_movement_message(session_id, payload_dict, message_hash):
    """Handle player movement messages"""
    doc = {
        "session_id": session_id,
        "player": int(payload_dict.get("Player", PLAYER_ID)),
        "marsami": int(payload_dict.get("Marsami", 0)),
        "room_origin": int(payload_dict.get("RoomOrigin", 0)),
        "room_destiny": int(payload_dict.get("RoomDestiny", 0)),
        "status": int(payload_dict.get("Status", 0)),
        "timestamp": datetime.now(),
        "message_hash": message_hash
    }
    game_sessions_col.update_one(
        {"_id": session_id},
        {"$push": {"movement_messages": doc}}
    )

def handle_sound_message(session_id, payload_dict, message_hash):
    """Handle sound detection messages"""
    doc = {
        "session_id": session_id,
        "player": int(payload_dict.get("Player", PLAYER_ID)),
        "sound_level": float(payload_dict.get("Sound", 0)),
        "hour": payload_dict.get("Hour", datetime.now().isoformat()),
        "timestamp": datetime.now(),
        "message_hash": message_hash
    }
    game_sessions_col.update_one(
        {"_id": session_id},
        {"$push": {"sound_messages": doc}}
    )

def on_message(client, userdata, msg):
    """Improved message handling with separate stages"""
    try:
        # Stage 1: Raw message handling
        payload = msg.payload.decode("utf-8").strip()
        message_hash = hash_message(payload)
        logger.info(f"Received message on {msg.topic}: {payload}")
        
        # Immediately store raw message
        raw_messages_col.insert_one({
            "raw_payload": payload,
            "topic": msg.topic,
            "received_at": datetime.now(),
            "hash": message_hash
        })
        
        # Stage 2: Duplicate check
        if message_cache.exists(message_hash):
            logger.info(f"Skipping duplicate message: {message_hash}")
            return
            
        # Stage 3: Message parsing
        payload_dict = parse_payload(payload)
        logger.debug(f"Parsed payload: {payload_dict}")
        
        # Stage 4: Message type handling
        current_session = game_sessions_col.find_one(
            {"player_id": PLAYER_ID, "status": "active"},
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

def run():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe([(topic, 0) for topic in TOPICS])
    client.loop_forever()

if __name__ == "__main__":
    run()
