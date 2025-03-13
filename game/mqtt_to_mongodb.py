import paho.mqtt.client as mqtt
from pymongo import MongoClient
import hashlib
import logging
from datetime import datetime
import json
import re
from threading import Lock, Thread
import queue
import signal
import sys
import time

# Configuration
PLAYER_ID = 33  # Replace with your player ID
MQTT_BROKER = "broker.mqtt-dashboard.com"  # Replace with your broker
MQTT_PORT = 1883
TOPICS = [
    f"pisid_mazeNovo_{PLAYER_ID}",  # This topic must be created by publishing to it
    f"pisid_mazemov_{PLAYER_ID}",
    f"pisid_mazesound_{PLAYER_ID}"
]
MONGO_URI = "mongodb://mongo:27017"  # Replace with your MongoDB URI
CACHE_TTL = 300  # Time-to-live for cached message hashes (seconds)
NUM_WORKERS = 8  # Number of worker threads

# Logging setup with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] [Thread-%(thread)d] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# MongoDB connection with optimized settings for real-time processing
try:
    mongo_client = MongoClient(
        MONGO_URI,
        maxPoolSize=50,  # Increase connection pool size
        socketTimeoutMS=5000,
        connectTimeoutMS=5000,
        serverSelectionTimeoutMS=5000,
        w=1,  # Write concern: acknowledge writes after they've been written to primary
        journal=False  # Don't wait for journal commit for better performance
    )
    res = mongo_client.server_info()  # Test connection
    logger.info(f"MongoDB connected: {res['version']}")
    db = mongo_client["game_monitoring"]
    game_sessions_col = db["game_sessions"]
    raw_messages_col = db["raw_messages"]
    move_messages_col = db["move_messages"]
    db.move_messages_col.remove({})
    sound_messages_col = db["sound_messages"]
    db.sound_messages_col.remove({})
    failed_messages_col = db["failed_messages"]
    
    # Create indexes for better query performance
    game_sessions_col.create_index([("player_id", 1), ("status", 1)])
    game_sessions_col.create_index([("start_time", -1)])
    raw_messages_col.create_index([("received_at", -1)])
    move_messages_col.create_index([("session_id", 1)])
    sound_messages_col.create_index([("session_id", 1)])
    failed_messages_col.create_index([("timestamp", -1)])
    
except Exception as e:
    logger.error(f"MongoDB connection failed: {e}")
    sys.exit(1)

# In-memory message cache using TTL with efficient locking
class MessageCache:
    def __init__(self, max_size=100000):
        self.cache = {}
        self.lock = Lock()
        self.max_size = max_size
        self.cleanup_counter = 0

    def add(self, hash_value):
        with self.lock:
            self.cache[hash_value] = datetime.now()
            
            # Periodic cleanup to prevent memory bloat
            self.cleanup_counter += 1
            if self.cleanup_counter > 1000:  # Clean up every 1000 additions
                self._cleanup()
                self.cleanup_counter = 0

    def exists(self, hash_value):
        with self.lock:
            if hash_value in self.cache:
                if (datetime.now() - self.cache[hash_value]).seconds < CACHE_TTL:
                    return True
                else:
                    del self.cache[hash_value]
            return False
    
    def _cleanup(self):
        """Remove expired entries from cache"""
        now = datetime.now()
        expired_keys = [k for k, v in self.cache.items() 
                       if (now - v).seconds > CACHE_TTL]
        for k in expired_keys:
            del self.cache[k]
        
        # If still too large, remove oldest entries
        if len(self.cache) > self.max_size:
            sorted_keys = sorted(self.cache.items(), key=lambda x: x[1])
            to_remove = len(self.cache) - self.max_size
            for k, _ in sorted_keys[:to_remove]:
                del self.cache[k]

message_cache = MessageCache()

# Session cache to reduce database lookups
active_session_cache = {
    "id": None,
    "timestamp": None,
    "lock": Lock()
}

def get_active_session():
    """Get the active session with caching to reduce DB lookups"""
    with active_session_cache["lock"]:
        now = datetime.now()
        
        # Use cached session if available and recently retrieved
        if (active_session_cache["id"] is not None and 
            active_session_cache["timestamp"] is not None and
            (now - active_session_cache["timestamp"]).seconds < 5):
            return active_session_cache["id"]
        
        # Otherwise fetch from DB
        session = game_sessions_col.find_one(
            {"player_id": PLAYER_ID, "status": "active"},
            sort=[("start_time", -1)]
        )
        
        if session:
            active_session_cache["id"] = session["_id"]
            active_session_cache["timestamp"] = now
            return session["_id"]
        
        return None

# Utility functions
def hash_message(payload):
    """Generate a SHA-256 hash of the message payload."""
    return hashlib.sha256(payload.encode()).hexdigest()


def parse_payload(payload):
    """Parse JSON payload, fixing unquoted keys."""
    try:
        # Use a regex that only matches keys that follow { or ,
        fixed = re.sub(r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3', payload)
        return json.loads(fixed)
    except json.JSONDecodeError:
        try:
            # Try replacing single quotes with double quotes and fix keys again
            payload = payload.replace("'", '"')
            fixed = re.sub(r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3', payload)
            return json.loads(fixed)
        except Exception as e:
            logger.error(f"Failed alternative parsing: {e}")
            return None
# Handler functions
def handle_maze_novo(payload_dict):
    """Handle new maze session creation."""
    logger.info("Creating new session")
    
    # Close active sessions asynchronously
    def close_active_sessions():
        game_sessions_col.update_many(
            {"player_id": PLAYER_ID, "status": "active"},
            {"$set": {"status": "completed", "end_time": datetime.now()}}
        )
    
    Thread(target=close_active_sessions).start()
    
    session_doc = {
        "player_id": PLAYER_ID,
        "start_time": datetime.now(),
        "status": "active",
        "maze_config": payload_dict,
        "movements": [],
        "sounds": [],
        "created_at": datetime.now()
    }
    
    result = game_sessions_col.insert_one(session_doc)
    session_id = result.inserted_id
    
    # Update cache
    with active_session_cache["lock"]:
        active_session_cache["id"] = session_id
        active_session_cache["timestamp"] = datetime.now()
    
    logger.info(f"Created new session ID {session_id}")
    return session_id

def handle_movement(session_id, payload_dict):
    """Handle movement message."""
    try:
        move_doc = {
            "session_id": session_id,
            "player": int(payload_dict.get("Player", 0)),
            "from_room": int(payload_dict.get("RoomOrigin", 0)),
            "to_room": int(payload_dict.get("RoomDestiny", 0)),
            "status": int(payload_dict.get("Status", 0)),
            "marsami": int(payload_dict.get("Marsami", 0)),
            "timestamp": datetime.now()
        }
        
        # Stream data - immediate write to MongoDB
        result = move_messages_col.insert_one(move_doc)
        
        # Update session in background to not block the processing
        def update_session():
            game_sessions_col.update_one(
                {"_id": session_id},
                {"$push": move_doc}
            )
        
        Thread(target=update_session).start()
        logger.info(f"Wrote movement data for session {session_id}")
        
    except (ValueError, KeyError) as e:
        logger.error(f"Error processing movement message: {e}")
        return False
    
    return True


def handle_sound(session_id, payload_dict):
    """Handle sound message with Hour as timestamp."""
    try:
        logger.debug(f"handle_sound received payload: {payload_dict}")
        hour_str = payload_dict.get("Hour")
        if hour_str:
            try:
                timestamp = datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                timestamp = datetime.now()
        else:
            timestamp = datetime.now()
        
        sound_doc = {
            "session_id": session_id,
            "player": int(payload_dict.get("Player", 0)),
            "level": float(payload_dict.get("Sound", 0.0)),
            "timestamp": timestamp
        }
        
        result = sound_messages_col.insert_one(sound_doc)
        
        def update_session():
            game_sessions_col.update_one(
                {"_id": session_id},
                {"$push": sound_doc}
            )
        
        Thread(target=update_session).start()
        logger.info(f"Wrote sound data for session {session_id}")
        
    except (ValueError, KeyError) as e:
        logger.error(f"Error processing sound message: {e}")
        return False
    
    return True


def process_message(msg):
    """Process a single message from the queue."""
    try:
        start_time = time.time()
        payload = msg.payload.decode().strip()
        
        # STEP 1: Write raw message to DB immediately
        raw_msg_doc = {
            "topic": msg.topic,
            "payload": payload,
            "received_at": datetime.now()
        }
        raw_messages_col.insert_one(raw_msg_doc)
        logger.info(f"Wrote raw message to database from topic {msg.topic}")
        
        # STEP 2: Check for duplicates
        msg_hash = hash_message(payload)
        if message_cache.exists(msg_hash):
            logger.info(f"Duplicate message on {msg.topic} - skipping further processing")
            return
        
        # STEP 3: Parse the payload, and if parsing fails, write to failed_messages
        payload_dict = parse_payload(payload)
        if payload_dict is None:
            failed_messages_col.insert_one({
                "topic": msg.topic,
                "payload": payload,
                "reason": "Failed to parse payload",
                "timestamp": datetime.now()
            })
            logger.warning(f"Failed to parse payload on {msg.topic}")
            return
        
        # STEP 4: Process message based on topic
        if "mazeNovo" in msg.topic:
            session_id = handle_maze_novo(payload_dict)
        else:
            session_id = get_active_session()
            if not session_id:
                logger.info("No active session found, creating one")
                default_config = {"auto_created": True, "timestamp": datetime.now().isoformat()}
                session_id = handle_maze_novo(default_config)
            
            if "mazemov" in msg.topic:
                handle_movement(session_id, payload_dict)
            elif "mazesound" in msg.topic:
                handle_sound(session_id, payload_dict)
        
        # STEP 5: After successful processing, add message hash to cache
        message_cache.add(msg_hash)
        
        process_time = (time.time() - start_time) * 1000  # in ms
        logger.info(f"Message processed in {process_time:.2f}ms")
    
    except Exception as e:
        logger.error(f"Processing error: {str(e)}", exc_info=True)
        try:
            failed_messages_col.insert_one({
                "topic": msg.topic,
                "payload": payload,
                "reason": f"Unexpected error: {str(e)}",
                "timestamp": datetime.now()
            })
            logger.error("Unexpected error processing message. Wrote to failed_messages collection")
        except Exception as inner_e:
            logger.critical(f"Failed to record error in database: {inner_e}")

# Message queue and worker setup for high throughput
message_queue = queue.Queue(maxsize=5000)  # Limit queue size to prevent memory issues

# MQTT callbacks


def on_message(client, userdata, msg):
    """Add message to queue for processing."""
    try:
        message_queue.put_nowait(msg)
        logger.debug(f"Queued message from {msg.topic}")
    except queue.Full:
        logger.warning(f"Queue full, dropping message from {msg.topic}")



# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    """Handle connection to MQTT broker."""
    if rc == 0:
        logger.info("Connected to MQTT broker")
        # Subscribe to all topics
        client.subscribe([(t, 0) for t in TOPICS])
        logger.info(f"Subscribed to topics: {TOPICS}")
        # Create mazeNovo topic by publishing to it once
        logger.info(f"Creating mazeNovo topic by publishing to {TOPICS[0]}")
        client.publish(TOPICS[0], json.dumps({"init": True, "timestamp": datetime.now().isoformat()}))
    else:
        logger.error(f"Failed to connect to MQTT broker with code {rc}")

def on_disconnect(client, userdata, rc):
    """Handle disconnection from MQTT broker."""
    if rc != 0:
        logger.warning(f"Unexpected MQTT disconnection with code {rc}, attempting to reconnect...")
        try:
            client.reconnect()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")

# Worker function remains unchanged
def worker():
    """Process messages from the queue in a separate thread."""
    while True:
        try:
            msg = message_queue.get(timeout=1)
            if msg is None:  # Termination signal
                message_queue.task_done()
                break
            try:
                process_message(msg)
            finally:
                message_queue.task_done()
        except queue.Empty:
            continue

# Shutdown handling
def shutdown(signum, frame):
    """Handle graceful shutdown."""
    logger.info("Shutdown signal received, cleaning up...")
    client.disconnect()

if __name__ == "__main__":
    # Start worker threads (non-daemon)
    workers = []
    for _ in range(NUM_WORKERS):
        t = Thread(target=worker)
        t.start()
        workers.append(t)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    # MQTT client setup
    client = mqtt.Client()
    
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe([(topic, 0) for topic in TOPICS])

    try:

        client.loop_start()
        
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Stopping MQTT client...")
        client.loop_stop()
        
        logger.info("Waiting for queued messages to process...")
        message_queue.join()  # Wait until all messages are processed
        
        logger.info("Stopping worker threads...")
        # Signal workers to exit
        for _ in range(NUM_WORKERS):
            message_queue.put(None)
        
        # Wait for workers to finish
        for t in workers:
            t.join()
        logger.info("All workers stopped")
