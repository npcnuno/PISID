import os
import threading
import queue
from datetime import datetime
from paho.mqtt import client as mqtt_client
from pymongo import MongoClient, errors 
import logging
import time
import hashlib

# Logging setup (set to DEBUG to capture debug logs)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
player_id = int(os.getenv('PLAYER_ID', '33'))
MQTT_BROKER = os.getenv('MQTT_BROKER', 'broker.mqtt-dashboard.com')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
TOPICS = [
    os.getenv('MOVEMENT_TOPIC', f'pisid_mazemov_{player_id}'),
    os.getenv('SOUND_TOPIC', f'pisid_mazesound_{player_id}')
]
SESSION_ID = os.getenv('SESSION_ID')
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

# Topic-specific queues and threads
topic_queues = {topic: queue.Queue() for topic in TOPICS}
topic_clients = {}
topic_threads = {}

def connect_to_mongodb(retry_count=5, retry_delay=5):
    """Establish connection to MongoDB with retry mechanism."""
    global mongo_client, db, raw_messages_col, failed_messages_col
    for attempt in range(retry_count):
        try:
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
            raw_messages_col = db["raw_messages"]
            failed_messages_col = db["failed_messages"]
            return True
        except errors.PyMongoError as e:
            logger.error(f"Connection failed (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
    logger.error("Failed to connect to MongoDB")
    raise SystemExit(1)

def connect_mqtt(topic: str) -> mqtt_client.Client:
    """Connect to MQTT broker and set up callbacks for a specific topic."""
    client_id = f"player_{player_id}_{topic.split('_')[1]}_raw"
    client = mqtt_client.Client(client_id=client_id)
    
    def on_connect(c, u, f, rc):
        logger.info(f"Client for topic {topic} connected with result code {rc}")
        c.subscribe(topic, qos=1)
    
    def on_message(c, u, msg):
        logger.debug(f"Received message on topic {topic}: QoS={msg.qos}, MID={msg.mid}")
        topic_queues[topic].put(msg)
    
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        logger.info(f"MQTT client for topic {topic} connected successfully")
    except Exception as e:
        logger.error(f"Failed to connect MQTT client for topic {topic}: {e}")
        raise
    return client

def worker(topic: str, seen_ids: set, hash_map: dict):
    """Process messages from the queue with duplicate checks."""
    while True:
        msg = topic_queues[topic].get()
        if msg is None:
            break
        try:
            # Compute hash of the payload (using raw bytes)
            hash_value = hashlib.md5(msg.payload).hexdigest()
            current_time = time.time()
            
            # Check for duplicate based on content (100ms TTL)
            is_content_duplicate = False
            if hash_value in hash_map:
                last_seen = hash_map[hash_value]
                time_diff = current_time - last_seen
                logger.debug(f"Hash {hash_value} found in hash_map for topic {topic}, time since last seen: {time_diff:.4f}s")
                if time_diff < 0.1:
                    is_content_duplicate = True
                    logger.debug(f"Content duplicate detected for topic {topic}, hash {hash_value}")
            # Update the hash_map with current timestamp
            hash_map[hash_value] = current_time
            
            # Check for duplicate based on message ID if QoS == 1
            is_mid_duplicate = False
            if msg.qos == 1:
                logger.debug(f"Checking QoS 1 message ID {msg.mid} for topic {topic}")
                if msg.mid in seen_ids:
                    is_mid_duplicate = True
                    logger.debug(f"Message ID duplicate detected for topic {topic}, MID {msg.mid}")
                else:
                    seen_ids.add(msg.mid)
                    logger.debug(f"Added new message ID {msg.mid} to seen_ids for topic {topic}")
            
            # Decode payload for storage (assuming UTF-8 text)
            payload_str = msg.payload.decode().strip()
            
            if is_content_duplicate or is_mid_duplicate:
                reason = "Duplicate content" if is_content_duplicate else "Duplicate message ID"
                failed_messages_col.insert_one({
                    "topic": msg.topic,
                    "session_id": SESSION_ID,
                    "payload": payload_str,
                    "reason": reason,
                    "timestamp": datetime.now(),
                    "processed": True,
                    "message_id": msg.mid if msg.qos > 0 else None,
                    "QOS": msg.qos if msg.qos > 0 else None
                })
                logger.info(f"Duplicate message from {topic}: {reason}, QoS={msg.qos}, MID={msg.mid}")
            else:
                # Insert into raw_messages_col
                document = {
                    "topic": msg.topic,
                    "session_id": SESSION_ID,
                    "payload": payload_str,
                    "timestamp": datetime.now(),
                    "processed": False,
                }
                if msg.qos > 0:
                    document["message_id"] = msg.mid
                    document["QOS"] = msg.qos
                raw_messages_col.insert_one(document)
                logger.info(f"Inserted message from {topic}: QoS={msg.qos}, MID={msg.mid}")
                logger.debug(f"Message inserted into raw_messages_col for topic {topic}, hash {hash_value}")
        
        except Exception as e:
            logger.error(f"Error processing message from {topic}: {e}")
            failed_messages_col.insert_one({
                "topic": msg.topic,
                "session_id": SESSION_ID,
                "payload": msg.payload.decode().strip(),
                "error": str(e),
                "timestamp": datetime.now(),
                "processed": True,
                "message_id": msg.mid if msg.qos > 0 else None,
                "QOS": msg.qos if msg.qos > 0 else None
            })
        finally:
            topic_queues[topic].task_done()

def start_client_loop(client: mqtt_client.Client, topic: str):
    """Run the MQTT client loop for a specific topic."""
    try:
        client.loop_forever()
    except Exception as e:
        logger.error(f"MQTT client loop for topic {topic} failed: {e}")

def main():
    """Main function to set up connections and start threads."""
    connect_to_mongodb()
    
    # Initialize data structures for duplicate checks
    seen_message_ids = {topic: set() for topic in TOPICS}
    message_hashes = {topic: {} for topic in TOPICS}
    
    # Start MQTT clients and worker threads for each topic
    for topic in TOPICS:
        client = connect_mqtt(topic)
        topic_clients[topic] = client
        worker_thread = threading.Thread(
            target=worker,
            args=(topic, seen_message_ids[topic], message_hashes[topic]),
            daemon=True
        )
        worker_thread.start()
        topic_threads[topic] = worker_thread
        client_thread = threading.Thread(
            target=start_client_loop,
            args=(client, topic),
            daemon=True
        )
        client_thread.start()
    
    # Keep the main thread running
    while True:
        time.sleep(60)

if __name__ == "__main__":
    logger.info(f"Starting MQTT listeners for player {player_id} on topics: {TOPICS}")
    main()
