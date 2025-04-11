import os
import threading
import queue
from datetime import datetime
from paho.mqtt import client as mqtt_client
from pymongo import MongoClient, errors
import logging
import time
import hashlib

# Logging setup
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
TIME_BETWEEN_MARSAMI_MOVEMENTS = 0.9

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
            return
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

def worker(topic: str, message_map: dict, hash_map: dict):
    """Process messages from the queue with duplicate checks."""
    cleanup_counter = 0
    while True:
        msg = topic_queues[topic].get()
        if msg is None:
            break
        try:
            payload_str = msg.payload.decode().strip()
            hash_value = hashlib.md5(msg.payload).hexdigest()
            current_time = time.time()
            is_duplicate = False

            if msg.qos == 1:
                if msg.mid in message_map:
                    is_duplicate = True
                    logger.debug(f"Duplicate QoS 1 message ID {msg.mid} for topic {topic}")
                else:
                    message_map[msg.mid] = hash_value
                    logger.debug(f"Added message ID {msg.mid} with hash {hash_value} for topic {topic}")
                #FIXME: If there is a need to check for duplicates from the sensor uncomment it 
            else:
                if hash_value in hash_map and (current_time - hash_map[hash_value]) < TIME_BETWEEN_MARSAMI_MOVEMENTS:
                    is_duplicate = True
                    logger.debug(f"Duplicate QoS 0 content hash {hash_value} for topic {topic}")
                hash_map[hash_value] = current_time

            if is_duplicate:
                failed_messages_col.insert_one({
                    "topic": msg.topic,
                    "session_id": SESSION_ID,
                    "payload": payload_str,
                    "reason": "Duplicate message",
                    "timestamp": datetime.now(),
                    "processed": True,
                    "message_id": msg.mid if msg.qos > 0 else None,
                    "QOS": msg.qos if msg.qos > 0 else None
                })
                logger.info(f"Duplicate message from {topic}: QoS={msg.qos}, MID={msg.mid}")
            else:
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
                for attempt in range(3):
                    try:
                        raw_messages_col.insert_one(document)
                        logger.info(f"Inserted message from {topic}: QoS={msg.qos}, MID={msg.mid}")
                        break
                    except errors.PyMongoError as e:
                        if attempt < 2:
                            logger.warning(f"Retry {attempt+1}/3 for insert: {e}")
                            time.sleep(1)
                        else:
                            raise

            # Periodic cleanup of hash_map for QoS 0 messages
            cleanup_counter += 1
            if cleanup_counter >= 100:
                cleanup_counter = 0
                to_remove = [k for k, v in hash_map.items() if current_time - v > 1]
                for k in to_remove:
                    del hash_map[k]
                logger.debug(f"Cleaned up {len(to_remove)} old entries from hash_map for topic {topic}")

        except Exception as e:
            logger.error(f"Error processing message from {topic}: {e}")
            failed_messages_col.insert_one({
                "topic": msg.topic,
                "session_id": SESSION_ID,
                "payload": payload_str,
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
    message_maps = {topic: {} for topic in TOPICS}  # message_id : hash_value | for QoS 1 in case of duplicates from the MQTT
    hash_maps = {topic: {} for topic in TOPICS}    # hash_value : timestamp  | for QoS 0/2 in case of duplicates from the sensor

    for topic in TOPICS:
        client = connect_mqtt(topic)
        topic_clients[topic] = client
        worker_thread = threading.Thread(
            target=worker,
            args=(topic, message_maps[topic], hash_maps[topic]),
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

    while True:
        time.sleep(60)

if __name__ == "__main__":
    logger.info(f"Starting MQTT listeners for player {player_id} on topics: {TOPICS}")
    main()
