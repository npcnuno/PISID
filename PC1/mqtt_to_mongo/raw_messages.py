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
PLAYER_ID = int(os.getenv('PLAYER_ID', '33'))
MQTT_BROKER = os.getenv('MQTT_BROKER', 'broker.mqtt-dashboard.com')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
TOPICS = [
    os.getenv('MOVEMENT_TOPIC', f'pisid_mazemov_{PLAYER_ID}'),
    os.getenv('SOUND_TOPIC', f'pisid_mazesound_{PLAYER_ID}')
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
TIME_BETWEEN_MARSAMI_MOVEMENTS = float(os.getenv("TIME_BETWEEN_MARSAMI_MOVEMENTS", 0.9))

# Topic-specific queues
topic_queues = {topic: queue.Queue() for topic in TOPICS}

def connect_to_mongodb(retry_count=5, initial_delay=1):
    """Establish connection to MongoDB with retry mechanism and exponential backoff."""
    global mongo_client, db, raw_messages_col, failed_messages_col
    delay = initial_delay
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
                time.sleep(delay)
                delay *= 2  # Exponential backoff
    logger.error("Failed to connect to MongoDB after retries")
    raise SystemExit(1)

def worker(topic: str, message_map: dict, hash_map: dict):
    """Process messages from the queue with duplicate checks and robust MongoDB writes."""
    cleanup_counter = 0
    while True:
        try:
            msg = topic_queues[topic].get()
            if msg is None:
                break
            document = {
                "topic": msg.topic,
                "session_id": SESSION_ID,
                "timestamp": datetime.now(),
            }
            if msg.qos > 0:
                document["message_id"] = msg.mid
                document["QOS"] = msg.qos

            try:
                payload_str = msg.payload.decode().strip()
                document["payload"] = payload_str
                hash_value = hashlib.md5(msg.payload).hexdigest()
                current_time = time.time()
                is_duplicate = False

                # Duplicate check
                if msg.qos == 1:
                    if hash_value == message_map.get(msg.mid):
                        is_duplicate = True
                        logger.debug(f"Duplicate QoS 1 message ID {msg.mid} for topic {topic}")
                    else:
                        message_map[msg.mid] = hash_value
                    logger.debug(f"Added message ID {msg.mid} with hash {hash_value} for topic {topic}")

                    if hash_value in hash_map and (current_time - hash_map[hash_value]) < TIME_BETWEEN_MARSAMI_MOVEMENTS:
                        is_duplicate = True
                        logger.debug(f"Duplicate content hash {hash_value} for topic {topic}")
                    hash_map[hash_value] = current_time
                else:
                    if hash_value in hash_map and (current_time - hash_map[hash_value]) < TIME_BETWEEN_MARSAMI_MOVEMENTS:
                        is_duplicate = True
                        logger.debug(f"Duplicate QoS 0 content hash {hash_value} for topic {topic}")
                    hash_map[hash_value] = current_time

                if is_duplicate:
                    document["processed"] = True
                    document["reason"] = "Duplicate message"
                else:
                    document["processed"] = False

            except Exception as e:
                logger.error(f"Error processing message from {topic}: {e}")
                document["payload"] = msg.payload.decode(errors='ignore').strip()
                document["processed"] = True
                document["error"] = str(e)

            # Attempt to insert into raw_messages with retries
            for attempt in range(3):
                try:
                    raw_messages_col.insert_one(document)
                    logger.info(f"Inserted message from {topic}: QoS={msg.qos}, MID={msg.mid}, processed={document['processed']}")
                    break
                except errors.PyMongoError as e:
                    if attempt < 2:
                        logger.warning(f"Retry {attempt+1}/3 for insert: {e}")
                        time.sleep(1)
                    else:
                        logger.error(f"Failed to insert message from {topic} after 3 attempts: {e}")
                        try:
                            failed_messages_col.insert_one(document)
                            logger.info(f"Inserted message into failed_messages from {topic}")
                        except Exception as fallback_e:
                            logger.error(f"Failed to insert into failed_messages: {fallback_e}")
                        break

            # Periodic cleanup of hash_map
            cleanup_counter += 1
            if cleanup_counter >= 100:
                cleanup_counter = 0
                current_time = time.time()
                to_remove = [k for k, v in hash_map.items() if current_time - v > 1]
                for k in to_remove:
                    del hash_map[k]
                logger.debug(f"Cleaned up {len(to_remove)} old entries from hash_map for topic {topic}")

        except Exception as e:
            logger.error(f"Unexpected error in worker for topic {topic}: {e}")
        finally:
            topic_queues[topic].task_done()

def main():
    """Main function to set up connections and start threads."""
    required_env_vars = ["PLAYER_ID", "SESSION_ID", "MQTT_BROKER", "MQTT_PORT"]
    for var in required_env_vars:
        if not os.getenv(var):
            logger.error(f"Missing required environment variable: {var}")
            raise SystemExit(1)

    connect_to_mongodb()
    message_maps = {topic: {} for topic in TOPICS}
    hash_maps = {topic: {} for topic in TOPICS}

    client_id = f"player_{PLAYER_ID}_raw"
    client = mqtt_client.Client(client_id=client_id)

    def on_connect(c, u, f, rc):
        logger.info(f"Connected with result code {rc}")
        subscriptions = [(topic, 1) for topic in TOPICS]
        result, mid = c.subscribe(subscriptions)
        if result == mqtt_client.MQTT_ERR_SUCCESS:
            logger.info(f"Subscribed to topics with mid {mid}")
        else:
            logger.error(f"Subscription failed with result {result}")

    def on_message(c, u, msg):
        if msg.topic in topic_queues:
            topic_queues[msg.topic].put(msg)
            logger.debug(f"Received message on topic {msg.topic}: QoS={msg.qos}, MID={msg.mid}")
        else:
            logger.warning(f"Received message on unknown topic: {msg.topic}")

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        logger.info("MQTT client connected successfully")
    except Exception as e:
        logger.error(f"Failed to connect MQTT client: {e}")
        raise

    # Start worker threads
    worker_threads = {}
    for topic in TOPICS:
        worker_thread = threading.Thread(
            target=worker,
            args=(topic, message_maps[topic], hash_maps[topic]),
            daemon=True
        )
        worker_thread.start()
        worker_threads[topic] = worker_thread

    # Start client loop
    client.loop_start()

    while True:
        time.sleep(60)

if __name__ == "__main__":
    logger.info(f"Starting MQTT listener for player {PLAYER_ID} on topics: {TOPICS}")
    main()