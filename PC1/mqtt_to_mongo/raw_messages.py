import os
import json
import threading
import queue
import time
from datetime import datetime
from paho.mqtt import client as mqtt_client
from pymongo import MongoClient, errors
import logging
import hashlib

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment variables
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://admin:adminpass@mongo1:27017,mongo2:27017,mongo3:27017/game_monitoring?replicaSet=my-mongo-set&authSource=admin')
MQTT_BROKER = os.getenv('MQTT_BROKER', 'test.mosquitto.org')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
PLAYER_ID = int(os.getenv('PLAYER_ID', '33'))
SESSION_ID = os.getenv('SESSION_ID', 'default_session')
TOPICS_CONFIG = json.loads(os.getenv('TOPICS_CONFIG', '[]'))
DELAY_TIME_BETWEEN_MESSAGES = int(os.getenv('DELAY_TIME_BETWEEN_MESSAGES', '60')) 
MONGO_DB = os.getenv("MONGO_DB", "game_monitoring")
# MongoDB setup
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['game_monitoring']
raw_messages_col = db['raw_messages']

# Topic-specific queues and duplicate tracking
topic_queues = {}
message_maps = {}
hash_maps = {}


def connect_to_mongodb(retry_count=5, retry_delay=5):
    global mongo_client, db
    for attempt in range(retry_count):
        try:
            mongo_client = MongoClient(
                MONGO_URI,
                maxPoolSize=20,
                minPoolSize=1,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000,
                serverSelectionTimeoutMS=5000,
                retryWrites=True
            )
            mongo_client.admin.command('ping')
            logger.info("Connected to MongoDB")
            db = mongo_client[MONGO_DB]
            return mongo_client
        except errors.PyMongoError as e:
            logger.error(f"MongoDB connection failed (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
    logger.error("Failed to connect to MongoDB")
    raise SystemExit(1)

def connect_mqtt():
    client = mqtt_client.Client(client_id=f"player_{PLAYER_ID}_raw")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT)
    return client

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        for config in TOPICS_CONFIG:
            topic = config['topic'].format(player_id=PLAYER_ID)
            client.subscribe(topic, qos=1)
            logger.info(f"Subscribed to {topic}")
    else:
        logger.error(f"Failed to connect to MQTT: {rc}")

def worker(topic: str):
    while True:
        try:
            msg = topic_queues[topic].get()
            if msg is None:
                break
            document = {
                "topic": msg.topic,
                "session_id": SESSION_ID,
                "timestamp": datetime.now(),
                "payload": msg.payload.decode(),
                "processed": False
            }
            if msg.qos > 0:
                document["message_id"] = msg.mid
                document["qos"] = msg.qos
            hash_value = hashlib.md5(msg.payload).hexdigest()
            current_time = time.time()
            is_duplicate = False
            with lock:
                if msg.qos == 1 and msg.mid in message_maps[topic]:
                    if hash_value == message_maps[topic][msg.mid]:
                        is_duplicate = True
                    else:
                        message_maps[topic][msg.mid] = hash_value
                if hash_value in hash_maps[topic] and (current_time - hash_maps[topic][hash_value]) < DELAY_TIME_BETWEEN_MESSAGES:
                    is_duplicate = True
                hash_maps[topic][hash_value] = current_time
            if is_duplicate:
                document["processed"] = True
                document["reason"] = "Duplicate message"
            raw_messages_col.insert_one(document)
            logger.info(f"Inserted message from {topic}")
        except Exception as e:
            logger.error(f"Error in worker for {topic}: {e}")
        finally:
            topic_queues[topic].task_done()

def on_message(client, userdata, msg):
    topic = msg.topic
    if topic in topic_queues:
        topic_queues[topic].put(msg)
        logger.debug(f"Received message on {topic}")
    else:
        logger.warning(f"Unknown topic: {topic}")

def main():
    global lock
    lock = threading.Lock()
    for config in TOPICS_CONFIG:
        topic = config['topic'].format(player_id=PLAYER_ID)
        topic_queues[topic] = queue.Queue()
        message_maps[topic] = {}
        hash_maps[topic] = {}
        worker_thread = threading.Thread(target=worker, args=(topic,), daemon=True)
        worker_thread.start()
    mqtt_client = connect_mqtt()
    mqtt_client.loop_forever()

if __name__ == "__main__":
    main()
