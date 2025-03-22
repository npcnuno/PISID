import os
import threading
import queue
from datetime import datetime
from paho.mqtt import client as mqtt_client
from pymongo import MongoClient, errors 
import logging
import time

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@"
    f"mongo1:27017,mongo2:27017,mongo3:27017/"
    f"{MONGO_DB}?replicaSet=my-mongo-set&"
    f"authSource={MONGO_AUTH_SOURCE}&w=1&journal=true&"
    f"retryWrites=true&connectTimeoutMS=5000&socketTimeoutMS=5000&"
    f"serverSelectionTimeoutMS=5000&readPreference=primaryPreferred"
)

# Topic-specific queues and threads
topic_queues = {topic: queue.Queue() for topic in TOPICS}
topic_clients = {}
topic_threads = {}

def connect_to_mongodb(retry_count=5, retry_delay=5):
    """Purpose: Establishes a connection to MongoDB with retry logic for reliability.
    Execution Flow:
    1. Declare global variables mongo_client and db to be set upon successful connection.
    2. Loop through retry_count attempts (default 5) to connect to MongoDB.
    3. Create a MongoClient with MONGO_URI and settings like maxPoolSize=20, retryWrites=True.
    4. Send a 'ping' command to verify the connection.
    5. On success, set mongo_client and db globals, log success, and return True.
    6. On failure (PyMongoError), log the error with attempt details.
    7. If not the last attempt, wait retry_delay seconds (default 5) before retrying.
    8. If all attempts fail, log a final error and raise SystemExit to terminate."""
    global mongo_client, db, raw_messages_col,failed_messages_col
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
    """Purpose: Creates and connects an MQTT client for a specific topic to receive raw messages.
    Execution Flow:
    1. Generate a unique client_id using player_id and topic type (e.g., 'player_33_mazemov_raw').
    2. Create an MQTT client instance with the generated client_id.
    3. Define on_connect callback to log connection status and subscribe to the topic with QoS 1.
    4. Define on_message callback to queue incoming messages in topic_queues[topic].
    5. Set the callbacks on the client.
    6. Attempt to connect to MQTT_BROKER:MQTT_PORT with a 60-second keepalive.
    7. On success, log the connection and return the client.
    8. On failure, log the error and raise an exception to halt execution."""
    client_id = f"player_{player_id}_{topic.split('_')[1]}_raw"
    client = mqtt_client.Client(client_id=client_id)
    def on_connect(c, u, f, rc):
        logger.info(f"Client for topic {topic} connected with result code {rc}")
        c.subscribe(topic, qos=1)
    def on_message(c, u, msg):
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

def worker(topic: str):
    """Purpose: Processes messages from a topic-specific queue and inserts them into MongoDB.
    Execution Flow:
    1. Enter an infinite loop to process messages from topic_queues[topic].
    2. Retrieve a message from the queue (blocks until a message is available).
    3. If the message is None, break the loop (shutdown signal).
    4. Check the message QoS level; if > 0, include message_id in the document.
    5. Create a document with topic, session_id, decoded payload, timestamp, processed=False, and optional QoS/message_id.
    6. Insert the document into raw_messages_col and log the insertion with QoS and MID.
    7. On error, log the exception and insert a failure record into failed_messages_col with error details.
    8. Mark the task as done in the queue to allow further processing."""
    while True:
        msg = topic_queues[topic].get()
        if msg is None:
            break
        try:
            if msg.qos > 0:
                raw_messages_col.insert_one({
                    "topic": msg.topic,
                    "session_id": SESSION_ID,
                    "payload": msg.payload.decode().strip(),
                    "message_id": msg.mid,
                    "timestamp": datetime.now(),
                    "processed": False,
                    "QOS": msg.qos,
                    "gametime": msg.timestamp,
                })
            else:
                raw_messages_col.insert_one({
                    "topic": msg.topic,
                    "session_id": SESSION_ID,
                    "payload": msg.payload.decode().strip(),
                    "timestamp": datetime.now(),
                    "processed": False,
                    "gametime": msg.timestamp,
                })
            logger.info(f"Inserted message from {topic}: QoS={msg.qos}, MID={msg.mid}")
        except Exception as e:
            logger.error(f"Error inserting message from {topic}: {e}")
            failed_messages_col.insert_one({
                "topic": msg.topic,
                "session_id": SESSION_ID,
                "payload": msg.payload.decode().strip(),
                "error": str(e),
                "timestamp": datetime.now(),
                "processed": True
            })
        finally:
            topic_queues[topic].task_done()

def start_client_loop(client: mqtt_client.Client, topic: str):
    """Purpose: Runs the MQTT client loop in a separate thread to maintain connectivity and message reception.
    Execution Flow:
    1. Enter a try block to run client.loop_forever(), which handles MQTT network events.
    2. On success, the function runs indefinitely, processing incoming messages via on_message.
    3. On exception (e.g., network failure), log the error with the topic and exit the thread."""
    try:
        client.loop_forever()
    except Exception as e:
        logger.error(f"MQTT client loop for topic {topic} failed: {e}")

def main():
    """Purpose: Initializes MQTT clients and worker threads for each topic to capture raw messages.
    Execution Flow:
    1. Iterate over the list of TOPICS (e.g., mazemov and mazesound for the player).
    2. For each topic, call connect_mqtt to create and connect an MQTT client.
    3. Store the client in topic_clients[topic].
    4. Start a daemon worker thread for the topic using the worker function and store in topic_threads.
    5. Start a daemon thread to run the client loop using start_client_loop.
    6. Enter an infinite loop, sleeping 60 seconds per iteration, to keep the main thread alive."""
    connect_to_mongodb()
    for topic in TOPICS:
        client = connect_mqtt(topic)
        topic_clients[topic] = client
        worker_thread = threading.Thread(target=worker, args=(topic,), daemon=True)
        worker_thread.start()
        topic_threads[topic] = worker_thread
        client_thread = threading.Thread(target=start_client_loop, args=(client, topic), daemon=True)
        client_thread.start()
    while True:
        time.sleep(60)

if __name__ == "__main__":
    logger.info(f"Starting MQTT listeners for player {player_id} on topics: {TOPICS}")
    main()
