import os
import threading
import queue
from datetime import datetime
from paho.mqtt import client as mqtt_client
from pymongo import MongoClient
import logging
import time
SESSION_ID = os.getenv('SESSION_ID')

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
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://admin:adminpass@mongo1:27017,mongo2:27017,mongo3:27017/game_monitoring?replicaSet=my-mongo-set&authSource=admin')

# MongoDB setup
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["game_monitoring"]
raw_messages_col = db["raw_messages"]
failed_messages_col = db["failed_messages"]

# Topic-specific queues and threads
topic_queues = {topic: queue.Queue() for topic in TOPICS}
topic_clients = {}
topic_threads = {}

def connect_mqtt(topic: str) -> mqtt_client.Client:
    """Create and connect an MQTT client for a specific topic."""
    client_id = f"player_{player_id}_{topic.split('_')[1]}_raw"  # Unique client ID per topic
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
    """Worker thread to process messages from a specific topic's queue."""
    while True:
        msg = topic_queues[topic].get()
        if msg is None:
            break
        try:
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
    """Run the MQTT client loop in a separate thread."""
    try:
        client.loop_forever()
    except Exception as e:
        logger.error(f"MQTT client loop for topic {topic} failed: {e}")

def main():
    # Initialize one client and one worker thread per topic
    for topic in TOPICS:
        # Create and connect MQTT client
        client = connect_mqtt(topic)
        topic_clients[topic] = client

        # Start worker thread for this topic
        worker_thread = threading.Thread(target=worker, args=(topic,), daemon=True)
        worker_thread.start()
        topic_threads[topic] = worker_thread

        # Start MQTT client loop in a separate thread
        client_thread = threading.Thread(target=start_client_loop, args=(client, topic), daemon=True)
        client_thread.start()

    # Keep the main thread alive
    while True:
        time.sleep(60)

if __name__ == "__main__":
    logger.info(f"Starting MQTT listeners for player {player_id} on topics: {TOPICS}")
    main()
