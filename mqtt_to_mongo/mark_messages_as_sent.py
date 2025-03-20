import os
from pymongo import MongoClient, errors
import logging
from datetime import datetime
import time
import queue
import json
from threading import Thread, Lock
from paho.mqtt import client as mqtt_client

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB Configuration
MONGO_USER = os.getenv('MONGO_USER', 'admin')
MONGO_PASS = os.getenv('MONGO_PASS', 'adminpass')
MONGO_DB = os.getenv('MONGO_DB', 'game_monitoring')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')
MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@mongo1:27017,mongo2:27017,mongo3:27017/"
    f"{MONGO_DB}?replicaSet=my-mongo-set&authSource={MONGO_AUTH_SOURCE}&"
    f"w=1&journal=false&retryWrites=true&"
    f"connectTimeoutMS=5000&socketTimeoutMS=5000&serverSelectionTimeoutMS=5000&"
    f"readPreference=primaryPreferred"
)

# MQTT Configuration
MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
PLAYER_ID = int(os.getenv('PLAYER_ID', '33'))
MQTT_TOPICS = {
    "move_messages": f"pisid_mazemov_{PLAYER_ID}_processed",
    "sound_messages": f"pisid_mazesound_{PLAYER_ID}_processed"
}
QOS = 1  # At-least-once delivery

# Message Queues
publish_queue = queue.Queue()  # For sending messages to MQTT
sent_queue = queue.Queue()     # For marking messages as sent
queue_lock = Lock()

def connect_to_mongodb():
    """Connect to MongoDB with retry logic"""
    while True:
        for attempt in range(5):
            try:
                client = MongoClient(MONGO_URI, maxPoolSize=1, minPoolSize=1)
                client.admin.command('ping')
                logger.info("Connected to MongoDB replica set")
                return client
            except errors.PyMongoError as e:
                logger.error(f"Connection attempt {attempt+1}/5 failed: {e}")
                if attempt < 4:
                    time.sleep(5)
        logger.error("Failed to reconnect to MongoDB, retrying in 5 seconds")
        time.sleep(5)

def connect_to_mqtt():
    """Connect to MQTT broker"""
    client = mqtt_client.Client(client_id=f"player_{PLAYER_ID}_sender", protocol=mqtt_client.MQTTv5)
    client.on_connect = on_connect
    client.on_publish = on_publish
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT)
            client.loop_start()
            logger.info(f"Connected to MQTT broker {MQTT_BROKER}:{MQTT_PORT}")
            return client
        except Exception as e:
            logger.error(f"MQTT connection failed: {e}")
            time.sleep(5)

def on_connect(client, userdata, flags, reason_code, properties=None):
    """Callback for MQTT connection"""
    if reason_code == 0:
        logger.info("MQTT connected successfully")
    else:
        logger.error(f"MQTT connection failed: {reason_code}")

def on_publish(client, userdata, mid):
    """Callback for when a message is published and acknowledged"""
    with queue_lock:
        sent_queue.put(mid)
    logger.info(f"Message {mid} acknowledged by broker")

def worker_publish(mqtt_client_instance):
    """Worker thread to publish messages to MQTT"""
    while True:
        message_data = publish_queue.get()
        try:
            message_id = message_data["message_id"]
            topic = message_data["topic"]
            payload = message_data["payload"]
            
            # Ensure 'hour' is a string if present (already should be from parse_payload)
            if "hour" in payload:
                if isinstance(payload["hour"], datetime):
                    payload["hour"] = payload["hour"].isoformat()
            
            # Publish with QoS 1
            result = mqtt_client_instance.publish(topic, json.dumps(payload), qos=QOS)
            if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
                logger.info(f"Published message {message_id} (MID {result.mid}) to {topic}")
                with queue_lock:
                    message_data["mid"] = result.mid
                    pending_messages[result.mid] = message_data
            else:
                logger.error(f"Failed to publish message {message_id} to {topic}: {result.rc}")
        except Exception as e:
            logger.error(f"Error publishing message {message_id}: {e}")
        finally:
            publish_queue.task_done()

def worker_mark_sent(db):
    """Worker thread to mark messages as sent after acknowledgment"""
    move_messages_col = db["move_messages"]
    sound_messages_col = db["sound_messages"]
    
    while True:
        mid = sent_queue.get()
        try:
            with queue_lock:
                if mid in pending_messages:
                    message_data = pending_messages.pop(mid)
                    message_id = message_data["message_id"]
                    collection_name = message_data["collection_name"]
                    
                    # Mark as sent in MongoDB
                    collection = move_messages_col if collection_name == "move_messages" else sound_messages_col
                    collection.update_one(
                        {"_id": message_id},
                        {"$set": {"sent": "true"}}
                    )
                    logger.info(f"Marked message {message_id} as sent after acknowledgment (MID {mid})")
        except errors.PyMongoError as e:
            logger.error(f"Failed to mark message {message_id} as sent: {e}")
        finally:
            sent_queue.task_done()

def mark_messages_as_sent(mongo_client, mqtt_client):
    """Continuously find unsent messages and queue them for publishing"""
    db = mongo_client[MONGO_DB]
    move_messages_col = db["move_messages"]
    sound_messages_col = db["sound_messages"]
    
    # Global dict to track pending messages by MID
    global pending_messages
    pending_messages = {}
    
    # Start worker threads
    for _ in range(2):  # Two threads for publishing
        Thread(target=worker_publish, args=(mqtt_client,), daemon=True).start()
    Thread(target=worker_mark_sent, args=(db,), daemon=True).start()  # One thread for marking sent
    
    while True:
        try:
            for collection, message_type in [
                (move_messages_col, "move_messages"),
                (sound_messages_col, "sound_messages")
            ]:
                unsent_messages = collection.find({"sent": "false"})
                for message in unsent_messages:
                    message_id = message["_id"]
                    try:
                        # Prepare payload, excluding all timestamps except 'hour'
                        payload = {
                            k: v for k, v in message.items()
                            if k not in ["_id", "session_id", "Already_moved", "sent", "sent_timestamp", "moved_timestamp", "timestamp"]
                        }
                        # Ensure 'hour' is included as a string for sound messages
                        if message_type == "sound_messages" and "hour" in message:
                            if isinstance(message["hour"], datetime):
                                payload["hour"] = message["hour"].isoformat()
                            else:
                                payload["hour"] = str(message["hour"])  # Already a string from parse_payload
                        
                        # Queue the message for publishing
                        with queue_lock:
                            publish_queue.put({
                                "message_id": message_id,
                                "collection_name": message_type,
                                "topic": MQTT_TOPICS[message_type],
                                "payload": payload
                            })
                            logger.info(f"Queued message {message_id} for publishing to {MQTT_TOPICS[message_type]}")
                    except Exception as e:
                        logger.error(f"Error queuing message {message_id}: {e}")
            time.sleep(1)  # Check every second
        except Exception as e:
            logger.error(f"Error in mark_messages_as_sent: {e}")
            time.sleep(5)  # Wait before retrying on major error

def main():
    mongo_client = connect_to_mongodb()
    mqtt_client = connect_to_mqtt()
    try:
        mark_messages_as_sent(mongo_client, mqtt_client)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        mongo_client.close()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        logger.info("Connections closed")

if __name__ == "__main__":
    main()
