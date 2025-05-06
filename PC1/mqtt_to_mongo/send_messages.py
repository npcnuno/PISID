import os
from pymongo import MongoClient, errors
from bson.objectid import ObjectId
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
MONGO_URI = os.getenv('MONGO_URI', (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@mongo1:27017,mongo2:27017,mongo3:27017/"
    f"{MONGO_DB}?replicaSet=my-mongo-set&authSource={MONGO_AUTH_SOURCE}&"
    f"w=1&journal=true&retryWrites=true&"
    f"connectTimeoutMS=5000&socketTimeoutMS=5000&serverSelectionTimeoutMS=5000&"
    f"readPreference=primaryPreferred"
))

# MQTT Configuration
MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
PLAYER_ID = int(os.getenv('PLAYER_ID', '33'))
SESSION_ID = os.getenv('SESSION_ID', 'default_session')
MQTT_TOPICS = {
    "move_messages": f"pisid_mazemov_{PLAYER_ID}_processed",
    "sound_messages": f"pisid_mazesound_{PLAYER_ID}_processed"
}
CONFIRMED_TOPICS = {
    "move_messages": f"pisid_mazemov_{PLAYER_ID}_confirmed",
    "sound_messages": f"pisid_mazesound_{PLAYER_ID}_confirmed"
}
QOS = int(os.getenv("QOS", 2))
POLL_INTERVAL = 5

# Message Queues
move_queue = queue.Queue()
sound_queue = queue.Queue()
queue_lock = Lock()

def connect_to_mongodb(retry_count=5, retry_delay=5):
    global mongo_client, db
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
            return mongo_client
        except errors.PyMongoError as e:
            logger.error(f"Connection failed (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
    logger.error("Failed to connect to MongoDB")
    raise SystemExit(1)

def connect_to_mqtt():
    client = mqtt_client.Client(client_id=f"player_{PLAYER_ID}_sender", protocol=mqtt_client.MQTTv5)
    client.on_connect = on_connect
    client.on_message = on_message
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
    if reason_code == 0:
        logger.info("MQTT connected successfully")
        client.subscribe(CONFIRMED_TOPICS["move_messages"], qos=QOS)
        client.subscribe(CONFIRMED_TOPICS["sound_messages"], qos=QOS)
    else:
        logger.error(f"MQTT connection failed: {reason_code}")

def on_message(client, userdata, msg):
    topic = msg.topic
    logger.info(f"Received message on topic {topic}")
    try:
        ack_payload = json.loads(msg.payload.decode('utf-8'))
        message_id = ack_payload["_id"]
        logger.debug(f"Processing acknowledgment for message ID {message_id}")
        collection = db["move_messages"] if topic == CONFIRMED_TOPICS["move_messages"] else db["sound_messages"]
        try:
            obj_id = ObjectId(message_id)
        except errors.InvalidURI:
            logger.error(f"Invalid ObjectId: {message_id}")
            return
        doc = collection.find_one({"_id": obj_id})
        if doc:
            if doc.get("processed") == False:
                result = collection.update_one(
                    {"_id": obj_id, "processed": False},
                    {"$set": {"processed": True}}
                )
                if result.modified_count > 0:
                    logger.info(f"Marked message with _id {message_id} as processed")
                else:
                    logger.warning(f"Failed to update message {message_id}, possibly already processed")
            else:
                logger.info(f"Message {message_id} already processed")
        else:
            logger.warning(f"No message found with _id {message_id}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse ack payload: {e}, payload: {msg.payload}")
    except KeyError as e:
        logger.error(f"Missing key in ack payload: {e}, payload: {ack_payload}")
    except errors.PyMongoError as e:
        logger.error(f"MongoDB error while processing ack: {e}")

def worker_publish(mqtt_client_instance, topic, message_queue):
    while True:
        message_data = message_queue.get()
        try:
            payload = {
                k: v for k, v in message_data.items()
                if k not in ["_id", "session_id", "sent", "processed", "timestamp"]
            }
            payload["_id"] = str(message_data["_id"])
            if topic == MQTT_TOPICS["sound_messages"] and "hour" in payload:
                if isinstance(payload["hour"], datetime):
                    payload["hour"] = payload["hour"].isoformat()
            mqtt_client_instance.publish(topic, json.dumps(payload), qos=QOS)
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
        finally:
            message_queue.task_done()

def stream_mazemov():
    move_messages_col = db["move_messages"]
    while True:
        try:
            with move_messages_col.watch(
                [{"$match": {"operationType": "insert", "fullDocument.session_id": SESSION_ID}}],
                full_document='updateLookup'
            ) as stream:
                for change in stream:
                    doc = change["fullDocument"]
                    if doc.get("processed") != True:
                        move_queue.put(doc)
                        logger.debug(f"Streamed mazemov message {doc['_id']}")
        except errors.PyMongoError as e:
            logger.error(f"Mazemov stream error: {e}, falling back to polling")
            messages = move_messages_col.find({"session_id": SESSION_ID, "processed": {"$ne": True}})
            for msg in messages:
                move_queue.put(msg)
                logger.debug(f"Polled mazemov message {msg['_id']}")
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.error(f"Mazemov stream unexpected error: {e}")
            time.sleep(5)

def stream_mazesound():
    sound_messages_col = db["sound_messages"]
    while True:
        try:
            with sound_messages_col.watch(
                [{"$match": {"operationType": "insert", "fullDocument.session_id": SESSION_ID}}],
                full_document='updateLookup'
            ) as stream:
                for change in stream:
                    doc = change["fullDocument"]
                    if doc.get("processed") != True:
                        sound_queue.put(doc)
                        logger.debug(f"Streamed mazesound message {doc['_id']}")
        except errors.PyMongoError as e:
            logger.error(f"Mazesound stream error: {e}, falling back to polling")
            messages = sound_messages_col.find({"session_id": SESSION_ID, "processed": {"$ne": True}})
            for msg in messages:
                sound_queue.put(msg)
                logger.debug(f"Polled mazesound message {msg['_id']}")
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.error(f"Mazesound stream unexpected error: {e}")
            time.sleep(5)

def main():
    mongo_client = connect_to_mongodb()
    mqtt_client = connect_to_mqtt()
    db = mongo_client[MONGO_DB]

    Thread(target=worker_publish, args=(mqtt_client, MQTT_TOPICS["move_messages"], move_queue), daemon=True).start()
    Thread(target=worker_publish, args=(mqtt_client, MQTT_TOPICS["sound_messages"], sound_queue), daemon=True).start()
    Thread(target=stream_mazemov, daemon=True).start()
    Thread(target=stream_mazesound, daemon=True).start()

    try:
        while True:
            time.sleep(1)
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
