import os
import json
import threading
import queue
import time
from datetime import datetime
from paho.mqtt import client as mqtt_client
from pymongo import MongoClient, errors
from bson.objectid import ObjectId
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment variables
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://admin:adminpass@mongo1:27017,mongo2:27017,mongo3:27017/game_monitoring?replicaSet=my-mongo-set&authSource=admin')
MONGO_DB = os.getenv('MONGO_DB', 'game_monitoring')
MQTT_BROKER = os.getenv('MQTT_BROKER', 'test.mosquitto.org')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
PLAYER_ID = int(os.getenv('PLAYER_ID', '33'))
SESSION_ID = os.getenv('SESSION_ID', 'default_session')
TOPICS_CONFIG = json.loads(os.getenv('TOPICS_CONFIG', '[]'))
QOS = int(os.getenv('QOS', '2'))
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '5'))
BATCH_DELAY_TIME = float(os.getenv('BATCH_DELAY_TIME', '0.250'))  
RETRY_BATCH_SIZE = int(os.getenv('RETRY_BATCH_SIZE', '5'))
DELAY_TIME_BETWEEN_SENDS = float(os.getenv("DELAY_TIME_BETWEEN_SENDS", "0.005"))

# Global variables
pending_acks = {}
retry_counts = {}  
ack_lock = threading.Lock()
collection_queues = {}
threads = []
mqtt_topics_configs = []


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

def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def connect_to_mqtt(userdata):
    client = mqtt_client.Client(client_id=f"player_{PLAYER_ID}_sender", userdata=userdata)
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

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        for confirmed_topic in userdata['confirmed_topic_to_collection'].keys():
            client.subscribe(confirmed_topic, qos=QOS)
            logger.info(f"Subscribed to {confirmed_topic}")
    else:
        logger.error(f"Failed to connect to MQTT: {rc}")

def on_message(client, userdata, msg):
    topic = msg.topic
    try:
        ack_payload = json.loads(msg.payload.decode('utf-8'))
        message_id = ack_payload["_id"]
        collection_name = userdata['confirmed_topic_to_collection'].get(topic)
        if not collection_name:
            logger.error(f"No collection mapped for topic {topic}")
            return
        collection = db[collection_name]
        try:
            obj_id = ObjectId(message_id)
        except errors.InvalidURI:
            logger.error(f"Invalid ObjectId: {message_id}")
            return
        result = collection.update_one(
            {"_id": obj_id, "processed": {"$ne": True}},
            {"$set": {"processed": True}}
        )
        if result.modified_count > 0:
            logger.info(f"Marked message {message_id} as processed in {collection_name}")
            with ack_lock:
                if message_id in pending_acks:
                    del pending_acks[message_id]
                    logger.info(f"Removed message {message_id} from pending_acks after acknowledgment")
                if message_id in retry_counts:
                    del retry_counts[message_id]
                    logger.info(f"Removed message {message_id} from retry_counts after acknowledgment")
        else:
            logger.warning(f"Message {message_id} not found or already processed in {collection_name}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse ACK payload: {e}, payload: {msg.payload}")
    except Exception as e:
        logger.error(f"Error processing ACK for topic {topic}: {e}")

def worker_publish(mqtt_client_instance, topic, message_queue, collection_name):
    while True:
        try:
            _, message_data = message_queue.get()  # Get message from PriorityQueue, ignore priority
            try:
                payload = {k: v for k, v in message_data.items() if k not in ["_id", "session_id", "processed"]}
                payload["_id"] = str(message_data["_id"])
                serialized_payload = json.dumps(payload, default=json_serial)
                if not mqtt_client_instance.is_connected():
                    logger.error(f"MQTT client not connected for message {message_data['_id']}")
                    raise ConnectionError("MQTT client disconnected")
                logger.info(f"Publishing message {message_data['_id']} to topic {topic}")
                result = mqtt_client_instance.publish(topic, serialized_payload, qos=2)
                if result.rc == 0:
                    logger.info(f"Successfully published message {message_data['_id']} (mid={result.mid})")
                    with ack_lock:
                        pending_acks[str(message_data["_id"])] = {
                            'topic': topic,
                            'message': message_data,
                            'send_time': datetime.now(),
                            'retry_count': 0,
                            'collection_name': collection_name
                        }
                else:
                    logger.error(f"Publish failed for message {message_data['_id']}: Return code {result.rc}")
                time.sleep(DELAY_TIME_BETWEEN_SENDS) 
            except Exception as e:
                logger.error(f"Error processing message {message_data['_id']}: {str(e)}")
            finally:
                message_queue.task_done()
        except Exception as e:
            logger.error(f"Error retrieving message from queue: {e}")

def stream_collection(collection_name, message_queue):
    collection = db[collection_name]
    try:
        messages = collection.find({"session_id": SESSION_ID, "processed": {"$ne": True}})
        for msg in messages:
            message_queue.put((0, msg))  
            logger.debug(f"Queued unprocessed message {msg['_id']} from {collection_name}")
    except errors.PyMongoError as e:
        logger.error(f"Error querying unprocessed messages in {collection_name}: {e}")
    while True:
        try:
            with collection.watch(
                [{"$match": {"operationType": "insert", "fullDocument.session_id": SESSION_ID}}],
                full_document='updateLookup'
            ) as stream:
                for change in stream:
                    doc = change["fullDocument"]
                    if doc.get("processed") != True:
                        message_queue.put((0, doc))  # Priority 0 for new messages
                        logger.debug(f"Streamed message {doc['_id']} from {collection_name}")
        except errors.PyMongoError as e:
            logger.error(f"Change stream error in {collection_name}: {e}, falling back to polling")
            try:
                messages = collection.find({"session_id": SESSION_ID, "processed": {"$ne": True}})
                for msg in messages:
                    message_queue.put((0, msg))  # Priority 0 for new messages
                    logger.debug(f"Polled message {msg['_id']} from {collection_name}")
            except errors.PyMongoError as e:
                logger.error(f"Polling error in {collection_name}: {e}")
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.error(f"Unexpected error in {collection_name} stream: {e}")
            time.sleep(POLL_INTERVAL)

def retry_worker():
    while True:
        time.sleep(BATCH_DELAY_TIME)
        current_time = datetime.now()
        with ack_lock:
            messages_to_retry = []
            messages_to_remove = []
            for message_id, data in pending_acks.items():
                if message_id not in retry_counts:
                    retry_counts[message_id] = 0
                elapsed = (current_time - data['send_time']).total_seconds()
                if elapsed > BATCH_DELAY_TIME:
                    messages_to_retry.append((message_id, data))
                if len(messages_to_retry) >= RETRY_BATCH_SIZE:
                    break
            for message_id, data in messages_to_retry:
                retry_counts[message_id] += 1
                message_queue = collection_queues.get(data['collection_name'])
                if message_queue:
                    message_queue.put((1, data['message']))  # Priority 1 for retries
                    data['send_time'] = current_time
                    logger.info(f"Retrying message {message_id} (attempt {retry_counts[message_id]})")
            for message_id in messages_to_remove:
                del pending_acks[message_id]
                if message_id in retry_counts:
                    del retry_counts[message_id]
                logger.error(f"Message {message_id} exceeded maximum retries and was removed")

def main():
    if not TOPICS_CONFIG:
        logger.error("TOPICS_CONFIG is empty. Exiting.")
        raise SystemExit(1)
    
    for config in TOPICS_CONFIG:
        mqtt_topics_configs.append({
            'collection': config['collection'],
            'processed_topic': config['processed_topic'].format(player_id=PLAYER_ID),
            'confirmed_topic': config['confirmed_topic'].format(player_id=PLAYER_ID)
        })
    confirmed_topic_to_collection = {
        config['confirmed_topic']: config['collection'] for config in mqtt_topics_configs
    }
    
    connect_to_mongodb()
    mqtt_client = connect_to_mqtt(userdata={'confirmed_topic_to_collection': confirmed_topic_to_collection})

    for config in mqtt_topics_configs:
        message_queue = queue.PriorityQueue()  # Use PriorityQueue
        collection_queues[config['collection']] = message_queue
        threads.append(threading.Thread(
            target=worker_publish,
            args=(mqtt_client, config['processed_topic'], message_queue, config['collection']),
            daemon=True
        ))
        threads.append(threading.Thread(
            target=stream_collection,
            args=(config['collection'], message_queue),
            daemon=True
        ))
    retry_thread = threading.Thread(target=retry_worker, daemon=True)
    retry_thread.start()

    for thread in threads:
        thread.start()
    try:
        mqtt_client.loop_start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        if mongo_client:
            mongo_client.close()
        logger.info("Connections closed")

if __name__ == "__main__":
    main()
