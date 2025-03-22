import os
import threading
import queue
import time
from datetime import datetime
from pymongo import MongoClient, errors 
from pydantic import BaseModel, conint, confloat, ValidationError
import hashlib
import pytz
import logging
import re
from typing import Optional
import signal
import sys
# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SESSION_ID = os.getenv('SESSION_ID')
PLAYER_ID = os.getenv('PLAYER_ID')
MONGO_URI = os.getenv('MONGO_URI')
INITIAL_STREAM_THREADS = 1  # Start with 1 streaming thread per topic
INITIAL_WORKER_THREADS = 1  # Start with 1 worker thread per queue
MAX_THREADS = 5  # Max threads per topic (streaming or worker)
MIN_THREADS = 1  # Min threads per topic
SCALE_UP_THRESHOLD = 50
SCALE_DOWN_THRESHOLD = 5
CHECK_INTERVAL = 5
POLL_INTERVAL = 1  # Fallback polling interval if change streams unavailable
INITIAL_THREADS_PER_TOPIC = 1
MAX_THREADS_PER_TOPIC = 5
SCALE_UP_THRESHOLD = 50
SCALE_DOWN_THRESHOLD = 5
CHECK_INTERVAL = 5
POLL_INTERVAL = 1
# MongoDB setup
MONGO_USER = os.getenv('MONGO_USER', 'admin')
MONGO_PASS = os.getenv('MONGO_PASS', 'adminpass')
MONGO_DB = os.getenv('MONGO_DB', 'game_monitoring')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')

MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@"
    f"mongo1:27017,mongo2:27017,mongo3:27017/"
    f"{MONGO_DB}?replicaSet=my-mongo-set&"
    f"authSource={MONGO_AUTH_SOURCE}&w=1&journal=false&"
    f"retryWrites=true&connectTimeoutMS=5000&socketTimeoutMS=5000&"
    f"serverSelectionTimeoutMS=5000&readPreference=primaryPreferred"
)


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
            return True
        except errors.PyMongoError as e:
            logger.error(f"Connection failed (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
    logger.error("Failed to connect to MongoDB")
    raise SystemExit( 1)

connect_to_mongodb()
db = mongo_client["game_monitoring"]
raw_messages_col = db["raw_messages"]
move_messages_col = db["move_messages"]
sound_messages_col = db["sound_messages"]
failed_messages_col = db["failed_messages"]

# Pydantic Models
class MazemovMessage(BaseModel):
    Player: conint(ge=1)
    Marsami: int
    RoomOrigin: int
    RoomDestiny: int
    Status: conint(ge=0, le=2)

class MazesoundMessage(BaseModel):
    Player: int
    Hour: datetime
    Sound: confloat(ge=0)

# Queues and thread management
mazemov_queue = queue.Queue()
mazesound_queue = queue.Queue()
mazemov_stream_threads = []
mazesound_stream_threads = []
mazemov_worker_threads = []
mazesound_worker_threads = []
lock = threading.Lock()
latency_lock = threading.Lock()
last_latency: Optional[float] = None

# Payload Parsing Functions
def parse_key_value_pair(part: str) -> tuple:
    match = re.match(r'\s*(\w+)\s*:\s*(?:(["\'])(.*?)\2|([^,\s]+))\s*', part)
    if match:
        key = match.group(1)
        value = match.group(3) if match.group(2) else match.group(4)
        try:
            return key, int(value)
        except ValueError:
            try:
                return key, float(value)
            except ValueError:
                return key, value
    return None, None

def parse_payload(payload: str) -> dict:
    payload = payload.strip()[1:-1]
    parsed_dict = {}
    for part in payload.split(','):
        key, value = parse_key_value_pair(part)
        if key:
            parsed_dict[key] = value
    if 'Hour' in parsed_dict:
        hour_str = str(parsed_dict['Hour']).replace(',', '.')
        try:
            parsed_dict['Hour'] = datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S.%f").isoformat()
        except ValueError:
            parsed_dict['Hour'] = datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S").isoformat()
    return parsed_dict

def get_player_id_from_topic(topic: str) -> int:
    parts = topic.split('_')
    if len(parts) >= 3 and parts[0] == 'pisid' and parts[1] in ['mazemov', 'mazesound']:
        try:
            return int(parts[2])
        except ValueError:
            pass
    return 33

# Streaming Threads
def stream_mazemov():
    while True:
        try:
            with raw_messages_col.watch(
                [{"$match": {"operationType": "insert", "fullDocument.session_id": SESSION_ID}}],
                full_document='updateLookup'
            ) as stream:
                for change in stream:
                    doc = change["fullDocument"]
                    if "mazemov" in doc["topic"].lower() and doc.get("processed") != True:
                        mazemov_queue.put(doc)
                        logger.debug(f"Streamed mazemov message {doc['_id']}")
        except errors.PyMongoError as e:
            logger.error(f"Mazemov stream error: {e}, falling back to polling")
            # Fallback to polling if change streams fail
            messages = raw_messages_col.find({"session_id": SESSION_ID, "processed": {"$ne": True}, "topic": {"$regex": "mazemov", "$options": "i"}})
            for msg in messages:
                mazemov_queue.put(msg)
                logger.debug(f"Polled mazemov message {msg['_id']}")
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.error(f"Mazemov stream unexpected error: {e}")
            time.sleep(5)

def stream_mazesound():
    while True:
        try:
            with raw_messages_col.watch(
                [{"$match": {"operationType": "insert", "fullDocument.session_id": SESSION_ID}}],
                full_document='updateLookup'
            ) as stream:
                for change in stream:
                    doc = change["fullDocument"]
                    if "mazesound" in doc["topic"].lower() and doc.get("processed") != True:
                        mazesound_queue.put(doc)
                        logger.debug(f"Streamed mazesound message {doc['_id']}")
        except errors.PyMongoError as e:
            logger.error(f"Mazesound stream error: {e}, falling back to polling")
            # Fallback to polling if change streams fail
            messages = raw_messages_col.find({"session_id": SESSION_ID, "processed": {"$ne": True}, "topic": {"$regex": "mazesound", "$options": "i"}})
            for msg in messages:
                mazesound_queue.put(msg)
                logger.debug(f"Polled mazesound message {msg['_id']}")
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.error(f"Mazesound stream unexpected error: {e}")
            time.sleep(5)

# Worker Functions
def worker_mazemov():
    while True:
        msg = mazemov_queue.get()
        if msg is None:
            break
        try:
            payload = msg["payload"]
            topic = msg["topic"]
            player_id = get_player_id_from_topic(topic)
            if player_id is None:
                raise ValueError("Invalid topic, cannot extract player_id")
            message_hash = hashlib.sha256(payload.encode()).hexdigest()
            payload_dict = parse_payload(payload)
            
            if payload_dict.get("Player") != player_id:
                raise ValueError(f"Player in payload ({payload_dict.get('Player')}) does not match session player ({player_id})")
            
            validated = MazemovMessage(**payload_dict)
            doc = {
                "session_id": SESSION_ID,
                "player": validated.Player,
                "marsami": validated.Marsami,
                "room_origin": validated.RoomOrigin,
                "room_destiny": validated.RoomDestiny,
                "status": validated.Status,
                "timestamp": datetime.now(),
                "message_hash": message_hash,
                "processed": False
            }
            move_messages_col.insert_one(doc)
            raw_messages_col.update_one({"_id": msg["_id"]}, {"$set": {"processed": True}})
        except Exception as e:
            logger.error(f"Error in worker_mazemov: {e}")
            failed_messages_col.insert_one({
                "session_id": SESSION_ID,
                "topic": topic,
                "payload": payload,
                "error": str(e),
                "timestamp": datetime.now(),
                "processed": True
            })
        finally:
            mazemov_queue.task_done()

def worker_mazesound():
    global last_latency
    while True:
        msg = mazesound_queue.get()
        if msg is None:
            break
        logger.info(f"Processing mazemov message {msg['_id']}")
        try:
            payload = msg["payload"]
            topic = msg["topic"]
            player_id = get_player_id_from_topic(topic)
            if player_id is None:
                raise ValueError("Invalid topic, cannot extract player_id")
            message_hash = hashlib.sha256(payload.encode()).hexdigest()
            payload_dict = parse_payload(payload)
            
            if payload_dict.get("Player") != player_id:
                raise ValueError(f"Player in payload ({payload_dict.get('Player')}) does not match session player ({player_id})")
            
            try:
                validated = MazesoundMessage(**payload_dict)
                timestamp1 = msg["timestamp"].replace(tzinfo=pytz.utc)  # Assuming timestamp is a datetime object from PyMongo
                send_time = validated.Hour.replace(tzinfo=pytz.utc)
                process_time = datetime.now(pytz.utc)
                latency = (process_time - send_time).total_seconds() * 1000
                with latency_lock:
                    last_latency = latency
            except ValidationError as e:
                errors = e.errors()
                modified = False
                if any(error['loc'][0] == 'Player' for error in errors):
                    payload_dict['Player'] = player_id
                    modified = True
                if any(error['loc'][0] == 'Hour' for error in errors):
                    with latency_lock:
                        # if last_latency is not None:
                        #     estimated_send_time = datetime.now(pytz.utc) -
                        #     payload_dict['Hour'] = estimated_send_time.isoformat()
                        # else:
                        #     payload_dict['Hour'] = datetime.now(pytz.utc).isoformat()
                        modified = True
                if modified:
                    validated = MazesoundMessage(**payload_dict)
                    send_time = datetime.fromisoformat(payload_dict['Hour']).replace(tzinfo=pytz.utc)
                    process_time = datetime.now(pytz.utc)
                    latency = (process_time - send_time).total_seconds() * 1000
                    with latency_lock:
                        last_latency = latency
                else:
                    raise
            
            doc = {
                "session_id": SESSION_ID,
                "player": validated.Player,
                "sound": validated.Sound,
                "hour": validated.Hour,
                "message_hash": message_hash,
                "processed": False,
                "latency_ms": latency
            }
            sound_messages_col.insert_one(doc)
            raw_messages_col.update_one({"_id": msg["_id"]}, {"$set": {"processed": True}})

            logger.info(f"CURRENT PROCESSING TIME: {(datetime.now(pytz.utc) - timestamp1).total_seconds()* 1000} ms ")
        except Exception as e:
            logger.error(f"Error in worker_mazesound: {e}")
            failed_messages_col.insert_one({
                "session_id": SESSION_ID,
                "topic": topic,
                "payload": payload,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "processed": True
            })
        finally:
            mazesound_queue.task_done()

# Queues and thread lists
mazemov_queue = queue.Queue()
mazesound_queue = queue.Queue()
mazemov_threads = []
mazesound_threads = []
lock = threading.Lock()

def process_mazemov_forever():
    """Continuously stream mazemov messages from raw_messages and enqueue them."""
    while True:
        try:
            pipeline = [{
                "$match": {
                    "operationType": "insert",
                    "fullDocument.session_id": SESSION_ID,
                    "fullDocument.topic": {"$regex": "^pisid_mazemov_", "$options": "i"},
                    "fullDocument.processed": {"$ne": True}
                }
            }]
            
            with raw_messages_col.watch(pipeline, full_document='updateLookup') as stream:
                logger.info("Mazemov change stream connected")
                for change in stream:
                    doc = change["fullDocument"]
                    if "processed" not in doc or not doc["processed"]:
                        mazemov_queue.put(doc)
                        logger.debug(f"Mazemov stream: Dispatched {doc['_id']}")
        except errors.PyMongoError as e:
            logger.error(f"Mazemov stream error: {e}")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Mazemov unexpected error: {e}")
            time.sleep(5)

def process_mazesound_forever():
    """Continuously stream mazesound messages from raw_messages and enqueue them."""
    while True:
        try:
            pipeline = [{
                "$match": {
                    "operationType": "insert",
                    "fullDocument.session_id": SESSION_ID,
                    "fullDocument.topic": {"$regex": "^pisid_mazesound_", "$options": "i"},
                    "fullDocument.processed": {"$ne": True}
                }
            }]

            with raw_messages_col.watch(pipeline, full_document='updateLookup') as stream:
                logger.info("Mazesound change stream connected")
                for change in stream:
                    doc = change["fullDocument"]
                    if "processed" not in doc or not doc["processed"]:
                        mazesound_queue.put(doc)
                        logger.debug(f"Mazesound stream: Dispatched {doc['_id']}")
        except errors.PyMongoError as e:
            logger.error(f"Mazesound stream error: {e}")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Mazesound unexpected error: {e}")
            time.sleep(5)

# [worker_mazemov and worker_mazesound unchanged from your original code]

def batch_process_historical_messages():
    """Process existing unprocessed messages on startup."""
    logger.info("Starting batch processing of historical messages")
    
    try:
        # Process mazemov messages
        mazemov_messages = raw_messages_col.find({
            "session_id": SESSION_ID,
            "processed": {"$ne": True},
            "topic": {"$regex": "mazemov", "$options": "i"}
        })
        for msg in mazemov_messages:
            mazemov_queue.put(msg)
            logger.debug(f"Enqueued historical mazemov message {msg['_id']}")

        # Process mazesound messages
        mazesound_messages = raw_messages_col.find({
            "session_id": SESSION_ID,
            "processed": {"$ne": True},
            "topic": {"$regex": "mazesound", "$options": "i"}
        })
        for msg in mazesound_messages:
            mazesound_queue.put(msg)
            logger.debug(f"Enqueued historical mazesound message {msg['_id']}")

    except Exception as e:
        logger.error(f"Error during batch processing: {e}")


def scale_threads(topic, queue, thread_list, worker_func):
    """Scale worker threads for a topic based on queue size."""
    while True:
        time.sleep(CHECK_INTERVAL)
        with lock:
            queue_size = queue.qsize()
            current_threads = len(thread_list)
            if queue_size > SCALE_UP_THRESHOLD and current_threads < MAX_THREADS_PER_TOPIC:
                new_threads = min(MAX_THREADS_PER_TOPIC - current_threads, (queue_size // SCALE_UP_THRESHOLD))
                for _ in range(new_threads):
                    t = threading.Thread(target=worker_func, daemon=True)
                    t.start()
                    thread_list.append(t)
                    logger.info(f"Scaled up {topic}: Total threads={len(thread_list)}")
            elif queue_size < SCALE_DOWN_THRESHOLD and current_threads > INITIAL_THREADS_PER_TOPIC:
                excess_threads = min(current_threads - INITIAL_THREADS_PER_TOPIC, (SCALE_DOWN_THRESHOLD - queue_size) // 2)
                for _ in range(excess_threads):
                    queue.put(None)
                    thread_list.pop()
                    logger.info(f"Scaled down {topic}: Total threads={len(thread_list)}")

def shutdown_handler(signum, frame):
    logger.info("Received shutdown signal, exiting...")
    sys.exit(0)

def main():
    if not SESSION_ID or not PLAYER_ID:
        logger.error("Missing SESSION_ID or PLAYER_ID environment variables")
        exit(1)

    # Signal handlers for subprocess termination
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    batch_process_historical_messages()
    # Start worker threads
    for _ in range(INITIAL_THREADS_PER_TOPIC):
        t1 = threading.Thread(target=worker_mazemov, daemon=True)
        t2 = threading.Thread(target=worker_mazesound, daemon=True)
        t1.start()
        t2.start()
        mazemov_threads.append(t1)
        mazesound_threads.append(t2)

    # Start persistent processing threads
    mazemov_processor = threading.Thread(target=process_mazemov_forever, daemon=True)
    mazesound_processor = threading.Thread(target=process_mazesound_forever, daemon=True)
    mazemov_processor.start()
    mazesound_processor.start()

    # Start scalers
    threading.Thread(target=scale_threads, args=("mazemov", mazemov_queue, mazemov_threads, worker_mazemov), daemon=True).start()
    threading.Thread(target=scale_threads, args=("mazesound", mazesound_queue, mazesound_threads, worker_mazesound), daemon=True).start()

    logger.info("Started persistent subprocess for raw_messages processing")

    # Keep main thread alive
    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()
