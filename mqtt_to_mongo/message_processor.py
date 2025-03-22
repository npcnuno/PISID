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
INITIAL_STREAM_THREADS = 1
INITIAL_WORKER_THREADS = 1
MAX_THREADS = 5
MIN_THREADS = 1
SCALE_UP_THRESHOLD = 50
SCALE_DOWN_THRESHOLD = 5
CHECK_INTERVAL = 5
POLL_INTERVAL = 1
INITIAL_THREADS_PER_TOPIC = 1
MAX_THREADS_PER_TOPIC = 5
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

# Queues and thread management
mazemov_queue = queue.Queue()
mazesound_queue = queue.Queue()
mazemov_worker_threads = []
mazesound_worker_threads = []
lock = threading.Lock()
latency_lock = threading.Lock()
last_latency: Optional[float] = None
mazemov_threads = []
mazesound_threads = []
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

def connect_to_mongodb(retry_count=5, retry_delay=5):
    """Purpose: Establishes a connection to MongoDB and sets up collection variables.
    Execution Flow:
    1. Declare global variables for mongo_client and collections to be set upon connection.
    2. Loop through retry_count attempts (default 5) to connect to MongoDB.
    3. Create a MongoClient with MONGO_URI and settings (e.g., maxPoolSize=20, retryWrites=True).
    4. Send a 'ping' command to verify connectivity.
    5. On success, set global variables for client and collections, log success, and return True.
    6. On failure (PyMongoError), log the error with attempt details.
    7. If not the last attempt, wait retry_delay seconds (default 5) before retrying.
    8. If all attempts fail, log a final error and raise SystemExit."""
    global mongo_client, raw_messages_col, move_messages_col, sound_messages_col, failed_messages_col
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
            move_messages_col = db["move_messages"]
            sound_messages_col = db["sound_messages"]
            failed_messages_col = db["failed_messages"]
            return True
        except errors.PyMongoError as e:
            logger.error(f"Connection failed (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
    logger.error("Failed to connect to MongoDB")
    raise SystemExit(1)

def parse_key_value_pair(part: str) -> tuple:
    """Purpose: Parses a single key-value pair from a payload string.
    Execution Flow:
    1. Use regex to match a key followed by a value (quoted or unquoted).
    2. If matched, extract the key and value (handling quotes if present).
    3. Attempt to convert the value to an integer; if successful, return (key, int_value).
    4. If integer conversion fails, try converting to float; if successful, return (key, float_value).
    5. If both fail, return (key, string_value).
    6. If no match, return (None, None)."""
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
    """Purpose: Parses a raw payload string into a dictionary.
    Execution Flow:
    1. Remove non-printable characters from the payload.
    2. Strip whitespace and remove outer brackets (assumes {key: value, ...} format).
    3. Split the payload by commas to get individual key-value parts.
    4. Initialize an empty dictionary.
    5. For each part, call parse_key_value_pair to get key and value.
    6. If key is valid, add to the dictionary.
    7. If 'Hour' is present, parse it as a datetime (with or without microseconds) and convert to ISO format.
    8. Return the parsed dictionary."""
    payload = ''.join(c for c in payload if c.isprintable())
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
    """Purpose: Extracts the player ID from an MQTT topic string.
    Execution Flow:
    1. Split the topic by '_' (e.g., 'pisid_mazemov_33').
    2. Check if there are at least 3 parts, first is 'pisid', and second is 'mazemov' or 'mazesound'.
    3. Try to convert the third part to an integer (player_id).
    4. On success, return the player_id.
    5. On failure or invalid format, return default 33."""
    parts = topic.split('_')
    if len(parts) >= 3 and parts[0] == 'pisid' and parts[1] in ['mazemov', 'mazesound']:
        try:
            return int(parts[2])
        except ValueError:
            pass
    return 33

def stream_mazemov():
    """Purpose: Streams new mazemov messages from raw_messages using MongoDB change streams.
    Execution Flow:
    1. Enter an infinite loop to continuously watch for changes.
    2. Use raw_messages_col.watch with a filter for inserts matching SESSION_ID and unprocessed status.
    3. For each change, get the full document and check if it’s a mazemov message and unprocessed.
    4. If valid, queue it in mazemov_queue and log at debug level.
    5. On PyMongoError, log the error, fall back to polling unprocessed mazemov messages, and sleep POLL_INTERVAL.
    6. On other exceptions, log and sleep 5 seconds before retrying."""
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
            messages = raw_messages_col.find({"session_id": SESSION_ID, "processed": {"$ne": True}, "topic": {"$regex": "mazemov", "$options": "i"}})
            for msg in messages:
                mazemov_queue.put(msg)
                logger.debug(f"Polled mazemov message {msg['_id']}")
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.error(f"Mazemov stream unexpected error: {e}")
            time.sleep(5)

def stream_mazesound():
    """Purpose: Streams new mazesound messages from raw_messages using MongoDB change streams.
    Execution Flow:
    1. Enter an infinite loop to watch for changes.
    2. Use raw_messages_col.watch with a filter for inserts matching SESSION_ID and unprocessed status.
    3. For each change, get the full document and check if it’s a mazesound message and unprocessed.
    4. If valid, queue it in mazesound_queue and log at debug level.
    5. On PyMongoError, log the error, fall back to polling unprocessed mazesound messages, and sleep POLL_INTERVAL.
    6. On other exceptions, log and sleep 5 seconds before retrying."""
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
            messages = raw_messages_col.find({"session_id": SESSION_ID, "processed": {"$ne": True}, "topic": {"$regex": "mazesound", "$options": "i"}})
            for msg in messages:
                mazesound_queue.put(msg)
                logger.debug(f"Polled mazesound message {msg['_id']}")
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.error(f"Mazesound stream unexpected error: {e}")
            time.sleep(5)

def worker_mazemov():
    """Purpose: Processes mazemov messages from the queue, validates them, and stores in move_messages.
    Execution Flow:
    1. Enter an infinite loop to process messages from mazemov_queue.
    2. Get a message (blocks until available); break if None (shutdown signal).
    3. Extract payload and topic, compute a SHA256 hash of the payload.
    4. Parse payload into a dictionary and get player_id from topic.
    5. Validate player_id consistency with payload; raise ValueError if mismatched.
    6. Validate payload with MazemovMessage model.
    7. Create a document with validated data, session_id, timestamp, and hash.
    8. In a transaction, insert into move_messages_col and mark raw message as processed.
    9. On error, log and insert into failed_messages_col.
    10. Mark task as done."""
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
            with mongo_client.start_session() as session:
                with session.start_transaction():
                    move_messages_col.insert_one(doc, session=session)
                    raw_messages_col.update_one({"_id": msg["_id"]}, {"$set": {"processed": True}}, session=session)
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
    """Purpose: Processes mazesound messages, validates them, calculates latency, and stores in sound_messages.
    Execution Flow:
    1. Enter an infinite loop to process messages from mazesound_queue.
    2. Get a message (blocks until available); break if None (shutdown signal).
    3. Log the start of processing for this message.
    4. Extract payload and topic, compute a SHA256 hash.
    5. Get player_id from topic; raise ValueError if None.
    6. Parse payload and validate player_id consistency.
    7. Try to validate with MazesoundMessage; on ValidationError, adjust Player or Hour if possible.
    8. Calculate latency from send_time to process_time.
    9. Create a document with validated data, latency, and hash.
    10. Insert into sound_messages_col and mark raw message as processed (non-transactional).
    11. Log processing time.
    12. On error, log and insert into failed_messages_col.
    13. Mark task as done."""
    global last_latency
    while True:
        msg = mazesound_queue.get()
        if msg is None:
            break
        logger.info(f"Processing mazesound message {msg['_id']}")
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
                timestamp1 = msg["timestamp"].replace(tzinfo=pytz.utc)
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
                    modified = True  # Note: Original code has commented logic; assuming intent to use current time
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
            with mongo_client.start_session() as session:
                with session.start_transaction():
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

def process_mazemov_forever():
    """Purpose: Continuously streams mazemov messages using a specific pipeline and enqueues them.
    Execution Flow:
    1. Enter an infinite loop to watch for changes.
    2. Define a pipeline to match inserts for SESSION_ID, mazemov topics, and unprocessed status.
    3. Use raw_messages_col.watch with the pipeline; log connection success.
    4. For each change, enqueue the document if unprocessed and log at debug level.
    5. On PyMongoError or other exceptions, log the error and sleep 5 seconds before retrying."""
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
    """Purpose: Continuously streams mazesound messages using a specific pipeline and enqueues them.
    Execution Flow:
    1. Enter an infinite loop to watch for changes.
    2. Define a pipeline to match inserts for SESSION_ID, mazesound topics, and unprocessed status.
    3. Use raw_messages_col.watch with the pipeline; log connection success.
    4. For each change, enqueue the document if unprocessed and log at debug level.
    5. On PyMongoError or other exceptions, log the error and sleep 5 seconds before retrying."""
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

def batch_process_historical_messages():
    """Purpose: Processes existing unprocessed messages on startup to clear backlog.
    Execution Flow:
    1. Log the start of batch processing.
    2. Query raw_messages for unprocessed mazemov messages matching SESSION_ID.
    3. Enqueue each message in mazemov_queue and log at debug level.
    4. Query raw_messages for unprocessed mazesound messages matching SESSION_ID.
    5. Enqueue each message in mazesound_queue and log at debug level.
    6. On error, log the exception."""
    logger.info("Starting batch processing of historical messages")
    try:
        mazemov_messages = raw_messages_col.find({
            "session_id": SESSION_ID,
            "processed": {"$ne": True},
            "topic": {"$regex": "mazemov", "$options": "i"}
        })
        for msg in mazemov_messages:
            mazemov_queue.put(msg)
            logger.debug(f"Enqueued historical mazemov message {msg['_id']}")
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
    """Purpose: Dynamically scales worker threads based on queue size for performance optimization.
    Execution Flow:
    1. Enter an infinite loop to check scaling needs every CHECK_INTERVAL seconds.
    2. Acquire a lock to safely access queue and thread_list.
    3. Get the current queue size and number of threads.
    4. If queue size exceeds SCALE_UP_THRESHOLD and threads are below MAX_THREADS_PER_TOPIC, add new threads.
    5. Calculate new threads needed, cap at remaining capacity, start them, and log the scale-up.
    6. If queue size is below SCALE_DOWN_THRESHOLD and threads exceed INITIAL_THREADS_PER_TOPIC, remove excess threads.
    7. Calculate excess threads, queue None to terminate them, remove from list, and log the scale-down."""
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
    """Purpose: Handles shutdown signals for graceful exit.
    Execution Flow:
    1. Log the receipt of a shutdown signal.
    2. Exit the program with status code 0."""
    logger.info("Received shutdown signal, exiting...")
    sys.exit(0)

def main():
    """Purpose: Sets up and runs the message processing system with streaming and worker threads.
    Execution Flow:
    1. Connect to MongoDB to initialize collections.
    2. Check if SESSION_ID and PLAYER_ID are set; exit with error if missing.
    3. Set signal handlers for SIGTERM and SIGINT to shutdown_handler.
    4. Process historical unprocessed messages with batch_process_historical_messages.
    5. Start INITIAL_THREADS_PER_TOPIC worker threads for mazemov and mazesound.
    6. Start persistent streaming threads for mazemov and mazesound.
    7. Start scaling threads for both topics.
    8. Log startup completion and enter an infinite loop to keep the main thread alive."""
    connect_to_mongodb()
    if not SESSION_ID or not PLAYER_ID:
        logger.error("Missing SESSION_ID or PLAYER_ID environment variables")
        exit(1)
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    batch_process_historical_messages()
    for _ in range(INITIAL_THREADS_PER_TOPIC):
        t1 = threading.Thread(target=worker_mazemov, daemon=True)
        t2 = threading.Thread(target=worker_mazesound, daemon=True)
        t1.start()
        t2.start()
        mazemov_threads.append(t1)
        mazesound_threads.append(t2)
    mazemov_processor = threading.Thread(target=process_mazemov_forever, daemon=True)
    mazesound_processor = threading.Thread(target=process_mazesound_forever, daemon=True)
    mazemov_processor.start()
    mazesound_processor.start()
    threading.Thread(target=scale_threads, args=("mazemov", mazemov_queue, mazemov_threads, worker_mazemov), daemon=True).start()
    threading.Thread(target=scale_threads, args=("mazesound", mazesound_queue, mazesound_threads, worker_mazesound), daemon=True).start()
    logger.info("Started persistent subprocess for raw_messages processing")
    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()
