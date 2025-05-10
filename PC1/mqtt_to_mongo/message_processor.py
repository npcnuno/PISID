import os
import json
import re
import threading
import queue
import time
import signal
import sys
from dateutil import parser as dateutil_parser
from datetime import datetime, timedelta
from typing import List
from dataclasses import dataclass
from pymongo import MongoClient, errors
from pymongo.collection import Collection
from pydantic import BaseModel, create_model, ValidationError, Field
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SESSION_ID = os.getenv('SESSION_ID')
PLAYER_ID = int(os.getenv('PLAYER_ID', '33'))
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
MONGO_DB = os.getenv("MONGO_DB", "game_monitoring")
INITIAL_THREADS_PER_TOPIC = 1
MAX_THREADS_PER_TOPIC = 5
CHECK_INTERVAL = 5
SCALE_UP_THRESHOLD = 50
SCALE_DOWN_THRESHOLD = 5
POLL_INTERVAL = 1


@dataclass
class TopicConfig:
    topic_template: str
    schema: dict
    collection: str

    def __post_init__(self):
        self.topic = self.topic_template.format(player_id=PLAYER_ID)
        self.model = self.create_model()
        self.queue = queue.Queue()
        self.worker_threads: List[threading.Thread] = []
        self.lock = threading.Lock()

    def create_model(self) -> BaseModel:
        fields = {}
        for field_name, field_info in self.schema.items():
            field_type = {
                "int": int,
                "float": float,
                "str": str,
                "datetime": datetime
            }.get(field_info["type"], str)
            constraints = field_info.get("constraints", {})
            field_kwargs = {}
            if "ge" in constraints:
                field_kwargs["ge"] = constraints["ge"]
            if "le" in constraints:
                field_kwargs["le"] = constraints["le"]
            if field_info.get("required", True):
                fields[field_name] = (field_type, Field(...))
            else:
                default = field_info.get("default", None)
                if default == "now":
                    fields[field_name] = (field_type, Field(default_factory=datetime.now))
                else:
                    fields[field_name] = (field_type, default)
        return create_model(f"{self.topic}_Model", **fields)


def connect_to_mongodb(retry_count=5, retry_delay=5) -> bool:
    global mongo_client, db, raw_messages_col, failed_messages_col
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
            db = mongo_client[MONGO_DB]
            raw_messages_col = db["raw_messages"]
            failed_messages_col = db["failed_messages"]
            logger.info("Connected to MongoDB")
            return True
        except errors.PyMongoError as e:
            logger.error(f"Connection failed (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
    logger.error("Failed to connect to MongoDB")
    raise SystemExit(1)

def parse_payload(payload: str) -> dict:
    try:
        payload = ''.join(c for c in payload if c.isprintable()).strip()
        if not (payload.startswith("{") and payload.endswith("}")):
            raise ValueError("Payload must be enclosed in curly braces")
        payload = payload[1:-1].strip()
        if not payload:
            raise ValueError("Empty payload")
        parts = re.split(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", payload)
        parsed_dict = {}
        for part in parts:
            part = part.strip()
            if not part:
                continue
            match = re.match(r"\s*(\w+)\s*:\s*(?:([\"\'])(.*?)\2|([^,\s]+))\s*", part)
            if not match:
                raise ValueError(f"Invalid key-value pair: {part}")
            key = match.group(1)
            value = match.group(3) if match.group(2) else match.group(4)
            try:
                parsed_dict[key] = int(value)
            except ValueError:
                try:
                    parsed_dict[key] = float(value)
                except ValueError:
                    parsed_dict[key] = value
        return parsed_dict
    except Exception as e:
        logger.error(f"Payload parsing failed: {e}")
        raise

def stream_topic(tc: TopicConfig):
    while True:
        try:
            pipeline = [{
                "$match": {
                    "operationType": "insert",
                    "fullDocument.session_id": SESSION_ID,
                    "fullDocument.topic": tc.topic,
                    "fullDocument.processed": {"$ne": True}
                }
            }]
            with raw_messages_col.watch(pipeline, full_document='updateLookup') as stream:
                logger.info(f"Change stream connected for {tc.topic}")
                for change in stream:
                    doc = change["fullDocument"]
                    if "processed" not in doc or not doc["processed"]:
                        tc.queue.put(doc)
                        logger.debug(f"Streamed message for {tc.topic}: {doc['_id']}")
        except errors.PyMongoError as e:
            logger.error(f"Stream error for {tc.topic}: {e}, falling back to polling")
            messages = raw_messages_col.find({
                "session_id": SESSION_ID,
                "processed": {"$ne": True},
                "topic": tc.topic
            })
            for msg in messages:
                tc.queue.put(msg)
                logger.debug(f"Polled message for {tc.topic}: {msg['_id']}")
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.error(f"Unexpected error for {tc.topic}: {e}")
            time.sleep(5)

def worker_topic(tc: TopicConfig):
    collection = db[tc.collection]
    while True:
        msg = tc.queue.get()
        if msg is None:
            break
        try:
            payload = msg["payload"]
            parsed_dict = parse_payload(payload)
            if "player" in parsed_dict and parsed_dict["player"] != PLAYER_ID:
                parsed_dict["player"] = PLAYER_ID
                logger.debug(f"Corrected player ID to {PLAYER_ID} for {tc.topic}")
            try:
                validated = tc.model(**parsed_dict)

            except ValidationError as e:
                errors = []
                for error in e.errors():
                    field = error['loc'][0]
                    if field in parsed_dict and field in tc.schema:
                        field_info = tc.schema[field]
                        if field_info["type"] == "int" and parsed_dict[field] < 0:
                            parsed_dict[field] = abs(parsed_dict[field])
                            errors.append(f"Corrected negative {field} to {parsed_dict[field]}")
                        elif field_info["type"] == "float" and parsed_dict[field] < 0:
                            parsed_dict[field] = abs(parsed_dict[field])
                            errors.append(f"Corrected negative {field} to {parsed_dict[field]}")
                        elif field_info["type"] == "datetime":
                            try:
                                parsed_dt = dateutil_parser.parse(parsed_dict[field])
                                parsed_dict[field] = parsed_dt
                                errors.append(f"Parsed datetime for {field} to {parsed_dt.isoformat()}")
                            except ValueError:
                                parsed_dt = datetime.now()
                                parsed_dict[field] = parsed_dt
                                errors.append(f"Set invalid datetime for {field} to current datetime {parsed_dt.isoformat()}")
                if errors:
                    try:
                        validated = tc.model(**parsed_dict)
                        logger.debug(", ".join(errors))
                    except ValidationError as e:
                        logger.error(f"Validation still failed after corrections: {e}")
                        raise
                else:
                    raise
            doc = validated.dict()
            doc["session_id"] = SESSION_ID
            doc["timestamp"] = (datetime.now() + timedelta(minutes=60)).isoformat()

            doc["processed"] = False
            with mongo_client.start_session() as session:
                with session.start_transaction():
                    collection.insert_one(doc, session=session)
                    raw_messages_col.update_one(
                        {"_id": msg["_id"]},
                        {"$set": {"processed": True}},
                        session=session
                    )
            logger.info(f"Processed message on {tc.topic}")
        except Exception as e:
            logger.error(f"Error processing message for {tc.topic}: {e}")
            with mongo_client.start_session() as session:
                with session.start_transaction():
                    failed_messages_col.insert_one({
                        "session_id": SESSION_ID,
                        "topic": tc.topic,
                        "payload": payload,
                        "error": str(e),
                        "timestamp": (datetime.now() + timedelta(minutes=60)).isoformat(),
                        "processed": False
                    }, session=session)
        finally:
            tc.queue.task_done()

def scale_threads(tc: TopicConfig):
    while True:
        time.sleep(CHECK_INTERVAL)
        with tc.lock:
            queue_size = tc.queue.qsize()
            current_threads = len(tc.worker_threads)
            if queue_size > SCALE_UP_THRESHOLD and current_threads < MAX_THREADS_PER_TOPIC:
                new_threads = min(
                    MAX_THREADS_PER_TOPIC - current_threads,
                    (queue_size // SCALE_UP_THRESHOLD)
                )
                for _ in range(new_threads):
                    t = threading.Thread(target=worker_topic, args=(tc,), daemon=True)
                    t.start()
                    tc.worker_threads.append(t)
                    logger.info(f"Scaled up {tc.topic}: Total threads={len(tc.worker_threads)}")
            elif queue_size < SCALE_DOWN_THRESHOLD and current_threads > INITIAL_THREADS_PER_TOPIC:
                excess_threads = min(
                    current_threads - INITIAL_THREADS_PER_TOPIC,
                    (SCALE_DOWN_THRESHOLD - queue_size) // 2
                )
                for _ in range(excess_threads):
                    tc.queue.put(None)
                    tc.worker_threads.pop()
                    logger.info(f"Scaled down {tc.topic}: Total threads={len(tc.worker_threads)}")

def batch_process_historical(tc: TopicConfig):
    try:
        messages = raw_messages_col.find({
            "session_id": SESSION_ID,
            "processed": {"$ne": True},
            "topic": tc.topic
        })
        for msg in messages:
            tc.queue.put(msg)
            logger.debug(f"Enqueued historical message for {tc.topic}: {msg['_id']}")
    except Exception as e:
        logger.error(f"Error during batch processing for {tc.topic}: {e}")

def shutdown_handler(signum, frame):
    logger.info("Received shutdown signal, exiting...")
    for tc in topic_configs:
        for _ in range(len(tc.worker_threads)):
            tc.queue.put(None)
    sys.exit(0)

# Global topic configurations
topic_configs: List[TopicConfig] = []

def main():
    global topic_configs
    connect_to_mongodb()
    if not SESSION_ID or not PLAYER_ID:
        logger.error("Missing SESSION_ID or PLAYER_ID environment variables")
        sys.exit(1)
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    TOPICS_CONFIG = json.loads(os.getenv("TOPICS_CONFIG", "[]"))
    if not TOPICS_CONFIG:
        logger.error("No topics configured in TOPICS_CONFIG")
        sys.exit(1)
    topic_configs = [
        TopicConfig(
            topic_template=config["topic"],
            schema=config["schema"],
            collection=config["collection"]
        ) for config in TOPICS_CONFIG
    ]
    for tc in topic_configs:
        batch_process_historical(tc)
        stream_thread = threading.Thread(target=stream_topic, args=(tc,), daemon=True)
        stream_thread.start()
        for _ in range(INITIAL_THREADS_PER_TOPIC):
            worker_thread = threading.Thread(target=worker_topic, args=(tc,), daemon=True)
            worker_thread.start()
            tc.worker_threads.append(worker_thread)
        scaling_thread = threading.Thread(target=scale_threads, args=(tc,), daemon=True)
        scaling_thread.start()
    logger.info("Message processor started")
    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()
