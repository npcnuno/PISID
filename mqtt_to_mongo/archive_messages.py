import os
from pymongo import MongoClient, errors, ReadPreference
from bson.objectid import ObjectId
import logging
from datetime import datetime
import time
from typing import List, Dict, Optional
import threading

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
CHECK_INTERVAL = 5  # Seconds to check process status
BATCH_SIZE = 100  # Number of documents to process per batch
POLL_INTERVAL = 10  # Seconds between batch checks (low priority)

mongo_client = None
db = None

def connect_to_mongodb(retry_count=5, retry_delay=5):
    global mongo_client, db
    for attempt in range(retry_count):
        try:
            mongo_client = MongoClient(
                MONGO_URI,
                maxPoolSize=1, minPoolSize=1,
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

def get_player_id_from_topic(topic: str) -> Optional[int]:
    parts = topic.split('_')
    if len(parts) >= 3 and parts[0] == 'pisid' and parts[1] in ['mazemov', 'mazesound']:
        try:
            return int(parts[2])
        except ValueError:
            pass
    return None

def get_player_id_from_doc(doc: Dict) -> Optional[int]:
    if "player" in doc:
        try:
            return int(doc["player"])
        except (ValueError, TypeError):
            pass
    if "topic" in doc:
        return get_player_id_from_topic(doc["topic"])
    return None

def get_session_objectid(session_id: str, game_sessions_col, session) -> Optional[ObjectId]:
    try:
        session_objectid = ObjectId(session_id)
        session_doc = game_sessions_col.find_one({"_id": session_objectid}, session=session)
        if session_doc:
            return session_doc["_id"]
        logger.warning(f"No session found for session_id {session_id}")
        return None
    except errors.InvalidURI:
        logger.error(f"Invalid session_id format: {session_id}, must be a valid ObjectId")
        return None

def process_batch(collection_name: str, batch: List[Dict], game_sessions_col, session) -> Dict[str, List]:
    session_to_docs = {}
    session_to_ids = {}
    skipped_docs = 0

    for doc in batch:
        try:
            if doc.get("processed") != True:
                logger.debug(f"Skipping unprocessed document {doc.get('_id')} from {collection_name}")
                skipped_docs += 1
                continue

            session_id = doc.get("session_id")
            if not session_id:
                logger.warning(f"Missing session_id in document {doc.get('_id')} from {collection_name}")
                skipped_docs += 1
                continue

            player_id = get_player_id_from_doc(doc)
            if player_id is None:
                logger.warning(f"Cannot determine player_id from document {doc.get('_id')} in {collection_name}")
                skipped_docs += 1
                continue

            session_objectid = get_session_objectid(session_id, game_sessions_col, session)
            if not session_objectid:
                logger.warning(f"Skipping document {doc.get('_id')} due to no matching session {session_id}")
                skipped_docs += 1
                continue

            session_doc = game_sessions_col.find_one({"_id": session_objectid}, session=session)
            if session_doc:
                session_player_id = session_doc.get("player_id")
                if session_player_id is not None and session_player_id != player_id:
                    logger.warning(f"Player ID mismatch for {doc.get('_id')}: session {session_id} has {session_player_id}, message has {player_id}")
                    skipped_docs += 1
                    continue

            archived_doc = doc.copy()
            archived_doc["archived_at"] = datetime.now().isoformat()

            if session_id not in session_to_docs:
                session_to_docs[session_id] = []
                session_to_ids[session_id] = []
            session_to_docs[session_id].append(archived_doc)
            session_to_ids[session_id].append(doc["_id"])

        except Exception as e:
            logger.error(f"Failed to process document {doc.get('_id', 'unknown')} from {collection_name}: {e}")
            skipped_docs += 1

    logger.info(f"Processed {len(batch) - skipped_docs} documents, skipped {skipped_docs} from {collection_name}")

    for session_id, docs in session_to_docs.items():
        try:
            session_objectid = get_session_objectid(session_id, game_sessions_col, session)
            if not session_objectid:
                logger.error(f"Session {session_id} not found during update, skipping")
                del session_to_ids[session_id]
                continue

            # Ensure the field exists as an array if it doesn't already
            game_sessions_col.update_one(
                {"_id": session_objectid},
                {"$setOnInsert": {collection_name: []}},
                upsert=True,
                session=session
            )

            result = game_sessions_col.update_one(
                {"_id": session_objectid},
                {"$push": {collection_name: {"$each": docs}}},
                session=session
            )
            if result.modified_count > 0:
                logger.info(f"Archived {len(docs)} documents from {collection_name} to session {session_id} (ObjectId: {session_objectid})")
            else:
                logger.warning(f"No documents archived from {collection_name} to session {session_id}, possibly no update needed")
                del session_to_ids[session_id]
        except errors.PyMongoError as e:
            logger.error(f"Error archiving to session {session_id} for {collection_name}: {e}")
            del session_to_ids[session_id]

    return session_to_ids

def archive_collection(collection_name: str, source_col, game_sessions_col):
    while True:
        try:
            with mongo_client.start_session() as session:
                session.start_transaction(read_preference=ReadPreference.PRIMARY)
                try:
                    messages = list(source_col.find({"processed": True}, limit=BATCH_SIZE, session=session))
                    if not messages:
                        session.commit_transaction()
                        time.sleep(POLL_INTERVAL)
                        continue

                    logger.info(f"Processing batch of {len(messages)} processed documents from {collection_name}")

                    session_to_ids = process_batch(collection_name, messages, game_sessions_col, session)

                    for session_id, ids_to_delete in session_to_ids.items():
                        if ids_to_delete:
                            result = source_col.delete_many({"_id": {"$in": ids_to_delete}}, session=session)
                            logger.info(f"Deleted {result.deleted_count} processed documents from {collection_name} for session {session_id}")
                        else:
                            logger.debug(f"No documents to delete for session {session_id} in {collection_name}")

                    session.commit_transaction()
                except Exception as e:
                    logger.error(f"Transaction failed for {collection_name}: {e}")
                    session.abort_transaction()
                    time.sleep(10)
        except errors.PyMongoError as e:
            logger.error(f"MongoDB error archiving {collection_name}: {e}")
            time.sleep(10)
        except Exception as e:
            logger.error(f"Unexpected error archiving {collection_name}: {e}")
            time.sleep(10)

def batch_archive_all_collections():
    connect_to_mongodb()
    
    raw_messages_col = db["raw_messages"]
    move_messages_col = db["move_messages"]
    sound_messages_col = db["sound_messages"]
    failed_messages_col = db["failed_messages"]
    game_sessions_col = db["game_sessions"]
    collections = {
        "raw_messages": raw_messages_col,
        "move_messages": move_messages_col,
        "sound_messages": sound_messages_col,
        "failed_messages": failed_messages_col
    }

    threads = []
    for collection_name, collection in collections.items():
        t = threading.Thread(target=archive_collection, args=(collection_name, collection, game_sessions_col), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

if __name__ == "__main__":
    logger.info("Starting batch archiving of processed messages into game_sessions")
    batch_archive_all_collections()
