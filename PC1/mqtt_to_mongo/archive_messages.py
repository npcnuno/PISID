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
MONGO_URI = os.getenv('MONGO_URI', (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@mongo1:27017,mongo2:27017,mongo3:27017/"
    f"{MONGO_DB}?replicaSet=my-mongo-set&authSource={MONGO_AUTH_SOURCE}&"
    f"w=1&journal=true&retryWrites=true&"
    f"connectTimeoutMS=5000&socketTimeoutMS=5000&serverSelectionTimeoutMS=5000&"
    f"readPreference=primaryPreferred"
))

CHECK_INTERVAL = os.getenv("CHECK_INTERVAL", 2)  # Seconds to check process status
BATCH_SIZE = os.getenv("BATCH_SIZE", 1000)  # Number of documents to process per batch
POLL_INTERVAL = os.getenv("POLL_INTERVAL", 2)  # Seconds between batch checks (low priority)

mongo_client = None
db = None

def connect_to_mongodb(retry_count=5, retry_delay=5):
    """Purpose: Establishes a connection to MongoDB with retry logic for reliable database access.
    Execution Flow:
    1. Declare global variables mongo_client and db to be set upon successful connection.
    2. Loop through retry_count attempts (default 5) to connect to MongoDB.
    3. Create a MongoClient instance with MONGO_URI and settings (e.g., maxPoolSize=5, retryWrites=True).
    4. Send a 'ping' command to the admin database to verify connectivity.
    5. On success, set mongo_client and db globals, log the connection, and return True.
    6. On failure (PyMongoError), log the error with attempt number and details.
    7. If not the last attempt, wait retry_delay seconds (default 5) before retrying.
    8. If all attempts fail, log a final error and raise SystemExit to terminate the program."""
    global mongo_client, db, game_sessions_col, raw_messages_col, sound_messages_col, move_messages_col, failed_messages_col
    for attempt in range(retry_count):
        try:
            mongo_client = MongoClient(
                MONGO_URI,
                maxPoolSize=5, minPoolSize=1,
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
            game_sessions_col = db["game_sessions"]
            return True
        except errors.PyMongoError as e:
            logger.error(f"Connection failed (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
    logger.error("Failed to connect to MongoDB")
    raise SystemExit(1)

def get_player_id_from_topic(topic: str) -> Optional[int]:
    """Purpose: Extracts the player ID from an MQTT topic string for message association.
    Execution Flow:
    1. Split the topic string by '_' to get parts (e.g., 'pisid_mazemov_33' -> ['pisid', 'mazemov', '33']).
    2. Check if there are at least 3 parts, the first is 'pisid', and the second is 'mazemov' or 'mazesound'.
    3. Try to convert the third part to an integer (player_id).
    4. On success, return the player_id.
    5. On ValueError (non-integer), return None."""
    parts = topic.split('_')
    if len(parts) >= 3 and parts[0] == 'pisid' and parts[1] in ['mazemov', 'mazesound']:
        try:
            return int(parts[2])
        except ValueError:
            pass
    return None

def get_player_id_from_doc(doc: Dict) -> Optional[int]:
    """Purpose: Retrieves the player ID from a document, either from 'player' field or topic.
    Execution Flow:
    1. Check if 'player' key exists in the document.
    2. If present, attempt to convert it to an integer and return it.
    3. On ValueError or TypeError, proceed to check 'topic'.
    4. If 'topic' exists, call get_player_id_from_topic to extract player_id.
    5. Return None if neither method succeeds."""
    if "player" in doc:
        try:
            return int(doc["player"])
        except (ValueError, TypeError):
            pass
    if "topic" in doc:
        return get_player_id_from_topic(doc["topic"])
    return None

def get_session_objectid(session_id: str, game_sessions_col, session) -> Optional[ObjectId]:
    """Purpose: Converts a session ID string to an ObjectId and verifies its existence in MongoDB.
    Execution Flow:
    1. Try to convert the session_id string to an ObjectId.
    2. Query game_sessions_col for a document with the ObjectId using the provided session.
    3. If found, return the ObjectId from the document.
    4. If not found, log a warning and return None.
    5. On InvalidURI error (invalid ObjectId format), log an error and return None."""
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
    """Purpose: Processes a batch of documents, grouping them by session for archiving.
    Execution Flow:
    1. Initialize dictionaries to store documents and IDs by session_id.
    2. Set a counter for skipped documents.
    3. Iterate through each document in the batch.
    4. Skip and log unprocessed documents (processed != True).
    5. Extract session_id; skip and log if missing.
    6. Get player_id from the document; skip and log if None.
    7. Get session ObjectId; skip and log if None.
    8. Verify player_id matches session's player_id; skip and log if mismatch.
    9. Copy document, add archived_at timestamp, and append to session_to_docs and session_to_ids.
    10. Log the number of processed and skipped documents.
    11. For each session_id in session_to_docs, ensure collection_name field exists, then archive documents.
    12. Log archiving success or warning; remove from session_to_ids on failure or no update.
    13. Return session_to_ids for deletion."""
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
    """Purpose: Continuously archives processed documents from a source collection into game_sessions.
    Execution Flow:
    1. Enter an infinite loop to process batches of documents.
    2. Start a MongoDB session with a transaction using primary read preference.
    3. Find up to BATCH_SIZE processed documents from source_col within the session.
    4. If no documents are found, commit the transaction and sleep for POLL_INTERVAL seconds.
    5. Log the batch size and process it with process_batch to get session_to_ids.
    6. For each session_id, delete the processed document IDs from source_col.
    7. Commit the transaction.
    8. On any error within the transaction, log it, abort the transaction, and sleep 10 seconds.
    9. On MongoDB-specific error outside the transaction, log and sleep 10 seconds."""
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
    """Purpose: Starts archiving threads for all relevant collections concurrently.
    Execution Flow:
    1. Call connect_to_mongodb to establish a database connection.
    2. Set up collection variables for raw_messages, move_messages, sound_messages, failed_messages, and game_sessions. Now inside connect_to_mongodb
    3. Define a dictionary mapping collection names to their respective collections.
    4. Create a list to store threads.
    5. For each collection, start a daemon thread running archive_collection with the collection name, source, and game_sessions_col.
    6. Append each thread to the list.
    7. Join all threads (though as daemons, they run indefinitely until the main program exits)."""
    connect_to_mongodb()
    
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
