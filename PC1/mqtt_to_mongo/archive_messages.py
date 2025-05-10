import os
import threading
import time
from pymongo import MongoClient, errors, ReadPreference
from bson.objectid import ObjectId
import logging
from datetime import datetime

# Configuration from environment variables
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://admin:adminpass@mongo1:27017,mongo2:27017,mongo3:27017/game_monitoring?replicaSet=my-mongo-set&authSource=admin')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '2'))

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_mongodb(retry_count=5, retry_delay=5):
    """Establish a connection to MongoDB with retry logic."""
    global mongo_client, db
    for attempt in range(retry_count):
        try:
            mongo_client = MongoClient(
                MONGO_URI,
                maxPoolSize=5, minPoolSize=1,
                connectTimeoutMS=5000, socketTimeoutMS=5000,
                serverSelectionTimeoutMS=5000, retryWrites=True
            )
            mongo_client.admin.command('ping')
            logger.info("Connected to MongoDB")
            db = mongo_client['game_monitoring']
            return True
        except errors.PyMongoError as e:
            logger.error(f"Connection failed (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
    logger.error("Failed to connect to MongoDB")
    raise SystemExit(1)

def get_relevant_collections():
    """Retrieve all collections that are not 'game_sessions'."""
    all_collections = db.list_collection_names()
    return [col for col in all_collections if col != 'game_sessions' and 'failed_messages']

def archive_collection(collection_name):
    """Archive processed documents from a collection into game session documents."""
    source_col = db[collection_name]
    game_sessions_col = db['game_sessions']
    while True:
        try:
            with mongo_client.start_session() as session:
                session.start_transaction(read_preference=ReadPreference.PRIMARY)
                try:
                    # Fetch batch of processed documents
                    messages = list(source_col.find({"processed": True}, limit=BATCH_SIZE, session=session))
                    if not messages:
                        session.commit_transaction()
                        time.sleep(POLL_INTERVAL)
                        continue
                    logger.info(f"Processing batch of {len(messages)} processed documents from {collection_name}")
                    
                    # Group documents by session_id
                    session_to_docs = {}
                    ids_to_archive = []
                    for doc in messages:
                        session_id = doc.get("session_id")
                        if not session_id:
                            logger.debug(f"Skipping document {doc.get('_id')} from {collection_name}: missing session_id")
                            continue
                        try:
                            session_objectid = ObjectId(session_id)
                        except errors.InvalidURI:
                            logger.debug(f"Skipping document {doc.get('_id')} from {collection_name}: invalid session_id {session_id}")
                            continue
                        # Verify session exists
                        session_doc = game_sessions_col.find_one({"_id": session_objectid}, session=session)
                        if not session_doc:
                            logger.debug(f"Skipping document {doc.get('_id')} from {collection_name}: session {session_id} not found")
                            continue
                        archived_doc = doc.copy()
                        archived_doc["archived_at"] = datetime.now()
                        if session_id not in session_to_docs:
                            session_to_docs[session_id] = []
                        session_to_docs[session_id].append(archived_doc)
                        ids_to_archive.append(doc["_id"])
                    
                    # Archive documents to session arrays
                    for session_id, docs in session_to_docs.items():
                        session_objectid = ObjectId(session_id)
                        # Initialize array if it doesn't exist
                        game_sessions_col.update_one(
                            {"_id": session_objectid},
                            {"$setOnInsert": {collection_name: []}},
                            upsert=True,
                            session=session
                        )
                        # Push documents to the array
                        result = game_sessions_col.update_one(
                            {"_id": session_objectid},
                            {"$push": {collection_name: {"$each": docs}}},
                            session=session
                        )
                        if result.modified_count > 0:
                            logger.info(f"Archived {len(docs)} documents from {collection_name} to session {session_id}")
                        else:
                            logger.warning(f"No documents archived from {collection_name} to session {session_id}, possibly no update needed")
                    
                    # Delete archived documents from source collection
                    if ids_to_archive:
                        result = source_col.delete_many({"_id": {"$in": ids_to_archive}}, session=session)
                        logger.info(f"Deleted {result.deleted_count} processed documents from {collection_name}")
                    
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

def main():
    """Start archiving threads for each collection specified in TOPICS_CONFIG."""
    connect_to_mongodb()
    collections_to_archive = get_relevant_collections()
    if not collections_to_archive:
        logger.error("No collections to archive. Set TOPICS_CONFIG with collection names.")
        raise SystemExit(1)
    
    threads = []
    for collection_name in collections_to_archive:
        t = threading.Thread(target=archive_collection, args=(collection_name,), daemon=True)
        threads.append(t)
        t.start()
    
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down")
        if mongo_client:
            mongo_client.close()

if __name__ == "__main__":
    main()
