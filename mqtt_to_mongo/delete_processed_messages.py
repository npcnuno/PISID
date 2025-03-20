import os
from pymongo import MongoClient, errors
import logging
import time

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

def delete_processed_messages(client):
    """Continuously delete messages where sent and Already_moved are true"""
    db = client[MONGO_DB]
    move_messages_col = db["move_messages"]
    sound_messages_col = db["sound_messages"]
    
    while True:
        try:
            for collection in [move_messages_col, sound_messages_col]:
                processed_messages = collection.find({"sent": "true", "Already_moved": "true"})
                for message in processed_messages:
                    message_id = message["_id"]
                    try:
                        collection.delete_one({"_id": message_id})
                        logger.info(f"Deleted message {message_id} from {collection.name}")
                    except errors.PyMongoError as e:
                        logger.error(f"Failed to delete message {message_id}: {e}")
            time.sleep(1)  # Check every second
        except Exception as e:
            logger.error(f"Error in delete_processed_messages: {e}")
            time.sleep(5)  # Wait before retrying on major error

def main():
    mongo_client = connect_to_mongodb()
    try:
        delete_processed_messages(mongo_client)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        mongo_client.close()
        logger.info("MongoDB connection closed")

if __name__ == "__main__":
    main()
