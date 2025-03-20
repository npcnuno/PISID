import os
from pymongo import MongoClient, errors
import logging
from datetime import datetime
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

def move_messages_to_sessions(client):
    """Continuously move unmoved messages to their respective game sessions"""
    db = client[MONGO_DB]
    move_messages_col = db["move_messages"]
    sound_messages_col = db["sound_messages"]
    game_sessions_col = db["game_sessions"]
    
    while True:
        try:
            for collection, message_type in [
                (move_messages_col, "movement_messages"),
                (sound_messages_col, "sound_messages")
            ]:
                # Find messages that haven’t been moved yet
                unmoved_messages = collection.find({"Already_moved": "false"})
                for message in unmoved_messages:
                    message_id = message["_id"]
                    session_id = message.get("session_id")
                    
                    if not session_id:
                        logger.error(f"Message {message_id} missing session_id, skipping")
                        continue
                    
                    try:
                        # Prepare message data, excluding internal fields
                        message_data = {
                            k: v for k, v in message.items()
                            if k not in ["_id", "session_id", "Already_moved", "sent"]
                        }
                        
                        # Move the message to its session by appending to the appropriate array
                        result = game_sessions_col.update_one(
                            {"_id": session_id},
                            {"$push": {message_type: message_data}}
                        )
                        
                        if result.modified_count == 0:
                            logger.warning(f"No session found or updated for session_id {session_id}, message {message_id}")
                            continue
                        
                        # Mark the message as moved
                        collection.update_one(
                            {"_id": message_id},
                            {"$set": {"Already_moved": "true", "moved_timestamp": datetime.now()}}
                        )
                        logger.info(f"Moved message {message_id} to session {session_id} in {message_type}")
                    except errors.PyMongoError as e:
                        logger.error(f"Failed to move message {message_id} to session {session_id}: {e}")
            time.sleep(1)  # Check every second
        except Exception as e:
            logger.error(f"Error in move_messages_to_sessions: {e}")
            time.sleep(5)  # Wait before retrying on major error

def main():
    mongo_client = connect_to_mongodb()
    try:
        move_messages_to_sessions(mongo_client)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        mongo_client.close()
        logger.info("MongoDB connection closed")

if __name__ == "__main__":
    main()
