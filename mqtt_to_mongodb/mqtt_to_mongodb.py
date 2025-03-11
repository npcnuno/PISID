import paho.mqtt.client as mqtt
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import time
import logging
from datetime import datetime
import re
import json

# Configuration
PLAYER_ID = 33
MQTT_BROKER = "broker.mqtt-dashboard.com"
MQTT_PORT = 1883
TOPICS = [
    f"pisid_mazeNovo_{PLAYER_ID}",
    f"pisid_mazemov_{PLAYER_ID}",
    f"pisid_mazesound_{PLAYER_ID}"
]
MONGO_URI = "mongodb://mongo:27017"
RECONNECT_DELAY = 5

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# MongoDB setup
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["game_monitoring"]

# Collections
players_col = db["players"]
games_col = db["game_sessions"]
raw_col = db["raw_messages"]
movement_col = db["movement_messages"]
sound_col = db["sound_messages"]
failed_col = db["failed_messages"]

# Active games tracker {player_id: game_id}
active_games = {}

def protect_quoted_colons(payload):
    """Protect colons inside quoted strings for JSON parsing"""
    protected = []
    in_quote = False
    buffer = []
    
    for char in payload:
        if char == '"':
            in_quote = not in_quote
            buffer.append(char)
            if not in_quote:
                protected.append(''.join(buffer).replace(':', '__COLON__'))
                buffer = []
        elif in_quote:
            buffer.append(char)
        else:
            protected.append(char)
    
    return ''.join(protected)

def parse_custom_message(raw_payload):
    """Robust parser for custom MQTT messages"""
    try:
        protected = protect_quoted_colons(raw_payload)
        processed = re.sub(r'(\w+)\s*:', r'"\1":', protected)
        processed = processed.replace('__COLON__', ':')
        processed = processed.replace("'", '"')
        processed = re.sub(r',\s*}', '}', processed)
        processed = re.sub(r',\s*]', ']', processed)
        return json.loads(processed)
    except Exception as e:
        logger.error(f"Parsing error: {str(e)}")
        return None

def update_player_status(player_id, is_playing):
    """Atomically update player status"""
    try:
        players_col.update_one(
            {"_id": player_id},
            {"$set": {
                "is_playing": is_playing,
                "last_updated": datetime.now()
            }},
            upsert=True
        )
    except PyMongoError as e:
        logger.error(f"Player status update failed: {str(e)}")

def start_new_game(player_id):
    """Initialize new game session with atomic operations"""
    try:
        # Close any existing active game
        if player_id in active_games:
            end_game(player_id)

        # Create new game document with reference arrays
        game_doc = {
            "player_id": player_id,
            "start_time": datetime.now(),
            "status": "active",
            "raw_messages": [],
            "movement_messages": [],
            "sound_messages": [],
            "failed_messages": []
        }
        result = games_col.insert_one(game_doc)
        game_id = result.inserted_id
        
        # Update tracker and player status
        active_games[player_id] = game_id
        update_player_status(player_id, True)
        logger.info(f"New game started: {game_id}")
        return game_id
    except PyMongoError as e:
        logger.error(f"Game creation failed: {str(e)}")
        return None

def end_game(player_id):
    """Finalize game session with atomic updates"""
    if player_id not in active_games:
        return

    game_id = active_games[player_id]
    
    try:
        # Atomic update of game status and player status
        with mongo_client.start_session() as session:
            with session.start_transaction():
                # Update game document
                games_col.update_one(
                    {"_id": game_id},
                    {"$set": {
                        "status": "completed",
                        "end_time": datetime.now()
                    }},
                    session=session
                )
                
                # Update player status
                update_player_status(player_id, False)
                
                # Remove from active games
                del active_games[player_id]
                logger.info(f"Game {game_id} completed")
    except PyMongoError as e:
        logger.error(f"Game completion failed: {str(e)}")

def check_game_end(parsed):
    """Detect game completion conditions"""
    if isinstance(parsed, dict):
        return any("Cannot move" in str(v) for v in parsed.values())
    return False

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe([(topic, 0) for topic in TOPICS])
    else:
        logger.error(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        raw_payload = msg.payload.decode("utf-8").strip()
        timestamp = datetime.now()
        game_id = active_games.get(PLAYER_ID)

        # Store raw message and get reference
        try:
            raw_doc = {
                "timestamp": timestamp,
                "topic": msg.topic,
                "payload": raw_payload,
                "game_id": game_id
            }
            raw_result = raw_col.insert_one(raw_doc)
            games_col.update_one(
                {"_id": game_id},
                {"$push": {"raw_messages": raw_result.inserted_id}}
            )
        except PyMongoError as e:
            logger.error(f"Raw message handling failed: {str(e)}")

        # Parse message
        parsed = parse_custom_message(raw_payload)
        if not parsed:
            try:
                failed_doc = {
                    "timestamp": timestamp,
                    "topic": msg.topic,
                    "payload": raw_payload,
                    "error": "Parse failed",
                    "game_id": game_id
                }
                failed_result = failed_col.insert_one(failed_doc)
                games_col.update_one(
                    {"_id": game_id},
                    {"$push": {"failed_messages": failed_result.inserted_id}}
                )
            except PyMongoError as e:
                logger.error(f"Failed message handling failed: {str(e)}")
            return

        # Store parsed message in appropriate collection
        try:
            if "mazemov" in msg.topic:
                mov_doc = {
                    "timestamp": timestamp,
                    "parsed_data": parsed,
                    "game_id": game_id
                }
                mov_result = movement_col.insert_one(mov_doc)
                games_col.update_one(
                    {"_id": game_id},
                    {"$push": {"movement_messages": mov_result.inserted_id}}
                )
            elif "mazesound" in msg.topic:
                sound_doc = {
                    "timestamp": timestamp,
                    "parsed_data": parsed,
                    "game_id": game_id
                }
                sound_result = sound_col.insert_one(sound_doc)
                games_col.update_one(
                    {"_id": game_id},
                    {"$push": {"sound_messages": sound_result.inserted_id}}
                )
        except PyMongoError as e:
            logger.error(f"Data storage failed: {str(e)}")

        # Handle game state changes
        if check_game_end(parsed):
            end_game(PLAYER_ID)
        elif "pid_mazeNovo" in msg.topic:
            start_new_game(PLAYER_ID)

    except Exception as e:
        logger.error(f"Message processing failed: {str(e)}")

def run():
    # Initialize new game on startup
    start_new_game(PLAYER_ID)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error(f"Connection error: {str(e)}. Retrying in {RECONNECT_DELAY}s...")
            time.sleep(RECONNECT_DELAY)
    
    # Cleanup
    mongo_client.close()

if __name__ == "__main__":
    run()
