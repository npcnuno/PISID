import os
import json
import time
import queue
import logging
from threading import Thread
import mysql.connector
from paho.mqtt import client as mqtt_client
from datetime import datetime

# Logging setup
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables with defaults
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'user')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'password')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'database')
MQTT_BROKER = os.getenv('MQTT_BROKER', 'test.mosquitto.org')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
PLAYER_ID = os.getenv('PLAYER_ID', '33')
MQTT_QOS = int(os.getenv('MQTT_QOS', '2'))
MOVEMENT_TOPIC_PREFIX = os.getenv('MOVEMENT_TOPIC_PREFIX', 'pisid_mazemov_')
SOUND_TOPIC_PREFIX = os.getenv('SOUND_TOPIC_PREFIX', 'pisid_mazesound_')
PROCESSED_SUFFIX = os.getenv('PROCESSED_SUFFIX', '_processed')
CONFIRMED_SUFFIX = os.getenv('CONFIRMED_SUFFIX', '_confirmed')
GAME_ID = int(os.getenv('GAME_ID', '1'))

# Global variables
CORRIDOR_MAP = {}
BASENOISE = 0.0
TOLERABLENOISEVARIATION = 0.0
movement_queue = queue.Queue()
sound_queue = queue.Queue()

def fetch_corridor_data():
    global CORRIDOR_MAP
    fetch_db_config = {
        "host": "194.210.86.10",
        "user": "aluno",
        "password": "aluno",
        "database": "maze",
        "port": 3306
    }
    mysql_conn = None
    cursor = None
    try:
        mysql_conn = mysql.connector.connect(**fetch_db_config)
        cursor = mysql_conn.cursor()
        cursor.execute("SELECT Rooma, Roomb FROM corridor")
        corridors = cursor.fetchall()
        for corridor in corridors:
            CORRIDOR_MAP[(corridor[0], corridor[1])] = True
        logger.info("Corridor data fetched successfully from cloud database")
    except mysql.connector.Error as e:
        logger.error(f"Error fetching corridor data from cloud database: {e}")
    finally:
        if cursor:
            cursor.close()
        if mysql_conn:
            mysql_conn.close()

# Fetch setupmaze data from cloud database
def fetch_setupmaze_data():
    global BASENOISE, TOLERABLENOISEVARIATION
    fetch_db_config = {
        "host": "194.210.86.10",
        "user": "aluno",
        "password": "aluno",
        "database": "maze",
        "port": 3306
    }
    mysql_conn = None
    cursor = None
    try:
        mysql_conn = mysql.connector.connect(**fetch_db_config)
        cursor = mysql_conn.cursor()
        cursor.execute("SELECT normalnoise, noisevartoleration FROM setupmaze LIMIT 1")
        result = cursor.fetchone()
        if result:
            BASENOISE = float(result[0])
            TOLERABLENOISEVARIATION = float(result[1])
        logger.info("Setupmaze data fetched successfully from cloud database")
    except mysql.connector.Error as e:
        logger.error(f"Error fetching setupmaze data from cloud database: {e}")
    finally:
        if cursor:
            cursor.close()
        if mysql_conn:
            mysql_conn.close()

def on_connect(client, userdata, flags, rc):
    """Callback for when the MQTT client connects to the broker."""
    if rc == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe(f"{MOVEMENT_TOPIC_PREFIX}{PLAYER_ID}{PROCESSED_SUFFIX}", qos=MQTT_QOS)
        client.subscribe(f"{SOUND_TOPIC_PREFIX}{PLAYER_ID}{PROCESSED_SUFFIX}", qos=MQTT_QOS)
    else:
        logger.error(f"Failed to connect to MQTT broker with code: {rc}")

def on_message(client, userdata, msg):
    """Callback for when an MQTT message is received."""
    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        if topic == f"{MOVEMENT_TOPIC_PREFIX}{PLAYER_ID}{PROCESSED_SUFFIX}":
            movement_queue.put(payload)
            logger.debug(f"Added movement message to queue from topic: {topic}")
        elif topic == f"{SOUND_TOPIC_PREFIX}{PLAYER_ID}{PROCESSED_SUFFIX}":
            sound_queue.put(payload)
            logger.debug(f"Added sound message to queue from topic: {topic}")
        else:
            logger.warning(f"Received message on unknown topic: {topic}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON payload: {e}")
    except Exception as e:
        logger.error(f"Error in on_message: {e}")

def worker_mazemov():
    """Worker function to process movement messages."""
    mysql_conn = None
    ack_topic = f"{MOVEMENT_TOPIC_PREFIX}{PLAYER_ID}{CONFIRMED_SUFFIX}"
    while True:
        payload = movement_queue.get()
        cursor = None
        try:
            if mysql_conn is None or not mysql_conn.is_connected():
                mysql_conn = mysql.connector.connect(
                    host=MYSQL_HOST,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    database=MYSQL_DATABASE
                )
                logger.info("Movement worker established MySQL connection")
            cursor = mysql_conn.cursor()

            game_id = GAME_ID
            sensor_id = int(payload.get("sensor_id", 1))
            timestamp = datetime.now().timestamp()
            player_id = payload["player"]
            origin_room = payload["room_origin"]
            destination_room = payload["room_destiny"]

            if (origin_room, destination_room) in CORRIDOR_MAP:
                leitura = None
                tipo_alerta = "Movement"
                mensagem = f"Player {player_id} moved from {origin_room} to {destination_room}"
                sql = """INSERT INTO Mensagens (hora, sensor, leitura, tipoAlerta, mensagem, horaEscrita, idJogo)
                         VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s, NOW(), %s)"""
                params = (timestamp, sensor_id, leitura, tipo_alerta, mensagem, game_id)
                cursor.execute(sql, params)
                mysql_conn.commit()
                logger.info(f"Inserted movement message for player {player_id}")
            else:
                logger.warning(f"Invalid movement from {origin_room} to {destination_room}")

            ack_payload = {"_id": payload["_id"]}
            mqtt_instance.publish(ack_topic, json.dumps(ack_payload), qos=MQTT_QOS)
            logger.info(f"Sent ACK to {ack_topic}")

        except KeyError as e:
            logger.error(f"Missing key in movement payload: {e}")
        except ValueError as e:
            logger.error(f"Invalid value in movement payload: {e}")
        except mysql.connector.Error as e:
            logger.error(f"MySQL error in worker_mazemov: {e}")
            if mysql_conn:
                mysql_conn.rollback()
        except Exception as e:
            logger.error(f"Unexpected error in worker_mazemov: {e}")
        finally:
            if cursor:
                cursor.close()
            movement_queue.task_done()

def worker_mazesound():
    """Worker function to process sound messages."""
    mysql_conn = None
    ack_topic = f"{SOUND_TOPIC_PREFIX}{PLAYER_ID}{CONFIRMED_SUFFIX}"
    while True:
        payload = sound_queue.get()
        cursor = None
        try:
            if mysql_conn is None or not mysql_conn.is_connected():
                mysql_conn = mysql.connector.connect(
                    host=MYSQL_HOST,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    database=MYSQL_DATABASE
                )
                logger.info("Sound worker established MySQL connection")
            cursor = mysql_conn.cursor()

            game_id = GAME_ID
            sensor_id = int(payload.get("sensor_id", 1))
            timestamp_str = payload["hour"]
            timestamp_dt = datetime.fromisoformat(timestamp_str)
            timestamp = timestamp_dt.timestamp()
            player_id = payload["player"]
            sound_level = float(payload["sound"])
            room_id = payload.get("room_id", "Unknown")
            if BASENOISE - TOLERABLENOISEVARIATION <= sound_level <= BASENOISE + TOLERABLENOISEVARIATION:
                leitura = sound_level
                tipo_alerta = "Sound"
                mensagem = f"Sound level: {sound_level} dB by player {player_id}"
                sql = """INSERT INTO Mensagens (hora, sensor, leitura, tipoAlerta, mensagem, horaEscrita, idJogo)
                         VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s, NOW(), %s)"""
                params = (timestamp, sensor_id, leitura, tipo_alerta, mensagem, game_id)
                cursor.execute(sql, params)
                mysql_conn.commit()
                logger.info(f"Inserted sound message for player {player_id}")
            else:
                logger.warning(f"Sound level {sound_level} dB out of tolerance in {room_id}")

            ack_payload = {"_id": payload["_id"]}
            mqtt_instance.publish(ack_topic, json.dumps(ack_payload), qos=MQTT_QOS)
            logger.info(f"Sent ACK to {ack_topic}")

        except KeyError as e:
            logger.error(f"Missing key in sound payload: {e}")
        except ValueError as e:
            logger.error(f"Invalid value in sound payload: {e}")
        except mysql.connector.Error as e:
            logger.error(f"MySQL error in worker_mazesound: {e}")
            if mysql_conn:
                mysql_conn.rollback()
        except Exception as e:
            logger.error(f"Unexpected error in worker_mazesound: {e}")
        finally:
            if cursor:
                cursor.close()
            sound_queue.task_done()

if __name__ == "__main__":
    fetch_corridor_data()
    fetch_setupmaze_data()

    movement_thread = Thread(target=worker_mazemov, daemon=True)
    sound_thread = Thread(target=worker_mazesound, daemon=True)
    movement_thread.start()
    sound_thread.start()

    mqtt_instance = mqtt_client.Client()
    mqtt_instance.on_connect = on_connect
    mqtt_instance.on_message = on_message
    mqtt_instance.connect(MQTT_BROKER, MQTT_PORT)
    mqtt_instance.loop_start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down")
        mqtt_instance.loop_stop()
        mqtt_instance.disconnect()
