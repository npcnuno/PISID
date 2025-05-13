import os
import json
import time
import queue
import logging
import threading
from threading import Thread
import mysql.connector
from paho.mqtt import client as mqtt_client
from datetime import datetime

# Logging setup
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables with defaults
LOCAL_MYSQL_HOST = os.getenv('LOCAL_MYSQL_HOST', 'localhost')
LOCAL_MYSQL_USER = os.getenv('LOCAL_MYSQL_USER', 'labuser')
LOCAL_MYSQL_PASSWORD = os.getenv('LOCAL_MYSQL_PASSWORD', 'password')
LOCAL_MYSQL_DATABASE = os.getenv('LOCAL_MYSQL_DATABASE', 'mydb')

CLOUD_MYSQL_HOST = os.getenv('CLOUD_MYSQL_HOST', '194.210.86.10')
CLOUD_MYSQL_USER = os.getenv('CLOUD_MYSQL_USER', 'aluno')
CLOUD_MYSQL_PASSWORD = os.getenv('CLOUD_MYSQL_PASSWORD', 'aluno')
CLOUD_MYSQL_DATABASE = os.getenv('CLOUD_MYSQL_DATABASE', 'maze')

MQTT_BROKER = os.getenv('MQTT_BROKER', 'test.mosquitto.org')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
MOVEMENT_TOPIC_PREFIX = os.getenv('MOVEMENT_TOPIC_PREFIX', 'pisid_mazemov_')
SOUND_TOPIC_PREFIX = os.getenv('SOUND_TOPIC_PREFIX', 'pisid_mazesound_')
PROCESSED_SUFFIX = os.getenv('PROCESSED_SUFFIX', '_processed')
CONFIRMED_SUFFIX = os.getenv('CONFIRMED_SUFFIX', '_confirmed')
SOUND_ALERT_TOLARANCE = int(os.getenv("SOUND_ALERT_TOLARANCE", "1"))

# Global variables
CORRIDOR_MAP = {}
BASENOISE = 0.0
TOLERABLENOISEVARIATION = 0.0
movement_queue = queue.Queue()
sound_queue = queue.Queue()

def fetch_corridor_data():
    """Fetch corridor data from the cloud MySQL database."""
    global CORRIDOR_MAP
    fetch_db_config = {
        "host": CLOUD_MYSQL_HOST,
        "user": CLOUD_MYSQL_USER,
        "password": CLOUD_MYSQL_PASSWORD,
        "database": CLOUD_MYSQL_DATABASE,
        "port": 3306
    }
    try:
        with mysql.connector.connect(**fetch_db_config) as mysql_conn:
            with mysql_conn.cursor() as cursor:
                cursor.execute("SELECT Rooma, Roomb FROM corridor")
                corridors = cursor.fetchall()
                for corridor in corridors:
                    CORRIDOR_MAP[(corridor[0], corridor[1])] = True
                logger.info("Corridor data fetched successfully from cloud database")
    except mysql.connector.Error as e:
        logger.error(f"Error fetching corridor data from cloud database: {e}")

def fetch_setupmaze_data():
    """Fetch noise setup data from the cloud MySQL database."""
    global BASENOISE, TOLERABLENOISEVARIATION
    fetch_db_config = {
        "host": CLOUD_MYSQL_HOST,
        "user": CLOUD_MYSQL_USER,
        "password": CLOUD_MYSQL_PASSWORD,
        "database": CLOUD_MYSQL_DATABASE,
        "port": 3306
    }
    try:
        with mysql.connector.connect(**fetch_db_config) as mysql_conn:
            with mysql_conn.cursor() as cursor:
                cursor.execute("SELECT normalnoise, noisevartoleration FROM setupmaze LIMIT 1")
                result = cursor.fetchone()
                if result:
                    BASENOISE = float(result[0])
                    TOLERABLENOISEVARIATION = float(result[1])
                logger.info("Setupmaze data fetched successfully from cloud database")
    except mysql.connector.Error as e:
        logger.error(f"Error fetching setupmaze data from cloud database: {e}")

def on_connect(client, userdata, flags, rc):
    """Callback for when the MQTT client connects to the broker."""
    if rc == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe(f"{MOVEMENT_TOPIC_PREFIX}{PLAYER_ID}{PROCESSED_SUFFIX}", qos=int(os.getenv('MQTT_QOS', '2')))
        client.subscribe(f"{SOUND_TOPIC_PREFIX}{PLAYER_ID}{PROCESSED_SUFFIX}", qos=int(os.getenv('MQTT_QOS', '2')))
    else:
        logger.error(f"Failed to connect to MQTT broker with code: {rc}")

def on_message(client, userdata, msg):
    """Callback for when a message is received from the MQTT broker."""
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
    """Worker thread to process movement messages and insert them into MySQL."""
    mysql_conn = None
    ack_topic = f"{MOVEMENT_TOPIC_PREFIX}{PLAYER_ID}{CONFIRMED_SUFFIX}"
    while True:
        payload = movement_queue.get()
        cursor = None
        try:
            if mysql_conn is None or not mysql_conn.is_connected():
                mysql_conn = mysql.connector.connect(
                    host=LOCAL_MYSQL_HOST,
                    user=LOCAL_MYSQL_USER,
                    password=LOCAL_MYSQL_PASSWORD,
                    database=LOCAL_MYSQL_DATABASE
                )
                logger.info("Movement worker established MySQL connection")
            cursor = mysql_conn.cursor()

            message_id = payload["_id"]
            origin_room = payload["RoomOrigin"]
            destination_room = payload["RoomDestiny"]
            marsami = payload["Marsami"]
            status = payload["Status"]
            hora_evento = payload["timestamp"]
            timestamp_dt = datetime.fromisoformat(hora_evento)
            hora_evento = timestamp_dt.timestamp()
            sql = """INSERT INTO MedicaoPassagem (hora, salaOrigem, salaDestino, marsami, status, idJogo)
                     VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s, %s)"""
            params = (hora_evento, origin_room, destination_room, marsami, status, GAME_ID)
            cursor.execute(sql, params)
            mysql_conn.commit()
            last_inserted_id = cursor.lastrowid
            logger.info(f"Inserted movement message for player {payload['Player']} with MySQL ID {last_inserted_id}")

            validate_and_log_invalid_movement(marsami, origin_room, destination_room, hora_evento)

            ack_payload = {"_id": message_id, "mysqlID": last_inserted_id}
            mqtt_instance.publish(ack_topic, json.dumps(ack_payload), qos=int(os.getenv('MQTT_QOS', '2')))
            logger.info(f"Sent ACK to {ack_topic} with _id {message_id} and mysqlID {last_inserted_id}")

        except KeyError as e:
            logger.error(f"Missing key in movement payload: {e}")
        except ValueError as e:
            logger.error(f"Invalid value in movement payload: {e}")
        except mysql.connector.Error as e:
            if e.errno == 1452:
                logger.error(f"Foreign key constraint failed: idJogo={GAME_ID} does not exist in Jogo table")
            else:
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
    """Worker thread to process sound messages and insert them into MySQL."""
    mysql_conn = None
    ack_topic = f"{SOUND_TOPIC_PREFIX}{PLAYER_ID}{CONFIRMED_SUFFIX}"
    while True:
        payload = sound_queue.get()
        cursor = None
        try:
            if mysql_conn is None or not mysql_conn.is_connected():
                mysql_conn = mysql.connector.connect(
                    host=LOCAL_MYSQL_HOST,
                    user=LOCAL_MYSQL_USER,
                    password=LOCAL_MYSQL_PASSWORD,
                    database=LOCAL_MYSQL_DATABASE
                )
                logger.info("Sound worker established MySQL connection")
            cursor = mysql_conn.cursor()

            message_id = payload["_id"]
            timestamp_str = payload["Hour"]
            timestamp_dt = datetime.fromisoformat(timestamp_str)
            timestamp = timestamp_dt.timestamp()
            sound_level = float(payload["Sound"])
            formatted_sound = f"{sound_level:.4f}"[:12]

            validate_and_log_alert_sound(sound_level, timestamp)
            validate_and_log_outlier_sound(sound_level, timestamp)

            sql = """INSERT INTO Sound (hora, sound, idJogo)
                     VALUES (FROM_UNIXTIME(%s), %s, %s)"""
            params = (timestamp, formatted_sound, GAME_ID)
            cursor.execute(sql, params)
            mysql_conn.commit()
            last_inserted_id = cursor.lastrowid
            logger.info(f"Inserted sound message for player {payload['Player']} with MySQL ID {last_inserted_id}")

            ack_payload = {"_id": message_id, "mysqlID": last_inserted_id}
            mqtt_instance.publish(ack_topic, json.dumps(ack_payload), qos=int(os.getenv('MQTT_QOS', '2')))
            logger.info(f"Sent ACK to {ack_topic} with _id {message_id} and mysqlID {last_inserted_id}")
        except KeyError as e:
            logger.error(f"Missing key in sound payload: {e}")
        except ValueError as e:
            logger.error(f"Invalid value in sound payload: {e}")
        except mysql.connector.Error as e:
            if e.errno == 1452:
                logger.error(f"Foreign key constraint failed: idJogo={GAME_ID} does not exist in Jogo table")
            else:
                logger.error(f"MySQL error in worker_mazesound: {e}")
            if mysql_conn:
                mysql_conn.rollback()
        except Exception as e:
            logger.error(f"Unexpected error in worker_mazesound: {e}")
        finally:
            if cursor:
                cursor.close()
            sound_queue.task_done()

def validate_and_log_invalid_movement(marsami, sala_origem, sala_destino, hora_evento):
    """Log invalid movement to the Mensagens table."""
    if (sala_origem, sala_destino) not in CORRIDOR_MAP:
        try:
            with mysql.connector.connect(
                host=LOCAL_MYSQL_HOST,
                user=LOCAL_MYSQL_USER,
                password=LOCAL_MYSQL_PASSWORD,
                database=LOCAL_MYSQL_DATABASE
            ) as mysql_conn:
                with mysql_conn.cursor() as cursor:
                    mensagem = f"Movimento inválido de {sala_origem} para {sala_destino} pelo marsami {marsami}"
                    insert_sql = """
                    INSERT INTO Mensagens (hora, sensor, leitura, tipoAlerta, mensagem, horaEscrita, idJogo)
                    VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s, NOW(), %s)
                    """
                    cursor.execute(insert_sql, (
                        hora_evento,
                        1,  # Assuming sensor 1 for movement
                        None,
                        "MOVIMENTO",
                        mensagem,
                        GAME_ID
                    ))
                    mysql_conn.commit()
                    logger.warning(f"Logged invalid movement: {mensagem}")
        except mysql.connector.Error as e:
            logger.error(f"Error logging invalid movement: {e}")

def validate_and_log_alert_sound(actual_sound, hora_evento):
    """Log sound alert if sound level is close to the limit."""
    if actual_sound > BASENOISE + TOLERABLENOISEVARIATION - SOUND_ALERT_TOLARANCE:
        try:
            with mysql.connector.connect(
                host=LOCAL_MYSQL_HOST,
                user=LOCAL_MYSQL_USER,
                password=LOCAL_MYSQL_PASSWORD,
                database=LOCAL_MYSQL_DATABASE
            ) as mysql_conn:
                with mysql_conn.cursor() as cursor:
                    mensagem = f"Alerta de som com o valor de: {actual_sound}, prestes a atingir o limite!"
                    insert_sql = """
                    INSERT INTO Mensagens (hora, sensor, leitura, tipoAlerta, mensagem, horaEscrita, idJogo)
                    VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s, NOW(), %s)
                    """
                    cursor.execute(insert_sql, (
                        hora_evento,
                        2,  # Assuming sensor 2 for sound
                        actual_sound,
                        "SOM",
                        mensagem,
                        GAME_ID
                    ))
                    mysql_conn.commit()
                    logger.warning(f"Logged sound alert: {mensagem}")
        except mysql.connector.Error as e:
            logger.error(f"Error logging sound alert: {e}")

def validate_and_log_outlier_sound(actual_sound, hora_evento):
    """Log sound outlier if sound level exceeds the tolerance."""
    if actual_sound > BASENOISE + TOLERABLENOISEVARIATION:
        try:
            with mysql.connector.connect(
                host=LOCAL_MYSQL_HOST,
                user=LOCAL_MYSQL_USER,
                password=LOCAL_MYSQL_PASSWORD,
                database=LOCAL_MYSQL_DATABASE
            ) as mysql_conn:
                with mysql_conn.cursor() as cursor:
                    mensagem = f"Outlier de som com o valor de: {actual_sound}"
                    insert_sql = """
                    INSERT INTO Mensagens (hora, sensor, leitura, tipoAlerta, mensagem, horaEscrita, idJogo)
                    VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s, NOW(), %s)
                    """
                    cursor.execute(insert_sql, (
                        hora_evento,
                        2,  # Assuming sensor 2 for sound
                        actual_sound,
                        "SOM",
                        mensagem,
                        GAME_ID
                    ))
                    mysql_conn.commit()
                    logger.warning(f"Logged sound outlier: {mensagem}")
        except mysql.connector.Error as e:
            logger.error(f"Error logging sound outlier: {e}")

if __name__ == "__main__":
    GAME_ID = os.getenv('GAME_ID')
    PLAYER_ID = os.getenv('PLAYER_ID')

    if GAME_ID is None or PLAYER_ID is None:
        logger.error("GAME_ID and PLAYER_ID must be set as environment variables")
        exit(1)

    try:
        GAME_ID = int(GAME_ID)
    except ValueError:
        logger.error("GAME_ID must be an integer")
        exit(1)

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
    mqtt_instance.loop_forever()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down")
        mqtt_instance.loop_stop()
        mqtt_instance.disconnect()