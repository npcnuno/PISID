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

# Environment variables with default
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'labuser')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'password')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'mydb')
MQTT_BROKER = os.getenv('MQTT_BROKER', 'test.mosquitto.org')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
PLAYER_ID = os.getenv('PLAYER_ID', '33')
MQTT_QOS = int(os.getenv('MQTT_QOS', '2'))
MOVEMENT_TOPIC_PREFIX = os.getenv('MOVEMENT_TOPIC_PREFIX', 'pisid_mazemov_')
SOUND_TOPIC_PREFIX = os.getenv('SOUND_TOPIC_PREFIX', 'pisid_mazesound_')
PROCESSED_SUFFIX = os.getenv('PROCESSED_SUFFIX', '_processed')
CONFIRMED_SUFFIX = os.getenv('CONFIRMED_SUFFIX', '_confirmed')
GAME_ID = int(os.getenv('GAME_ID', '1'))
USER_EMAIL = os.getenv('USER_EMAIL', 'default@user.com')

# Global variables
CORRIDOR_MAP = {}
BASENOISE = 0.0
TOLERABLENOISEVARIATION = 0.0
movement_queue = queue.Queue()
sound_queue = queue.Queue()


def initialize_game():
    """Initialize the game by creating a user and game entry in MySQL if they don't exist."""
    global GAME_ID
    mysql_conn = None
    cursor = None
    try:
        mysql_conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = mysql_conn.cursor()
        cursor.execute("SELECT email FROM Users WHERE email = %s", (USER_EMAIL,))
        if cursor.fetchone() is None:
            sql_user = """INSERT INTO Users (email, nome, telemovel, tipo, grupo)
                          VALUES (%s, %s, %s, %s, %s)"""
            user_params = (USER_EMAIL, 'Default User', '000000000', 'player', 1)
            cursor.execute(sql_user, user_params)
            mysql_conn.commit()
            logger.info(f"Created user with email={USER_EMAIL}")
        else:
            logger.info(f"User with email={USER_EMAIL} already exists")
        sql_game = """INSERT INTO Jogo (email, jogador, scoreTotal, dataHoraInicio)
                      VALUES (%s, %s, %s, NOW())"""
        game_params = (USER_EMAIL, PLAYER_ID, 0)
        cursor.execute(sql_game, game_params)
        mysql_conn.commit()
        GAME_ID = cursor.lastrowid
        logger.info(f"Created new game with idJogo={GAME_ID} for player {PLAYER_ID}")
    except mysql.connector.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if cursor:
            cursor.close()
        if mysql_conn:
            mysql_conn.close()

def fetch_corridor_data():
    """Fetch corridor data from a cloud MySQL database."""
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

def fetch_setupmaze_data():
    """Fetch noise setup data from a cloud MySQL database."""
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
                    host=MYSQL_HOST,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    database=MYSQL_DATABASE
                )
                logger.info("Movement worker established MySQL connection")
            cursor = mysql_conn.cursor()

            message_id = payload["_id"]
            game_id = GAME_ID
            origin_room = payload["room_origin"]
            destination_room = payload["room_destiny"]
            marsami = payload["marsami"]
            status = payload["status"]

            hora_evento = datetime.now()

            #NOTE:  insert the movement message
            sql = """INSERT INTO MedicaoPassagem (hora, salaOrigem, salaDestino, marsami, status, idJogo)
                     VALUES (NOW(), %s, %s, %s, %s, %s)"""
            params = (origin_room, destination_room, marsami, status, game_id)
            cursor.execute(sql, params)
            mysql_conn.commit()
            last_inserted_id = cursor.lastrowid
            logger.info(f"Inserted movement message for player {payload['player']} with MySQL ID {last_inserted_id}")

            validate_and_log_invalid_movement(marsami, origin_room, destination_room, hora_evento)

            #NOTE: Send acknowledgment
            ack_payload = {"_id": message_id, "mysqlID": last_inserted_id}
            mqtt_instance.publish(ack_topic, json.dumps(ack_payload), qos=MQTT_QOS)
            logger.info(f"Sent ACK to {ack_topic} with _id {message_id} and mysqlID {last_inserted_id}")

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
    """Worker thread to process sound messages and insert them into MySQL."""
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

            message_id = payload["_id"]
            game_id = GAME_ID
            timestamp_str = payload["hour"]
            timestamp_dt = datetime.fromisoformat(timestamp_str)
            timestamp = timestamp_dt.timestamp()
            sound_level = float(payload["sound"])
            formatted_sound = f"{sound_level:.4f}"[:12]

            #NOTE: insert the sound message
            sql = """INSERT INTO Sound (hora, sound, idJogo)
                     VALUES (FROM_UNIXTIME(%s), %s, %s)"""
            params = (timestamp, formatted_sound, game_id)
            cursor.execute(sql, params)
            mysql_conn.commit()
            last_inserted_id = cursor.lastrowid
            logger.info(f"Inserted sound message for player {payload['player']} with MySQL ID {last_inserted_id}")
            #NOTE: Send acknowledgment
            ack_payload = {"_id": message_id, "mysqlID": last_inserted_id}
            mqtt_instance.publish(ack_topic, json.dumps(ack_payload), qos=MQTT_QOS)
            logger.info(f"Sent ACK to {ack_topic} with _id {message_id} and mysqlID {last_inserted_id}")
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

def validate_and_log_invalid_movement(marsami, sala_origem, sala_destino, hora_evento):
    if (sala_origem, sala_destino) not in CORRIDOR_MAP:
        try:
            mysql_conn = mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE
            )
            cursor = mysql_conn.cursor()
            mensagem = f"Movimento inválido de {sala_origem} para {sala_destino} pelo marsami {marsami}"
            insert_sql = """
            INSERT INTO Mensagens (hora, sensor, leitura, tipoAlerta, mensagem, horaEscrita, idJogo)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
            cursor.execute(insert_sql, (
                hora_evento,  # hora real do evento
                None,  # sensor não se aplica
                None,  # leitura não se aplica
                "MOVIMENTO",  # tipo de alerta
                mensagem,  # texto da mensagem
                datetime.now(),  # hora de escrita
                GAME_ID  # id do jogo atual
            ))
            mysql_conn.commit()
            logger.warning(f"Movimento inválido registado: {mensagem}")
        except mysql.connector.Error as e:
            logger.error(f"Erro ao inserir mensagem de erro: {e}")
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()

if __name__ == "__main__":
    time.sleep(10)
    initialize_game()
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
