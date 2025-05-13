import os
import time
import threading
from mysql.connector import pooling, Error as MySQLError
import logging
from graph import Graph
from database_manager import DatabaseManager
from decision_maker import DecisionMaker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MazeController:
    def __init__(self):
        self.PLAYER_ID = os.getenv("PLAYER_ID", "33")
        self.FRESH_START = os.getenv("FRESH_START", "True").lower() == "true"
        self.MQTT_BROKER = os.getenv("MQTT_BROKER_TO_SEND_COMMANDS", "test.mosquitto.org")
        self.MQTT_TOPIC = os.getenv("MQTT_TOPIC_TO_SEND_COMMANDS", "pisid_mazeact")
        
        DEFAULT_CLOUD_DB = {"host": "194.210.86.10", "user": "aluno", "password": "aluno", "database": "maze", "port": 3306}
        DEFAULT_LOCAL_DB = {"host": "mysql", "port": 3306, "user": "labuser", "password": "labpass", "database": "mydb"}
        
        self.cloud_db_config = DEFAULT_CLOUD_DB
        self.local_db_config = DEFAULT_LOCAL_DB
        self.cloud_pool = self.create_db_pool("cloud_pool", self.cloud_db_config)
        self.local_pool = self.create_db_pool("local_pool", self.local_db_config)

        self.graph = Graph()
        self.current_sound = [None]
        self.sound_condition = threading.Condition()
        self.decision_condition = threading.Condition()

        self.decision_maker = DecisionMaker(
            self.graph, self.sound_condition, self.current_sound, self.decision_condition,
            self.PLAYER_ID, self.cloud_pool, self.MQTT_BROKER, self.MQTT_TOPIC, []
        )
        self.db_manager = DatabaseManager(
            self.graph, self.PLAYER_ID, self.FRESH_START, self.cloud_pool, self.local_pool,
            self.decision_maker, self.decision_condition, self.current_sound, self.sound_condition
        )
        self.running = True

    def create_db_pool(self, pool_name, db_config):
        try:
            pool = pooling.MySQLConnectionPool(pool_name=pool_name, pool_size=5, **db_config)
            logging.info(f"Created pool: {pool_name}")
            return pool
        except MySQLError as e:
            logging.error(f"Failed to create pool {pool_name}: {e}")
            raise

    def start(self):
        threading.Thread(target=self.db_manager.process_new_movements, daemon=True).start()
        threading.Thread(target=self.db_manager.process_new_sounds, daemon=True).start()
        threading.Thread(target=self.decision_maker.start, daemon=True).start()
        try:
            while self.running:
                time.sleep(0.25)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.running = False
        self.db_manager.stop()
        self.decision_maker.stop()
        logging.info("MazeController stopped")

if __name__ == "__main__":
    controller = MazeController()
    controller.start()
