import os
import threading
import logging
from mysql.connector import pooling, Error as MySQLError
from graph import Graph
from database_manager import DatabaseManager
from decision_maker import DecisionMaker

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read environment variables
USER_EMAIL = os.getenv('USER_EMAIL', 'user@example.com')
PLAYER_ID = os.getenv('PLAYER_ID', 'player1')
FRESH_START = os.getenv('FRESH_START', 'True').lower() == 'true'
MQTT_BROKER = os.getenv('MQTT_BROKER', 'test.mosquitto.org')
MQTT_TOPIC = os.getenv('MQTT_TOPIC', 'pisid_mazeact')

# Database configurations from environment variables
cloud_db_config = {
    "host": os.getenv("CLOUD_MYSQL_HOST", "194.210.86.10"),
    "user": os.getenv("CLOUD_MYSQL_USER", "aluno"),
    "password": os.getenv("CLOUD_MYSQL_PASSWORD", "aluno"),
    "database": os.getenv("CLOUD_MYSQL_DATABASE", "maze"),
    "port": 3306
}
local_db_config = {
    "host": os.getenv("LOCAL_MYSQL_HOST", "mysql"),
    "port": 3306,
    "user": os.getenv("LOCAL_MYSQL_USER", "labuser"),
    "password": os.getenv("LOCAL_MYSQL_PASSWORD", "labpass"),
    "database": os.getenv("LOCAL_MYSQL_DATABASE", "mydb")
}

# Create connection pools
try:
    cloud_pool = pooling.MySQLConnectionPool(pool_name="cloud_pool", pool_size=5, **cloud_db_config)
    local_pool = pooling.MySQLConnectionPool(pool_name="local_pool", pool_size=5, **local_db_config)
    logging.info("Successfully created MySQL connection pools")
except MySQLError as e:
    logging.error(f"Failed to create connection pools: {e}")
    raise

class MazeController:
    def __init__(self, user_email, player_id, fresh_start, cloud_pool, local_pool, mqtt_broker, mqtt_topic):
        self.USER_EMAIL = user_email
        self.PLAYER_ID = player_id
        self.FRESH_START = fresh_start
        self.cloud_pool = cloud_pool
        self.local_pool = local_pool
        self.MQTT_BROKER = mqtt_broker
        self.MQTT_TOPIC = mqtt_topic
        
        self.graph = Graph()
        self.decision_condition = threading.Condition()
        
        self.db_manager = DatabaseManager(
            self.graph, self.USER_EMAIL, self.FRESH_START, self.cloud_pool, self.local_pool,
            self, self.decision_condition
        )
        self.decision_maker = DecisionMaker(
            self.graph, self.PLAYER_ID, self.cloud_pool, self.MQTT_BROKER, self.MQTT_TOPIC
        )
        self.running = True

    def start(self):
        """Start the maze controller."""
        movement_thread = threading.Thread(target=self.db_manager.process_new_movements, daemon=True)
        movement_thread.start()
        
        try:
            self.decision_maker.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop the maze controller."""
        self.running = False
        self.db_manager.stop()
        self.decision_maker.stop()
        logging.info("MazeController stopped")

if __name__ == "__main__":
    controller = MazeController(
        user_email=USER_EMAIL,
        player_id=PLAYER_ID,
        fresh_start=FRESH_START,
        cloud_pool=cloud_pool,
        local_pool=local_pool,
        mqtt_broker=MQTT_BROKER,
        mqtt_topic=MQTT_TOPIC
    )
    controller.start()