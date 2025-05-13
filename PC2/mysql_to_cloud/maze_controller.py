import threading
import logging
from graph import Graph
from database_manager import DatabaseManager
from decision_maker import DecisionMaker

class MazeController:
    def __init__(self, user_email, player_id, fresh_start, cloud_pool, local_pool, mqtt_broker, mqtt_topic):
        # Initialize core components
        self.USER_EMAIL = user_email
        self.PLAYER_ID = player_id
        self.FRESH_START = fresh_start
        self.cloud_pool = cloud_pool
        self.local_pool = local_pool
        self.MQTT_BROKER = mqtt_broker
        self.MQTT_TOPIC = mqtt_topic
        
        # Set up the maze graph and synchronization
        self.graph = Graph()
        self.decision_condition = threading.Condition()
        
        # Instantiate simplified DatabaseManager and DecisionMaker
        self.db_manager = DatabaseManager(
            self.graph, self.USER_EMAIL, self.FRESH_START, self.cloud_pool, self.local_pool,
            self.decision_condition
        )
        self.decision_maker = DecisionMaker(
            self.graph, self.PLAYER_ID, self.cloud_pool, self.MQTT_BROKER, self.MQTT_TOPIC
        )
        self.running = True

    def start(self):
        """Start the maze controller."""
        # Start movement processing in a background thread
        movement_thread = threading.Thread(target=self.db_manager.process_new_movements, daemon=True)
        movement_thread.start()
        
        # Run the decision maker in the main thread
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
    # Example initialization (replace with actual values)
    controller = MazeController(
        user_email="user@example.com",
        player_id="player1",
        fresh_start=True,
        cloud_pool=None,  # Replace with actual pool
        local_pool=None,  # Replace with actual pool
        mqtt_broker="broker.example.com",
        mqtt_topic="maze/control"
    )
    controller.start()