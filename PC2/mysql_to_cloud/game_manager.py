import os
import time
import logging
import mysql.connector
from mysql.connector import pooling, Error as MySQLError
import subprocess
import signal
import sys

# Logging setup
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables for local MySQL
LOCAL_MYSQL_HOST = os.getenv('LOCAL_MYSQL_HOST', 'mysql')
LOCAL_MYSQL_USER = os.getenv('LOCAL_MYSQL_USER', 'labuser')
LOCAL_MYSQL_PASSWORD = os.getenv('LOCAL_MYSQL_PASSWORD', 'labpass')
LOCAL_MYSQL_DATABASE = os.getenv('LOCAL_MYSQL_DATABASE', 'mydb')

# Check if all required env vars are set
required_vars = [LOCAL_MYSQL_HOST, LOCAL_MYSQL_USER, LOCAL_MYSQL_PASSWORD, LOCAL_MYSQL_DATABASE]
if any(var is None for var in required_vars):
    logger.error("Missing required environment variables for local MySQL connection: %s", required_vars)
    sys.exit(1)

# Dictionary to keep track of running subprocesses
running_subprocesses = {}

# Graceful shutdown flag
shutdown_flag = False

# Signal handler for graceful shutdown
def signal_handler(signum, frame):
    global shutdown_flag
    logger.info("Received termination signal. Shutting down gracefully...")
    shutdown_flag = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Create a connection pool
try:
    db_pool = pooling.MySQLConnectionPool(
        pool_name="game_manager_pool",
        pool_size=5,
        host=LOCAL_MYSQL_HOST,
        user=LOCAL_MYSQL_USER,
        password=LOCAL_MYSQL_PASSWORD,
        database=LOCAL_MYSQL_DATABASE
    )
    logger.info("Successfully created MySQL connection pool")
except MySQLError as e:
    logger.error("Failed to create MySQL connection pool: %s", e)
    sys.exit(1)

def get_active_games():
    """Fetch active games with estado = 1 from the database."""
    try:
        with db_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT J.idJogo, U.email, U.grupo
                FROM Jogo J
                JOIN Users U ON J.email = U.email
                WHERE J.estado = 1 AND U.tipo = 'player'
                """
                cursor.execute(query)
                return cursor.fetchall()
    except MySQLError as e:
        logger.error("MySQL error in get_active_games: %s", e)
        return []

def manage_subprocesses():
    """Launch and terminate subprocesses based on active games."""
    global running_subprocesses
    logger.debug("Checking for active games with estado = 1")
    active_games = get_active_games()
    active_game_ids = set()

    for game in active_games:
        idJogo, email, grupo = game
        active_game_ids.add(idJogo)
        if idJogo not in running_subprocesses:
            # Launch mongodb_to_mysql.py subprocess
            env_mongo = os.environ.copy()
            env_mongo['GAME_ID'] = str(idJogo)
            env_mongo['PLAYER_ID'] = str(grupo)
            try:
                proc_mongo = subprocess.Popen(['python', './mongodb_to_mysql.py'], env=env_mongo)
                logger.info(f"Launched mongodb_to_mysql.py for GAME_ID={idJogo}, PLAYER_ID={grupo}")
            except FileNotFoundError as e:
                logger.error(f"Cannot find mongodb_to_mysql.py: %s", e)
                continue
            except Exception as e:
                logger.error(f"Failed to launch mongodb_to_mysql.py for GAME_ID={idJogo}: %s", e)
                continue

            # Launch maze_controller.py subprocess
            env_maze = os.environ.copy()
            env_maze['USER_EMAIL'] = email
            env_maze['PLAYER_ID'] = str(grupo)
            env_maze['FRESH_START'] = 'True'  # Adjust based on game logic if needed
            env_maze['MQTT_BROKER'] = os.getenv('MQTT_BROKER', 'test.mosquitto.org')
            env_maze['MQTT_TOPIC'] = os.getenv('MQTT_TOPIC', 'pisid_mazeact')
            try:
                proc_maze = subprocess.Popen(['python', './maze_controller.py'], env=env_maze)
                logger.info(f"Launched maze_controller.py for GAME_ID={idJogo}, USER_EMAIL={email}, PLAYER_ID={grupo}")
            except FileNotFoundError as e:
                logger.error(f"Cannot find maze_controller.py: %s", e)
                continue
            except Exception as e:
                logger.error(f"Failed to launch maze_controller.py for GAME_ID={idJogo}: {e}")
                continue

            # Store both processes
            running_subprocesses[idJogo] = {'mongo': proc_mongo, 'maze': proc_maze}

    # Check for games that are no longer active
    for idJogo in list(running_subprocesses.keys()):
        if idJogo not in active_game_ids:
            processes = running_subprocesses.pop(idJogo)
            for proc_name, proc in processes.items():
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                    logger.info(f"Terminated {proc_name} for GAME_ID={idJogo}")
                except subprocess.TimeoutExpired:
                    logger.warning(f"{proc_name} for GAME_ID={idJogo} did not terminate in time. Killing process.")
                    proc.kill()
                except Exception as e:
                    logger.error(f"Error terminating {proc_name} for GAME_ID={idJogo}: {e}")

def main():
    """Main loop to manage game subprocesses every 5 seconds."""
    global shutdown_flag
    while not shutdown_flag:
        manage_subprocesses()
        time.sleep(5)  # Check every 5 seconds

    # Shutdown all subprocesses
    for idJogo, processes in running_subprocesses.items():
        for proc_name, proc in processes.items():
            try:
                proc.terminate()
                proc.wait(timeout=5)
                logger.info(f"Terminated {proc_name} for GAME_ID={idJogo} during shutdown")
            except subprocess.TimeoutExpired:
                logger.warning(f"{proc_name} for GAME_ID={idJogo} did not terminate in time. Killing process.")
                proc.kill()
            except Exception as e:
                logger.error(f"Error terminating {proc_name} for GAME_ID={idJogo} during shutdown: {e}")

if __name__ == "__main__":
    main()