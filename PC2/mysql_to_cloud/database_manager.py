from time import sleep
from graph import Graph
from datetime import datetime
import time
import logging

class DatabaseManager:
    def __init__(self, graph, player_id, fresh_start, cloud_pool, local_pool, decision_maker, event_delays, event_delays_lock):
        self.graph = graph
        self.player_id = player_id
        self.fresh_start = fresh_start
        self.cloud_pool = cloud_pool
        self.local_pool = local_pool
        self.decision_maker = decision_maker
        self.event_delays = event_delays
        self.event_delays_lock = event_delays_lock
        self.game_id = None
        self.number_marsamis = None
        self.marsami_positions = {}
        self.running = True
        self._load_maze_structure()
        self._load_initial_state()

    def _load_maze_structure(self):
        conn = self.cloud_pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT Rooma FROM corridor UNION SELECT DISTINCT Roomb FROM corridor")
                for (room_id,) in cursor.fetchall():
                    self.graph.add_room(room_id)
                cursor.execute("SELECT Rooma, Roomb FROM corridor")
                for room1, room2 in cursor.fetchall():
                    self.graph.add_corridor(room1, room2)  # Directed corridor: Rooma -> Roomb
        except Exception as e:
            logging.error(f"Error loading maze structure: {e}")
        finally:
            conn.close()

    def _fetch_latest_game_id(self):
        conn = self.local_pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(idJogo) as latest_game FROM MedicaoPassagem WHERE idJogo IN "
                              "(SELECT idJogo FROM Jogo WHERE jogador = %s)", (self.player_id,))
                result = cursor.fetchone()
                return result[0] if result and result[0] else None
        except Exception as e:
            logging.error(f"Error fetching latest game ID: {e}")
            return None
        finally:
            conn.close()

    def load_cloud_data(self):
        """Load setup data from the cloud database."""
        conn = self.cloud_pool.get_connection()
        try:
            if not conn.is_connected():
                logging.info("Reconnecting to cloud database")
                conn.reconnect(attempts=3, delay=1)
            if not conn.is_connected():
                raise Exception("Failed to reconnect to the cloud database")
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT numbermarsamis FROM setupmaze LIMIT 1")
                result = cursor.fetchone()
                self.number_marsamis = result['numbermarsamis'] if result else 0
        except Exception as e:
            logging.error(f"Error loading cloud data: {e}")
        finally:
            conn.close()

    def load_local_data(self):
        """Load game-specific data from the local database."""
        self.game_id = self._fetch_latest_game_id()
        if not self.game_id:
            logging.warning("No existing game found, treating as fresh start.")
            self.fresh_start = True
            return

        conn = self.local_pool.get_connection()
        try:
            if not conn.is_connected():
                logging.info("Reconnecting to local database")
                conn.reconnect(attempts=3, delay=1)
            if not conn.is_connected():
                raise Exception("Failed to reconnect to the local database")
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT m1.marsami, m1.salaDestino, m1.status
                    FROM MedicaoPassagem m1
                    INNER JOIN (
                        SELECT marsami, MAX(hora) as max_hora
                        FROM MedicaoPassagem
                        WHERE idJogo = %s AND marsami BETWEEN 1 AND %s
                        GROUP BY marsami
                    ) m2 ON m1.marsami = m2.marsami AND m1.hora = m2.max_hora
                    WHERE m1.idJogo = %s
                """, (self.game_id, self.number_marsamis, self.game_id))
                latest_movements = cursor.fetchall()

                self.marsami_positions = {movement['marsami']: (movement['salaDestino'], movement['status']) for movement in latest_movements}

                room_counts = {room_id: {'odds': 0, 'evens': 0} for room_id in self.graph.rooms}
                for movement in latest_movements:
                    marsami = movement['marsami']
                    room_id = movement['salaDestino']
                    if marsami % 2 == 0:
                        room_counts[room_id]['evens'] += 1
                    else:
                        room_counts[room_id]['odds'] += 1
                for room_id, counts in room_counts.items():
                    self.graph.update_room(room_id, counts['odds'], counts['evens'], 0)
        except Exception as e:
            logging.error(f"Error loading local data: {e}")
        finally:
            conn.close()

    def _load_initial_state(self):
        """Load initial state by calling appropriate data loading functions."""
        if self.fresh_start:
            self.load_cloud_data()
        else:
            self.load_cloud_data()  # Load number_marsamis from cloud
            self.load_local_data()  # Then load game-specific data from local

    def process_movement(self, mysqlID):
        conn = self.local_pool.get_connection()
        try:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT marsami, salaDestino, status, hora FROM MedicaoPassagem WHERE idMedicao = %s", (mysqlID,))
                result = cursor.fetchone()
                if result:
                    marsami = result['marsami']
                    new_room = result['salaDestino']
                    status = result['status']
                    hora = result['hora']
                    current_time = time.time()
                    event_time = hora.timestamp()
                    delay = max(0, current_time - event_time)  # Prevent negative delays
                    with self.event_delays_lock:
                        self.event_delays.append(delay)
                        if len(self.event_delays) > 100:
                            self.event_delays.pop(0)
                    with self.graph.lock:
                        old_room = self.marsami_positions.get(marsami, (None, None))[0] if marsami in self.marsami_positions else None
                        if old_room:
                            old_state = self.graph.get_room_state(old_room)
                            if marsami % 2 == 0:
                                old_state['evens'] -= 1
                            else:
                                old_state['odds'] -= 1
                            self.graph.update_room(old_room, old_state['odds'], old_state['evens'], old_state['points'])
                            logging.info(f"At {hora.strftime('%Y-%m-%d %H:%M:%S')}: Marsami {marsami} removed from room {old_room}")
                        self.marsami_positions[marsami] = (new_room, status)
                        new_state = self.graph.get_room_state(new_room)
                        if marsami % 2 == 0:
                            new_state['evens'] += 1
                        else:
                            new_state['odds'] += 1
                        self.graph.update_room(new_room, new_state['odds'], new_state['evens'], new_state['points'], marsami, True)
                    
                    self.decision_maker.update_marsami_position(marsami, new_room, status)
        except Exception as e:
            logging.error(f"Error processing movement {mysqlID}: {e}")
        finally:
            conn.close()

    def update_room_states(self):
        while self.running:
            time.sleep(60)
            if not self.running:
                break

    def stop(self):
        self.running = False
        logging.info("DatabaseManager shut down gracefully")
