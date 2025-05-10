from time import sleep
from graph import Graph
from datetime import datetime, timedelta
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
        self.marsami_last_timestamp = {}  # Dictionary to track the last timestamp for each Marsami
        self.valid_rooms = set()  # Set to store valid room IDs
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
                    self.valid_rooms.add(room_id)  # Add to valid rooms set
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

    def _load_initial_state(self):
        if self.fresh_start:
            conn = self.cloud_pool.get_connection()
            try:
                if not conn.is_connected():
                    logging.info("Reconnecting to cloud database")
                    conn.reconnect(attempts=3, delay=1)
                if not conn.is_connected():
                    raise Exception("Failed to reconnect to the database")
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("SELECT numbermarsamis FROM setupmaze LIMIT 1")
                    result = cursor.fetchone()
                    self.number_marsamis = result['numbermarsamis'] if result else 0
            except Exception as e:
                logging.error(f"Error loading initial state: {e}")
            finally:
                conn.close()
        else:
            self.game_id = self._fetch_latest_game_id()
            if not self.game_id:
                logging.warning("No existing game found, treating as fresh start.")
                self.fresh_start = True
                return

            conn = self.cloud_pool.get_connection()
            try:
                if not conn.is_connected():
                    logging.info("Reconnecting to cloud database")
                    conn.reconnect(attempts=3, delay=1)
                if not conn.is_connected():
                    raise Exception("Failed to reconnect to the database")
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("SELECT numbermarsamis FROM setupmaze LIMIT 1")
                    result = cursor.fetchone()
                    self.number_marsamis = result['numbermarsamis'] if result else 0
                
                    cursor1 = self.local_pool.get_connection().cursor(dictionary=True)
                    cursor1.execute("""
                    SELECT m1.marsami, m1.salaDestino, m1.status, m1.hora
                    FROM MedicaoPassagem m1
                    INNER JOIN (
                        SELECT marsami, MAX(hora) as max_hora
                        FROM MedicaoPassagem
                        WHERE idJogo = %s AND marsami BETWEEN 1 AND %s
                        GROUP BY marsami
                    ) m2 ON m1.marsami = m2.marsami AND m1.hora = m2.max_hora
                    WHERE m1.idJogo = %s
                    """, (self.game_id, self.number_marsamis, self.game_id))
                    latest_movements = cursor1.fetchall()

                    self.marsami_positions = {movement['marsami']: (movement['salaDestino'], movement['status']) for movement in latest_movements}
                    self.marsami_last_timestamp = {movement['marsami']: movement['hora'].timestamp() for movement in latest_movements}

                    room_counts = {room_id: {'odds': 0, 'evens': 0} for room_id in self.graph.rooms}
                    for movement in latest_movements:
                        marsami = movement['marsami']
                        room_id = movement['salaDestino']
                        if room_id in self.valid_rooms:
                            if marsami % 2 == 0:
                                room_counts[room_id]['evens'] += 1
                            else:
                                room_counts[room_id]['odds'] += 1
                    for room_id, counts in room_counts.items():
                        self.graph.update_room(room_id, counts['odds'], counts['evens'], 0)
            except Exception as e:
                logging.error(f"Error loading initial state: {e}")
            finally:
                conn.close()

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
                    event_time = hora  # Keep as datetime object, not string
                   
                    current_time = datetime.now() + timedelta(minutes=60)
                    delay = current_time - event_time  # Now both are datetime objects
                    delay = delay.total_seconds()

                   

                    if marsami in self.marsami_last_timestamp and event_time < self.marsami_last_timestamp[marsami]:
                        logging.info(f"Discarding movement for Marsami {marsami} with earlier timestamp {hora}")
                        return

                # Update the last timestamp for this Marsami
                    self.marsami_last_timestamp[marsami] = event_time

                    with self.event_delays_lock:
                        self.event_delays.append(delay)
                        if len(self.event_delays) > 100:
                            self.event_delays.pop(0)

                    with self.graph.lock:
                        old_room = self.marsami_positions.get(marsami, (None, None))[0] if marsami in self.marsami_positions else None
                        if old_room and old_room in self.valid_rooms:
                            old_state = self.graph.get_room_state(old_room)
                            if marsami % 2 == 0:
                                old_state['evens'] = max(0, old_state['evens'] - 1)
                            else:
                                old_state['odds'] = max(0, old_state['odds'] - 1)
                            self.graph.update_room(old_room, old_state['odds'], old_state['evens'], old_state['points'])
                            logging.info(f"At {hora.strftime('%Y-%m-%d %H:%M:%S')}: Marsami {marsami} removed from room {old_room}")
                        self.marsami_positions[marsami] = (new_room, status)
                        if new_room in self.valid_rooms:
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
