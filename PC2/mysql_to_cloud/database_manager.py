from datetime import datetime
import time
import threading
from mysql.connector import Error as MySQLError
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseManager:
    def __init__(self, graph, player_email, fresh_start, cloud_pool, local_pool, decision_maker, decision_condition):
        self.graph = graph
        self.player_email = player_email
        self.fresh_start = fresh_start
        self.cloud_pool = cloud_pool
        self.local_pool = local_pool
        self.decision_maker = decision_maker
        self.decision_condition = decision_condition
        self.game_id = None
        self.number_marsamis = 0
        self.marsami_positions = {}
        self.marsami_last_timestamp = {}
        self.valid_rooms = set()
        self.running = True
        self.last_movement_hora = datetime.min
        self._load_maze_structure()
        self._load_initial_state()
        
        logging.info("DatabaseManager initialized")

    def _load_maze_structure(self):
        conn = self.cloud_pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT Rooma FROM corridor UNION SELECT DISTINCT Roomb FROM corridor")
                for (room_id,) in cursor.fetchall():
                    self.graph.add_room(room_id)
                    self.valid_rooms.add(room_id)
                cursor.execute("SELECT Rooma, Roomb FROM corridor")
                for room1, room2 in cursor.fetchall():
                    self.graph.add_corridor(room1, room2)
        except MySQLError as e:
            logging.error(f"Error loading maze structure: {e}")
        finally:
            conn.close()

    def _fetch_latest_game_id(self):
        conn = self.local_pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(idJogo) FROM Jogo WHERE email = %s", (self.player_email,))
                result = cursor.fetchone()
                return result[0] if result and result[0] else None
        except MySQLError as e:
            logging.error(f"Error fetching game ID: {e}")
            return None
        finally:
            conn.close()

    def _load_initial_state(self):
        if self.fresh_start:
            conn = self.cloud_pool.get_connection()
            try:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("SELECT numbermarsamis FROM setupmaze LIMIT 1")
                    result = cursor.fetchone()
                    self.number_marsamis = result['numbermarsamis'] if result else 0
            except MySQLError as e:
                logging.error(f"Error loading initial state: {e}")
            finally:
                conn.close()
        else:
            self.game_id = self._fetch_latest_game_id()
            if not self.game_id:
                self.fresh_start = True
                return
            cloud_conn = self.cloud_pool.get_connection()
            try:
                with cloud_conn.cursor(dictionary=True) as cursor:
                    cursor.execute("SELECT numbermarsamis FROM setupmaze LIMIT 1")
                    result = cursor.fetchone()
                    self.number_marsamis = result['numbermarsamis'] if result else 0
            except MySQLError as e:
                logging.error(f"Error loading numbermarsamis: {e}")
            finally:
                cloud_conn.close()

            local_conn = self.local_pool.get_connection()
            try:
                with local_conn.cursor(dictionary=True) as cursor1:
                    cursor1.execute("""
                        SELECT m1.marsami, m1.salaOrigem, m1.salaDestino, m1.status, m1.hora
                        FROM MedicaoPassagem m1
                        INNER JOIN (
                            SELECT marsami, MAX(hora) as max_hora
                            FROM MedicaoPassagem
                            WHERE idJogo = %s AND marsami BETWEEN 1 AND %s
                            GROUP BY marsami
                        ) m2 ON m1.marsami = m2.marsami AND m1.hora = m2.max_hora
                        WHERE m1.idJogo = %s
                    """, (self.game_id, self.number_marsamis, self.game_id))
                    movements = cursor1.fetchall()
                    self.marsami_positions = {m['marsami']: (m['salaDestino'], m['status']) for m in movements}
                    self.marsami_last_timestamp = {m['marsami']: m['hora'] for m in movements}
                    self.last_movement_hora = max(m['hora'] for m in movements) if movements else datetime.min
            except MySQLError as e:
                logging.error(f"Error loading state: {e}")
            finally:
                local_conn.close()
        logging.info(f"Initial state loaded: {self.marsami_positions}")

    def process_new_movements(self):
        logging.info("Starting process_new_movements loop")
        while self.running:
            start_time = time.time()
            conn = self.local_pool.get_connection()
            try:
                with conn.cursor(dictionary=True) as cursor:
                    logging.info(f"Fetching movements after {self.last_movement_hora}")
                    cursor.execute("""
                        SELECT m1.marsami, m1.salaOrigem, m1.salaDestino, m1.status, m1.hora
                        FROM MedicaoPassagem m1
                        INNER JOIN (
                            SELECT marsami, MAX(hora) as max_hora
                            FROM MedicaoPassagem
                            WHERE idJogo = %s AND hora > %s
                            GROUP BY marsami
                        ) m2 ON m1.marsami = m2.marsami AND m1.hora = m2.max_hora
                        WHERE m1.idJogo = %s
                    """, (self.game_id, self.last_movement_hora, self.game_id))
                    movements = cursor.fetchall()
                    logging.info(f"Fetched {len(movements)} movements")
                    if movements:
                        self._process_movement_batch(movements)
                        self.last_movement_hora = max(m['hora'] for m in movements)
                        logging.info(f"Updated last_movement_hora to {self.last_movement_hora}")
                        with self.decision_condition:
                            self.decision_condition.notify_all()
            except MySQLError as e:
                logging.error(f"Error processing movements: {e}")
            finally:
                conn.close()
            elapsed = time.time() - start_time
            sleep_time = max(0.25 - elapsed, 0)
            time.sleep(sleep_time)
            
    def _process_movement_batch(self, movements):
        room_changes = {room_id: {'odds': 0, 'evens': 0} for room_id in self.graph.rooms}
        with self.graph.lock:
            for m in movements:
                marsami = m['marsami']
                salaOrigem = m['salaOrigem']
                new_room = m['salaDestino']
                status = m['status']
                hora = m['hora']
                if marsami in self.marsami_last_timestamp and hora <= self.marsami_last_timestamp[marsami]:
                    logging.info(f"Skipping marsami {marsami}: hora {hora} <= last {self.marsami_last_timestamp[marsami]}")
                    continue
                logging.info(f"Processing marsami {marsami} to room {new_room}")
                self.marsami_last_timestamp[marsami] = hora
                old_room = self.marsami_positions.get(marsami, (None, None))[0]
                if old_room and old_room in self.valid_rooms:
                    if marsami % 2 == 0:
                        room_changes[old_room]['evens'] -= 1
                    else:
                        room_changes[old_room]['odds'] -= 1
                self.marsami_positions[marsami] = (new_room, status)
                if new_room in self.valid_rooms:
                    if marsami % 2 == 0:
                        room_changes[new_room]['evens'] += 1
                    else:
                        room_changes[new_room]['odds'] += 1
            for room_id, changes in room_changes.items():
                if changes['odds'] or changes['evens']:
                    state = self.graph.get_room_state(room_id)
                    new_odds = max(0, state['odds'] + changes['odds'])
                    new_evens = max(0, state['evens'] + changes['evens'])
                    self.graph.update_room(room_id, new_odds, new_evens, state['points'])

    def stop(self):
        self.running = False
        logging.info("DatabaseManager stopped")