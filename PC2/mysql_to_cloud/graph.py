import threading
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Graph:
    def __init__(self):
        self.rooms = {}
        self.adjacency = {}  # Directed edges: room1 -> [room2, room3, ...]
        self.door_states = {}  # Directed doors: (room1, room2) -> state
        self.last_movement_time = {}
        self.lock = threading.RLock()

    def add_room(self, room_id):
        with self.lock:
            if room_id not in self.rooms:
                self.rooms[room_id] = {'odds': 0, 'evens': 0, 'points': 0}
                self.adjacency[room_id] = []
                self.last_movement_time[room_id] = None

    def add_corridor(self, room1, room2):
        with self.lock:
            if room1 in self.rooms and room2 in self.rooms:
                self.adjacency[room1].append(room2)  # Directed edge: room1 -> room2
                key = (room1, room2)  # Ordered tuple for directed door
                self.door_states[key] = False
            else:
                logging.error(f"Cannot add corridor: Room {room1} or {room2} does not exist")

    def update_room(self, room_id, odds, evens, points, marsami=None, is_new_room=False):
        with self.lock:
            if room_id in self.rooms:
                self.rooms[room_id].update({'odds': odds, 'evens': evens, 'points': points})
                if marsami is not None and is_new_room:
                    logging.info(f"Marsami {marsami} moved to room {room_id} (Graph updated: odds={odds}, evens={evens})")
                self.last_movement_time[room_id] = datetime.now()

    def get_room_state(self, room_id):
        with self.lock:
            return self.rooms.get(room_id, {'odds': 0, 'evens': 0, 'points': 0})

    def get_door_state(self, room1, room2):
        with self.lock:
            key = (room1, room2)
            return self.door_states.get(key, False)

    def set_door_state(self, room1, room2, state):
        with self.lock:
            key = (room1, room2)
            if key in self.door_states:
                self.door_states[key] = state
                logging.info(f"Door state updated: {room1} -> {room2} set to {'open' if state else 'closed'}")
