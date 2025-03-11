
import threading
import time
import json
import os
from pymongo import MongoClient
import mysql.connector

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/labirinto")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["labirinto"]
messages_col = db["all_messages"]

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_USER = os.getenv("MYSQL_USER", "labuser")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "labpass")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "labirinto_mysql")

mysql_conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    autocommit=False
)
mysql_cursor = mysql_conn.cursor()

def process_batch(docs):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            mysql_conn.start_transaction()
            for doc in docs:
                topic = doc.get("topic", "")
                data = doc.get("data", {})
                ts = doc["timestamp"]  # float

                if "mazemov" in topic:
                    # Movement => Insert into MedicoesPassagens
                    # Expecting: {Player, Marsami, RoomOrigin, RoomDestiny, Status}
                    RoomOrigin = data.get("RoomOrigin", 0)
                    RoomDestiny = data.get("RoomDestiny", 0)
                    Marsami = data.get("Marsami", 0)
                    Status = data.get("Status", 0)

                    sql = """INSERT INTO MedicoesPassagens 
                             (Hora, SalaOrigem, SalaDestino, Marsami, Status) 
                             VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s)"""
                    mysql_cursor.execute(sql, (ts, RoomOrigin, RoomDestiny, Marsami, Status))

                elif "mazesound" in topic:
                    # Sound => Insert into Mensagens
                    # Expecting: {Player, Hour, Sound, ...}
                    # We'll store "Sound" as Leitura, "Player" as Sensor for example
                    sensor = data.get("Player", 0)
                    leitura = data.get("Sound", 0.0)

                    sql = """INSERT INTO Mensagens
                             (Hora, Sala, Sensor, Leitura, TipoArea, MsgExtra, HoraEscrita)
                             VALUES (FROM_UNIXTIME(%s), 0, %s, %s, 0, '', NOW())"""
                    mysql_cursor.execute(sql, (ts, sensor, leitura))

                else:
                    # If needed, handle other topics or ignore
                    pass

            mysql_conn.commit()
            # Mark as processed
            ids = [doc["_id"] for doc in docs]
            messages_col.update_many({"_id": {"$in": ids}}, {"$set": {"processed": True}})
            print(f"Committed {len(docs)} docs (attempt {attempt+1})")
            return True
        except mysql.connector.Error as err:
            mysql_conn.rollback()
            print(f"Error inserting batch: {err}, retry {attempt+1}/{max_retries}")
            time.sleep(1)
    print("Failed after max retries.")
    return False

def worker():
    while True:
        docs = list(messages_col.find({"processed": False}).limit(10))
        if docs:
            process_batch(docs)
        else:
            time.sleep(1)

if __name__ == "__main__":
    worker()

