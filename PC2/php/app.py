#!/usr/bin/env python3
# app.py

from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text, Column, String, Integer, Float, TIMESTAMP, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pymysql
from datetime import datetime

Base = declarative_base()

# ------------------ DATABASE CONFIG ------------------ #
DATABASE_URL = "mysql+pymysql://root:rootpass@mysql:3306/labirinto_mysql"
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# ------------------ MODELS ------------------ #
class User(Base):
    __tablename__ = 'User'
    email = Column(String(50), primary_key=True)
    nome = Column(String(100))
    telemovel = Column(String(12))
    tipo = Column(String(3))
    grupo = Column(Integer)

class OcupacaoLabirinto(Base):
    __tablename__ = 'OcupacaoLabirinto'
    sala = Column(Integer, primary_key=True)
    numeroMarsamiOdd = Column(Integer)
    numeroMarsamiEven = Column(String(45))
    score = Column(Integer)
    idJogo = Column(Integer)

# ------------------ FLASK APP ------------------ #
app = Flask(__name__)

def db_connect(db_name, username, password):
    return create_engine(
        f"mysql+pymysql://{username}:{password}@mysql:3306/{db_name}",
        pool_recycle=3600
    )

# ================================ ROUTES ================================ #

@app.route("/scripts/php/abrirPorta.php", methods=["POST"])
def open_door():
    try:
        username = request.form["username"]
        password = request.form["password"]
        origin = request.form["SalaOrigemController"]
        destiny = request.form["SalaDestinoController"]

        engine = db_connect("labirinto_mysql", username, password)
        with engine.connect() as conn:
            conn.execute(text(f"CALL openDoor({origin}, {destiny})"))
        return jsonify(True), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/scripts/php/abrirTodasPortas.php", methods=["POST"])
def open_all_doors():
    try:
        username = request.form["username"]
        password = request.form["password"]

        engine = db_connect("labirinto_mysql", username, password)
        with engine.connect() as conn:
            conn.execute(text("CALL startGame()"))
        return jsonify(True), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/scripts/php/fecharPorta.php", methods=["POST"])
def close_door():
    try:
        username = request.form["username"]
        password = request.form["password"]
        origin = request.form["SalaOrigemController"]
        destiny = request.form["SalaDestinoController"]

        engine = db_connect("labirinto_mysql", username, password)
        with engine.connect() as conn:
            conn.execute(text(f"CALL closeDoor({origin}, {destiny})"))
        return jsonify(True), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/scripts/php/fecharTodasPortas.php", methods=["POST"])
def close_all_doors():
    try:
        username = request.form["username"]
        password = request.form["password"]

        engine = db_connect("labirinto_mysql", username, password)
        with engine.connect() as conn:
            conn.execute(text("CALL closeAllDoor()"))
        return jsonify(True), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/scripts/php/getMsgs.php", methods=["POST"])
def get_msgs():
    try:
        username = request.form["username"]
        password = request.form["password"]

        engine = db_connect("labirinto_mysql", username, password)
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    m.mensagem AS Msg,
                    m.leitura AS Leitura,
                    m.sensor AS Sensor,
                    m.tipoAlerta AS TipoMensagensa,
                    m.hora AS Hora,
                    m.horaEscrita AS HoraEscrita,
                    NULL AS Sala
                FROM Mensagens m
                ORDER BY m.hora DESC
                LIMIT 50
            """)
            result = conn.execute(query)

            mensagens = []
            for row in result:
                mensagens.append({
                    "Msg": row[0],
                    "Leitura": row[1],
                    "Sensor": row[2],
                    "TipoMensagensa": row[3],
                    "Hora": row[4].isoformat() if row[4] else None,
                    "HoraEscrita": row[5].isoformat() if row[5] else None,
                    "Sala": row[6]
                })

        return jsonify({"mensagens": mensagens}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


        
@app.route("/scripts/php/getSensors.php", methods=["POST"])
def get_sensors():
    try:
        # Retrieving form data
        username = request.form["username"]
        password = request.form["password"]
        sensor_id = int(request.form["sensor"])

        # Connect to the database
        engine = db_connect("labirinto_mysql", username, password)
        with engine.connect() as conn:
            # Adjusting the query to work with the actual schema
            query = text("""
                SELECT s.hora AS Hour, s.Soundcol AS Sound, o.sala AS Room, s.idSound AS Sensor, 
                    m.tipoAlerta AS MessageType, m.horaEscrita AS WrittenHour
                FROM Sound s
                LEFT JOIN Mensagens m ON s.idSound = m.sensor
                LEFT JOIN OcupacaoLabirinto o ON o.sala = s.idSound  -- Adjust this according to how `sala` and `sensor_id` relate
                WHERE s.idSound = :sensor_id
                ORDER BY s.hora DESC
            """)
            # Execute the query with the sensor_id parameter
            result = conn.execute(query, {"sensor_id": sensor_id})

            # Convert the result to a list of dictionaries
            sensors = []
            for row in result:
                sensors.append({
                    "Hora": row[0].isoformat() if row[0] else None,  # Assuming row[0] is 'Hour'
                    "Sound": row[1],  # Assuming row[1] is 'Sound'
                    "Room": row[2],   # Assuming row[2] is 'Room'
                    "Sensor": row[3], # Assuming row[3] is 'Sensor'
                    "TipoMensagensa": row[4], # Assuming row[4] is 'MessageType'
                    "HoraEscrita": row[5].isoformat() if row[5] else None, # Assuming row[5] is 'WrittenHour'
                })


        # Return the result as JSON
        return jsonify({"mensagens": sensors}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/scripts/php/iniciarJogo.php", methods=["POST"])
def start_game():
    try:
        username = request.form["username"]
        password = request.form["password"]

        engine = db_connect("labirinto_mysql", username, password)
        with engine.connect() as conn:
            conn.execute(text("CALL startGame()"))
        return jsonify(True), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/scripts/php/getMarsamRoom.php", methods=["POST"])
def get_marsam_data():
    session = SessionLocal()
    try:
        max_id = session.query(func.max(OcupacaoLabirinto.idJogo)).scalar()
        if max_id is None:
            return jsonify([]), 200

        rows = (
            session.query(OcupacaoLabirinto)
            .filter(OcupacaoLabirinto.idJogo == max_id)
            .order_by(OcupacaoLabirinto.sala)
            .all()
        )

        payload = [
            {
                "Sala": row.sala,
                "NumeroMarsamisOdd": row.numeroMarsamiOdd,
                "NumeroMarsamisEven": row.numeroMarsamiEven,
                "Score": row.score,
                "IDJogo": row.idJogo
            }
            for row in rows
        ]

        return jsonify(payload), 200

    except Exception as e:
        app.logger.error(f"DB error: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

@app.route("/scripts/php/obterPontuacao.php", methods=["POST"])
def get_points():
    try:
        username = request.form["username"]
        password = request.form["password"]

        engine = db_connect("labirinto_mysql", username, password)
        with engine.connect() as conn:
            result = conn.execute(text("CALL getPoints()"))
            points = [dict(row) for row in result.mappings()]
        return jsonify(points), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/scripts/php/validateLogin.php", methods=["POST"])
def validate_login():
    return_msg = {"success": False, "message": ""}
    try:
        username = request.form["username"]
        password = request.form["password"]

        engine = db_connect("labirinto_mysql", username, password)
        with engine.connect() as conn:
            return_msg["success"] = True
        return jsonify(return_msg), 200
    except Exception:
        return_msg["message"] = "The login failed. Check if the user exists in the database."
        return jsonify(return_msg), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
