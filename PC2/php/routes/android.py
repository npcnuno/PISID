# app.py
import os

import mysql.connector
from flask import Blueprint, request, jsonify, session, abort
from dotenv import load_dotenv

android = Blueprint('android', __name__)

load_dotenv()
def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="rootpass",
        database="mydb"
    )
def run_stored_procedure(proc_name, args=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.callproc(proc_name, args or [])
        conn.commit()
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        cursor.close()
        conn.close()


@android.route("/abrirPorta.php", methods=["POST"])
def open_door():
    success, error = run_stored_procedure(
        "openDoor",
        [request.form["SalaOrigemController"], request.form["SalaDestinoController"]]
    )
    return (jsonify(True), 200) if success else (jsonify({"error": error}), 500)


@android.route("/abrirTodasPortas.php", methods=["POST"])
def open_all_doors():
    success, error = run_stored_procedure("startGame")
    return (jsonify(True), 200) if success else (jsonify({"error": error}), 500)


@android.route("/fecharPorta.php", methods=["POST"])
def close_door():
    success, error = run_stored_procedure(
        "closeDoor",
        [request.form["SalaOrigemController"], request.form["SalaDestinoController"]]
    )
    return (jsonify(True), 200) if success else (jsonify({"error": error}), 500)


@android.route("/fecharTodasPortas.php", methods=["POST"])
def close_all_doors():
    success, error = run_stored_procedure("closeAllDoor")
    return (jsonify(True), 200) if success else (jsonify({"error": error}), 500)


@android.route("/getMsgs.php", methods=["POST"])
def get_msgs():
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:

        cursor.execute("""
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
        mensagens = []
        for row in cursor:
            row["Hora"] = row["Hora"].isoformat() if row["Hora"] else None
            row["HoraEscrita"] = row["HoraEscrita"].isoformat() if row["HoraEscrita"] else None
            mensagens.append(row)
        return jsonify({"mensagens": mensagens}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@android.route("/getSensors.php", methods=["POST"])
def get_sensors():
    sensor_id = int(request.form["sensor"])
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:

        cursor.execute("""
            SELECT s.hora AS Hora, s.Soundcol AS Sound, o.sala AS Room, s.idSound AS Sensor, 
                m.tipoAlerta AS TipoMensagensa, m.horaEscrita AS HoraEscrita
            FROM Sound s
            LEFT JOIN Mensagens m ON s.idSound = m.sensor
            LEFT JOIN OcupacaoLabirinto o ON o.sala = s.idSound
            WHERE s.idSound = %s
            ORDER BY s.hora DESC
        """, (sensor_id,))
        rows = []
        for row in cursor:
            row["Hora"] = row["Hora"].isoformat() if row["Hora"] else None
            row["HoraEscrita"] = row["HoraEscrita"].isoformat() if row["HoraEscrita"] else None
            rows.append(row)
        return jsonify({"mensagens": rows}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@android.route("/iniciarJogo.php", methods=["POST"])
def start_game():
    success, error = run_stored_procedure("startGame")
    return (jsonify(True), 200) if success else (jsonify({"error": error}), 500)


@android.route("/getMarsamRoom.php", methods=["POST"])
def get_marsam_data():
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:

        cursor.execute("SELECT MAX(idJogo) AS max_id FROM OcupacaoLabirinto")
        max_id = cursor.fetchone()["max_id"]
        if max_id is None:
            return jsonify([]), 200
        cursor.execute("""
            SELECT sala, numeroMarsamiOdd, numeroMarsamiEven, score, idJogo
            FROM OcupacaoLabirinto
            WHERE idJogo = %s
            ORDER BY sala
        """, (max_id,))
        data = cursor.fetchall()
        filtered_data = [row for row in data if row['sala'] != 0]

        # Convert to JSON
        json = jsonify(filtered_data)
        return json, 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@android.route("/obterPontuacao.php", methods=["POST"])
def get_points():
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.callproc("getPoints")
        points = []
        for result in cursor.stored_results():
            points.extend(result.fetchall())
        return jsonify(points), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@android.route("/validateLogin.php", methods=["POST"])
def validate_login():
    try:
        conn = get_db()
        conn.close()
        return jsonify({"success": True, "message": ""}), 200
    except Exception:
        return jsonify({
            "success": False,
            "message": "The login failed. Check if the user exists in the database."
        }), 200
