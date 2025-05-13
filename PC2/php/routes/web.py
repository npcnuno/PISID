import os
import subprocess

import mysql.connector
import pexpect
from mysql.connector import Error
from flask import Blueprint, request, redirect, url_for, session, render_template, Response, stream_with_context, abort
import json
import time
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
web = Blueprint('web', __name__)

load_dotenv()
def get_db():
    if "db_user" not in session or "db_pass" not in session:
        abort(401, description="Credenciais não encontradas na sessão")

    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=session["db_user"],
            password=session["db_pass"],
            database=os.getenv("DB_NAME", "mydb")
        )
        return connection
    except mysql.connector.Error as err:
        print(f"[DB ERROR] {err}")
        abort(503, description="Serviço indisponível, tente mais tarde.")

@web.route('/', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]

        try:
            conn = mysql.connector.connect(
                host="localhost",
                user=email,
                password=password,
                database="mydb"  # This must exist and be accessible
            )
            if conn.is_connected():
                session["db_user"] = email
                session["db_pass"] = password
                print(session)
                conn.close()
                return redirect(url_for("web.dashboard"))

        except Error:
            error = "Login inválido ou sem acesso ao banco de dados."
    return render_template("login.html", error=error)

@web.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    if 'db_user' not in session:
        return redirect(url_for('web.login'))

    db = get_db()
    cursor = db.cursor(dictionary=True)
    db_user = session["db_user"]

    if request.method == 'POST':
        descricao = request.form['descricao']
        try:
            cursor.execute("CALL Criar_jogo(%s)", (
                descricao,
            ))
            db.commit()
        except mysql.connector.Error as err:
            print("Erro ao criar jogo:", err)
            db.rollback()
        return redirect(url_for('web.dashboard'))
    query = "SELECT * FROM Jogo WHERE email LIKE %s"
    cursor.execute(query, (db_user + '@%',))
    jogos = cursor.fetchall()
    return render_template("dashboard.html", jogos=jogos)

@web.route('/edit/<int:idJogo>', methods=['GET', 'POST'])
def edit_game(idJogo):
    if 'db_user' not in session:
        return redirect(url_for('web.login'))

    db = get_db()
    cursor = db.cursor(dictionary=True)

    # Consulta o jogo para renderizar a tela (GET) ou validar antes da chamada da SP
    cursor.execute("SELECT * FROM Jogo WHERE idJogo = %s", (idJogo,))
    jogo = cursor.fetchone()
    if not jogo:
        return "Erro: Jogo não encontrado.", 404

    if request.method == 'POST':
        descricao = request.form['descricao']

        try:
            # Chamada à stored procedure
            cursor.callproc('Alterar_jogo', (idJogo, descricao))
            db.commit()
        except mysql.connector.Error as err:
            # Trata erros da SP (ex: SIGNALs personalizados)
            return f"Erro ao alterar o jogo: {err.msg}", 400

        return redirect(url_for('web.dashboard'))

    return render_template("edit.html", jogo=jogo)


@web.route('/start/<int:idJogo>')
def start_game(idJogo):
    if 'db_user' not in session:
        return redirect(url_for('web.login'))


    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT * FROM Jogo WHERE idJogo = %s", (idJogo,))
        jogo = cursor.fetchone()
    except:
        abort(503, description="Serviço indisponível, tente mais tarde.")


    if not jogo or jogo[5]:  # Assuming column 5 is dataHoraInicio
        return "Jogo já iniciado ou inválido."

    if not start_maze_player(33 ):
        abort(503, description="Serviço indisponível, tente mais tarde.")

    try:
        cursor.execute("CALL startGame(%s)", (idJogo,))
        db.commit()
    except:
        abort(503, description="Serviço indisponível, tente mais tarde.")

    def generate():
        yield "<h3>Jogo iniciado</h3><pre>"

        child = pexpect.spawn("wine ./mazerun.exe 33 1 5 test.mosquitto.org 1883")

        while True:
            try:
                line = child.readline()
                if not line:
                    break
                yield line
            except pexpect.exceptions.EOF:
                break
            except Exception as e:
                yield f"\n[ERROR] {e}\n"
                break

        yield "</pre><a href='/dashboard'>Voltar</a>"

    return Response(stream_with_context(generate()), mimetype='text/html')
@web.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('web.login'))




def start_maze_player(player_id: int, timeout: int = 15):
    broker = os.getenv("MQTT_BROKER", "test.mosquitto.org")
    port = int(os.getenv("MQTT_PORT", 1883))
    topic = os.getenv("MQTT_TOPIC_MAIN", "pisid_maze/mazeManager")
    topic_ack = os.getenv("MQTT_TOPIC_ACK", "pisid_maze/mazeManager_ack")
    ack_received = {"status": False}

    def on_connect(client, userdata, flags, rc):
        print("🔌 Connected with result code", rc)
        client.subscribe(topic_ack)

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            print("Message received:", payload)
            if (
                isinstance(payload, dict)
                and payload.get("player_id") == str(player_id)
                and payload.get("status") in ["started", "closed"]
            ):
                print("✅ Acknowledgment received:", payload)
                ack_received["status"] = True
                client.disconnect()
        except Exception as e:
            print("❌ Error processing message:", e)

    # Create and configure a single MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port, 60)

    client.loop_start()  # Start the loop in a background thread

    # Build and send the start message
    start_message = {
        "action": "start",
        "player_id": player_id,
        "mqtt_config": {
            "broker_url": broker,
            "port": port,
            "topics": [
                {
                    "topic": f"pisid_mazemov_{player_id}",
                    "schema": {
                        "Player": {"type": "int", "required": True},
                        "Marsami": {"type": "int", "required": True},
                        "RoomOrigin": {"type": "int", "required": True},
                        "RoomDestiny": {"type": "int", "required": True},
                        "Status": {"type": "int", "required": True},
                        "timestamp": {"type": "datetime", "required": False, "default": "now"},
                    },
                    "collection": "move_messages",
                    "processed_topic": f"pisid_mazemov_{player_id}_processed",
                    "confirmed_topic": f"pisid_mazemov_{player_id}_confirmed",
                },
                {
                    "topic": f"pisid_mazesound_{player_id}",
                    "schema": {
                        "Player": {"type": "int", "required": True},
                        "Sound": {"type": "float", "required": True},
                        "Hour": {"type": "datetime", "required": True},
                        "timestamp": {"type": "datetime", "required": False, "default": "now"},
                    },
                    "collection": "sound_messages",
                    "processed_topic": f"pisid_mazesound_{player_id}_processed",
                    "confirmed_topic": f"pisid_mazesound_{player_id}_confirmed",
                },
            ],
        },
    }
    time.sleep(2)
    client.publish(topic, json.dumps(start_message))
    print(f"Sent 'start' message for player {player_id}, waiting for acknowledgment...")

    # Wait for acknowledgment or timeout
    deadline = time.time() + timeout
    while not ack_received["status"] and time.time() < deadline:
        time.sleep(0.1)

    client.loop_stop()

    if ack_received["status"]:
        print("✅ Acknowledgment received in time.")
        return True
    else:
        print("⏰ No acknowledgment received within timeout.")
        return False
