import subprocess

import mysql.connector
import pexpect
from mysql.connector import Error
from flask import Blueprint, request, redirect, url_for, session, render_template, Response, stream_with_context

web = Blueprint('web', __name__)

def get_db():
    if "db_user" not in session or "db_pass" not in session:
        raise Exception("Database credentials not in session")
    return mysql.connector.connect(
        host="localhost",
        user=session["db_user"],
        password=session["db_pass"],
        database="mydb"  # Replace with your actual DB name
    )

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
@web.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    if 'db_user' not in session:
        return redirect(url_for(''))

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

    cursor.execute("SELECT * FROM Jogo  ", ())
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

    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM Jogo WHERE idJogo = %s", (idJogo,))
    jogo = cursor.fetchone()

    if not jogo or jogo[5]:  # Assuming column 5 is dataHoraInicio
        return "Jogo já iniciado ou inválido."

    cursor.execute("CALL startGame(%s)", (idJogo,))
    db.commit()

    def generate():
        yield "<h3>Jogo iniciado</h3><pre>"

        child = pexpect.spawn("wine ./mazerun.exe 33")

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
