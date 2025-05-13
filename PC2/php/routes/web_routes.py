from flask import render_template, request, redirect, url_for, session, Blueprint
import mysql.connector
from mysql.connector import Error

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
def dashboard():
    if 'db_user' not in session:
        return redirect(url_for(''))

    db = get_db()
    cursor = db.cursor(dictionary=True)
    db_user = session["db_user"]

    if request.method == 'POST':
        descricao = request.form['descricao']
        cursor.execute("""
            INSERT INTO Jogo (email, descricao, jogador, scoreTotal, dataHoraInicio)
            VALUES (%s, %s, %s, 0, NULL)
        """, (db_user, descricao, session['nome']))
        db.commit()

    cursor.execute("SELECT * FROM Jogo WHERE email = %s", (db_user,))
    jogos = cursor.fetchall()
    return render_template("dashboard.html", jogos=jogos)

@web.route('/edit/<int:idJogo>', methods=['GET', 'POST'])
def edit_game(idJogo):
    if 'email' not in session:
        return redirect(url_for('login'))

    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM Jogo WHERE idJogo = %s AND email = %s", (idJogo, session['email']))
    jogo = cursor.fetchone()

    if not jogo or jogo['dataHoraInicio']:
        return "Este jogo não pode ser editado."

    if request.method == 'POST':
        descricao = request.form['descricao']
        jogador = request.form['jogador']
        cursor.execute("UPDATE Jogo SET descricao = %s, jogador = %s WHERE idJogo = %s",
                       (descricao, jogador, idJogo))
        db.commit()
        return redirect(url_for('dashboard'))

    return render_template("edit_game.html", jogo=jogo)

@web.route('/start/<int:idJogo>')
def start_game(idJogo):
    if 'email' not in session:
        return redirect(url_for('login'))

    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM Jogo WHERE idJogo = %s AND email = %s", (idJogo, session['email']))
    jogo = cursor.fetchone()

    if not jogo or jogo[5]:  # dataHoraInicio not null
        return "Jogo já iniciado ou inválido."

    cursor.execute("UPDATE Jogo SET dataHoraInicio = NOW() WHERE idJogo = %s", (idJogo,))
    db.commit()

    # Simula execução de binário
    import subprocess
    output = subprocess.getoutput(f"./jogo_exec {idJogo}")

    return f"<h3>Jogo iniciado</h3><pre>{output}</pre><a href='/dashboard'>Voltar</a>"

@web.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))
