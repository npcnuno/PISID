import mysql
from flask import Flask, session, abort
from routes.web import web
from routes.android import android

# ------------------ FLASK APP ------------------ #
app = Flask(__name__)

def get_db():
    if "db_user" not in session or "db_pass" not in session:
        abort(401, description="Credenciais não encontradas na sessão")

    try:
        connection = mysql.connector.connect(
            host="localhost",
            user=session["db_user"],
            password=session["db_pass"],
            database="mydb"
        )
        return connection
    except mysql.connector.Error as err:
        print(f"[DB ERROR] {err}")
        abort(503, description="Serviço indisponível, tente mais tarde.")

app.secret_key = "your_secret_key"
app.register_blueprint(web, url_prefix='/scripts/web')
app.register_blueprint(android, url_prefix='/scripts/php')


if __name__ == "__main__":
    app.run( host='0.0.0.0', port=5000)

