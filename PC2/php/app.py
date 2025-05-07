from flask import Flask, jsonify
from routes.html import web
from routes.android import android
# ------------------ FLASK APP ------------------ #
app = Flask(__name__)

app.secret_key = "your_secret_key"
app.register_blueprint(web, url_prefix='/scripts/web')
app.register_blueprint(android, url_prefix='/scripts/android')


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)