import logging
import os

from flask import Flask, render_template
from back import get_all_communes, get_data_of

if os.getenv("GOOGLE_RUNTIME"):
    import google.cloud.logging

    gcp_logging_client = google.cloud.logging.Client()
    gcp_logging_client.get_default_handler()
    gcp_logging_client.setup_logging()
    logging.info("Google logging enabled")
else:
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("Local logging enabled")
app = Flask(__name__)


@app.route("/")
def index():
    communes = get_all_communes()
    return render_template("index.html", communes=communes)


@app.route('/robots.txt')
def robots():
    """Route pour servir le fichier robots.txt"""
    return "User-agent: *\nAllow: /", 200, {'Content-Type': 'text/plain; charset=utf-8'}


@app.route("/commune/<insee_code>")
def commune(insee_code):
    data = get_data_of(insee_code)
    if not data:
        return "Commune non-disponible", 404
    return render_template("commune.html", data=data)


if __name__ == "__main__":
    app.run(debug=True)
