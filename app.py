import logging

from flask import Flask, render_template
from back import get_all_communes, get_data_of

app = Flask(__name__)


@app.route("/")
def index():
    communes = get_all_communes()
    return render_template("index.html", communes=communes)


@app.route("/commune/<insee_code>")
def commune(insee_code):
    data = get_data_of(insee_code)
    if not data:
        return "Commune non-disponible", 404
    return render_template("commune.html", data=data)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    app.run(debug=True)
