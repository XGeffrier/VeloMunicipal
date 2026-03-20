import logging
import os

from flask import Flask, render_template, send_from_directory, request

from back import get_all_communes, get_data_of, is_valid

if os.getenv("GOOGLE_RUNTIME"):
    import google.cloud.logging

    gcp_logging_client = google.cloud.logging.Client()
    gcp_logging_client.get_default_handler()
    gcp_logging_client.setup_logging()
    logging.info("Google logging enabled")
else:
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("Local logging enabled")
app = Flask(__name__, static_folder="static")


@app.route("/")
def index():
    communes = get_all_communes()
    return render_template("index.html", communes=communes)


@app.route("/commune/<insee_code>")
def commune(insee_code):
    data = get_data_of(insee_code)
    if not data:
        return "Commune non prise en charge, désolé", 404

    # évolution pistes cyclables
    evo_pct = None
    evo_direction = None

    p2021 = data.get("longueur_piste_2021")
    p2026 = data.get("longueur_piste_2026")

    if is_valid(p2021) and is_valid(p2026) and p2021 > 0:
        evo_pct = ((p2026 - p2021) / p2021) * 100
        evo_direction = "up" if evo_pct >= 0 else "down"

    # ratios
    ratio_2021 = None
    ratio_2026 = None
    route = data.get("longueur_route")

    if is_valid(route) and route > 0:
        if is_valid(p2021):
            ratio_2021 = (p2021 / route) * 100
        if is_valid(p2026):
            ratio_2026 = (p2026 / route) * 100

    return render_template(
        "commune.html",
        data=data,
        is_valid=is_valid,
        evo_pct=evo_pct,
        evo_direction=evo_direction,
        ratio_2021=ratio_2021,
        ratio_2026=ratio_2026,
    )


@app.route('/robots.txt')
@app.route('/sitemap.xml')
def static_from_root():
    return send_from_directory(app.static_folder, request.path[1:])


if __name__ == "__main__":
    app.run(debug=True)
