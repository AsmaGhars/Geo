from flask import Flask, Response
from flask_cors import CORS
import threading
from backend.delete_temp_files import delete_temp_files
from backend.routes.upload import upload_blueprint
from prometheus_client import start_http_server

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST


app = Flask(__name__)
CORS(app)


app.register_blueprint(upload_blueprint)


@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


threading.Thread(target=delete_temp_files, daemon=True).start()

if __name__ == "__main__":
    start_http_server(8000)
    app.run(debug=True, use_reloader=False, port=5000)