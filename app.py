from flask import Flask, request, jsonify
from flask_cors import CORS
import threading

from collect_and_manage_data import collect_data_and_insert, format_and_save_data

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes and origins


@app.route('/receive', methods=['POST'])
def receive_data():
    try:
        data = request.data.decode('utf-8')
        format_and_save_data(data)
        return jsonify({"message": "HTML received and saved successfully!"}), 200
    except Exception as e:
        print(f"Failed to save HTML: {e}")
        return jsonify({"message": "Failed to save HTML"}), 500


if __name__ == '__main__':
    collection_thread = threading.Thread(
        target = collect_data_and_insert,
        args = (),
        daemon = True
    )
    collection_thread.start()
    port = 3000
    app.run(port=port, debug=True)

