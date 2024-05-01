from flask import Flask, request, jsonify
from flask_cors import CORS
import threading

from process_html import format_and_save_data
from manage_db import collect_data_and_insert

app = Flask(__name__)
app.config['DEBUG'] = True
CORS(app)  # Enable CORS for all routes and origins

# Creating a queue and threads with a lock to ensure that only one process can access the database at the time
#db_lock = threading.Lock()


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
    print('setting port')
    port = 3000
    app.run(port=port, debug=True)

