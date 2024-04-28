from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import os

from process_html import format_and_save_data

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes and origins

@app.route('/receive', methods=['POST'])
def receive_data():
    # Format the current date and time in the desired format
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = f"dex_sol_newpairs_{timestamp}"

    try:
        # Write the received HTML data to a file
#        with open(file_path, 'w') as file:
#            file.write(request.data.decode('utf-8'))  # Decode bytes to string
        format_and_save_data(request.data.decode('utf-8'))
        
        return jsonify({"message": "HTML received and saved successfully!"}), 200
    except Exception as e:
        print(f"Failed to save HTML: {e}")
        return jsonify({"message": "Failed to save HTML"}), 500

if __name__ == '__main__':
    port = 3000
    app.run(port=port, debug=True)
