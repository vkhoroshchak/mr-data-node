from receive_commands.receive_commands import Command as cmd
from http_communication import shuffle as sf
from flask import Flask, jsonify, request
import json
import os

with open(os.path.join(os.path.dirname(__file__), "config", "config.json")) as config_file:
    config = json.load(config_file)

app = Flask(__name__)


@app.route("/command/create_config_and_filesystem", methods=["POST"])
def create_config_and_filesystem():
    file_name = request.json["file_name"]
    cmd.init_folder_variables(file_name)
    cmd.create_folders()
    return jsonify(success=True)


@app.route("/command/write", methods=["POST"])
def write():
    cmd.write(request.json)
    return jsonify(success=True)


@app.route("/command/map", methods=["POST"])
def map():
    response = {'mapped_folder_name': cmd.map(request.json)}
    return jsonify(response)


@app.route("/command/shuffle", methods=["POST"])
def shuffle():
    sf.shuffle(request.json)
    return jsonify(success=True)


@app.route("/command/finish_shuffle", methods=["POST"])
def finish_shuffle():
    cmd.finish_shuffle(request.json)
    return jsonify(success=True)


@app.route("/command/min_max_hash", methods=["POST"])
def min_max_hash():
    field_delimiter = request.json['field_delimiter']
    key = request.json['key']

    cmd.min_max_hash(cmd.hash_keys(key, field_delimiter),
                     cmd.init_folder_name_path,
                     key, field_delimiter)

    return jsonify(success=True)


@app.route("/command/clear_data", methods=["POST"])
def clear_data():
    cmd.clear_data(request.json)

    return jsonify(success=True)


@app.route("/command/reduce", methods=["POST"])
def reduce():
    cmd.reduce(request.json)

    return jsonify(success=True)


@app.route('/command/move_file_to_init_folder', methods=['POST'])
def move_file_to_init_folder():
    cmd.move_file_to_init_folder()
    return jsonify(success=True)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5002, debug=True, use_reloader=False)
