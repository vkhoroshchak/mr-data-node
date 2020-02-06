from http import server
import json
from multiprocessing import Process
from receive_commands.receive_commands import Command as cmd
from http_communication import shuffle as sf
from flask import Flask, jsonify, request, make_response
import base64
import json
import os
import requests
import shutil
import moz_sql_parser as sp

with open(os.path.join(os.path.dirname(__file__), "config", "config.json")) as config_file:
    config = json.load(config_file)

app = Flask(__name__)


@app.route("/command/make_file", methods=["POST"])
def make_file():
    file_name = request.json["file_name"]
    cmd.init_folder_variables(file_name)
    cmd.create_filesystem()

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
    print("SSSS")
    print(request.json)
    sql = request.json['sql_query']

    parsed_sql = json.dumps(sp.parse(sql))
    json_res = json.loads(parsed_sql)
    group_by_keys = cmd.group_by_parser(json_res)
    sf.shuffle(request.json, group_by_keys[0])
    return jsonify(success=True)


@app.route("/command/finish_shuffle", methods=["POST"])
def finish_shuffle():
    cmd.finish_shuffle(request.json)
    return jsonify(success=True)


@app.route("/command/min_max_hash", methods=["POST"])
def min_max_hash():
    print(request.json)

    sql = request.json['sql_query']

    parsed_sql = json.dumps(sp.parse(sql))
    json_res = json.loads(parsed_sql)
    group_by_keys = cmd.group_by_parser(json_res)

    cmd.min_max_hash(cmd.hash_keys(cmd.init_folder_name_path, group_by_keys[0]), cmd.init_folder_name_path, sql)

    return jsonify(success=True)


@app.route("/command/clear_data", methods=["POST"])
def clear_data():
    cmd.init_folder_variables(request.json["folder_name"])
    cmd.clear_data(request.json)

    return jsonify(success=True)


@app.route("/command/reduce", methods=["POST"])
def reduce():
    cmd.reduce(request.json)

    return jsonify(success=True)


def recognize_command(self, content):
    json_data_obj = {}
    if "make_file" in content:
        json_data_obj = content["make_file"]
        cmd.init_folder_variables(json_data_obj["file_name"])
        cmd.create_dest_file(json_data_obj["file_name"])
    elif "write" in content:
        json_data_obj = content["write"]
        cmd.write(json_data_obj)
    elif "map" in content:
        json_data_obj = content["map"]
        json_data_obj["destination_file"] = cmd.map(json_data_obj)
        cmd.min_max_hash(cmd.hash_keys(json_data_obj["destination_file"]), json_data_obj["destination_file"])
    elif "shuffle" in content: # ???????????????????
        shuffle.shuffle(content["shuffle"])
    elif "reduce" in content:
        cmd.reduce(content["reduce"])
    elif "finish_shuffle" in content:
        cmd.finish_shuffle(content)
    elif "clear_data" in content:
        cmd.init_folder_variables(content["clear_data"]["folder_name"])
        cmd.clear_data(content)
    elif "get_file" in content:
        json_data_obj.clear()
        json_data_obj["file"] = cmd.get_file(content["get_file"])
    elif "get_hash_of_key" in content:
        json_data_obj.clear()
        json_data_obj["key_hash"] = cmd.hash_f(content["get_hash_of_key"])
    elif "get_result_of_key" in content:
        json_data_obj.clear()
        json_data_obj["result"] = cmd.get_result_of_key(content["get_result_of_key"])


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5002, debug=True, use_reloader=False)
