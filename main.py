from http import server
import json
from multiprocessing import Process
from receive_commands.receive_commands import Command as cmd
from http_communication import shuffle
from flask import Flask, jsonify, request, make_response
import base64
import json
import os
import requests
import shutil

with open(os.path.join(os.path.dirname(__file__), "config", "config.json")) as config_file:
    config = json.load(config_file)

app = Flask(__name__)


@app.route("/command/shuffle", methods=["POST"])
def shuffle():
    shuffle.shuffle(request.json)


@app.route("/command/make_file", methods=["POST"])
def make_file():
    file_name = request.json["file_name"]
    cmd.init_folder_variables(file_name)
    cmd.create_dest_file(file_name)

    return jsonify(success=True)


@app.route("/command/write", methods=["POST"])
def write():
    cmd.write(request.json)
    return jsonify(success=True)


@app.route("/command/map", methods=["POST"])
def map():
    response = {'mapped_folder_name': cmd.map(request.json)}
    return jsonify(response)


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
    elif "shuffle" in content:
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


# class Command:
#     file_name = None
#     folder_name = None
#     fragments_folder_name = None
#     reduced_fragments_folder_name = None
#     shuffled_fragments_folder_name = None
#     mapped_fragments_folder_name = None
#     data_folder_name = None
#
#     @staticmethod
#     def init_folder_variables(file_name):
#         name, extension = os.path.splitext(file_name)
#         folder_format = name + config["name_delimiter"] + "{}" + extension
#         Command.file_name = file_name
#         Command.folder_name = folder_format.format(config["folder_name"])
#         Command.fragments_folder_name = folder_format.format(config["fragments_folder_name"])
#         Command.reduced_fragments_folder_name = folder_format.format(config["reduced_fragments_folder_name"])
#         Command.shuffled_fragments_folder_name = folder_format.format(config["shuffled_fragments_folder_name"])
#         Command.mapped_fragments_folder_name = folder_format.format(config["mapped_fragments_folder_name"])
#         Command.data_folder_name = config["data_folder_name"]
#
#     @staticmethod
#     @app.route('/command/make_file', methods=["POST"])
#     def create_dest_file():
#         file_name = request.json["file_name"]
#         Command.init_folder_variables(file_name)
#         if not os.path.isfile(os.path.join(Command.data_folder_name, file_name)):
#             with open(os.path.join(Command.data_folder_name, file_name), "w+") as f:
#                 f.close()
#
#         if not os.path.isdir(os.path.join(Command.data_folder_name, Command.folder_name)):
#             os.mkdir(os.path.join(Command.data_folder_name, Command.folder_name))
#
#         if not os.path.isdir(
#                 os.path.join(Command.data_folder_name, Command.folder_name, Command.fragments_folder_name)):
#             os.makedirs(os.path.join(Command.data_folder_name, Command.folder_name, Command.fragments_folder_name))
#
#         return '', 204
#
#     @staticmethod
#     def make_file(path):
#         if not os.path.isdir(os.path.join(Command.data_folder_name, path)):
#             os.makedirs(os.path.join(Command.data_folder_name, path))
#
#     @staticmethod
#     @app.route("/command/write", methods=["POST"])
#     def write():
#         file_name = request.json["file_name"].split(os.sep)[-1]
#         path = os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name, Command.folder_name,
#                             Command.fragments_folder_name, file_name)
#         with open(path, "w+") as f:
#             f.writelines(request.json["segment"])
#
#         return '', 204
#
#     @staticmethod
#     @app.route("/command/get_hash_of_key", methods=["POST"])
#     def hash_f():
#         res = 545
#         return jsonify({"key_hash": sum([res + ord(i) for i in request.json["get_hash_of_key"]])})
#
#     @staticmethod
#     @app.route("/command/hash_keys", methods=["POST"])
#     def hash_keys(content):
#
#         dir_name = content.split(os.sep)[-1]
#
#         path = os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name, Command.folder_name, dir_name)
#
#         # r=root, d=directories, f = files
#         files = [os.path.join(r, file) for r, d, f in os.walk(path) for file in f]
#         hash_key_list = [Command.hash_f(line.split("^")[0]) for f in files for line in open(f)]
#         return hash_key_list
#
#     @staticmethod
#     @app.route('/command/reduce', methods=['POST'])
#     def reduce():
#
#         reducer = base64.b64decode(request.json["reducer"])
#         key_delimiter = request.json["key_delimiter"]
#         dest = request.json["destination_file"]
#         dir_name = os.path.join(Command.data_folder_name, dest)
#
#         with open(os.path.join(os.path.dirname(__file__), "..", "data", Command.folder_name,
#                                Command.shuffled_fragments_folder_name, "shuffled")) as f:
#             shuffle_content = f.readlines()
#
#         exec(reducer)
#         result = locals()["custom_reducer"](shuffle_content, key_delimiter)
#         with open(os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name, dest), "w+") as f:
#             f.writelines(result)
#         return result
#
#     @staticmethod
#     @app.route('/command/finish_shuffle', methods=['POST'])
#     def finish_shuffle(content):
#
#         data = content["finish_shuffle"]
#         dir_name = data["file_path"]
#
#         full_dir_name = os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name, Command.folder_name,
#                                      dir_name)
#         if not os.path.isfile(full_dir_name):
#             Command.make_file(full_dir_name)
#         with open(os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name, Command.folder_name, dir_name,
#                                "shuffled"), "a+") as f:
#             f.writelines(data["content"])
#
#     @staticmethod
#     @app.route('/command/map', methods=['POST'])
#     def map():
#
#         dest = request.json["destination_file"]
#         mapper = request.json["mapper"]
#         field_delimiter = request.json["field_delimiter"]
#         key_delimiter = request.json["key_delimiter"]
#
#         dir_name = os.path.join(Command.data_folder_name, dest)
#         folder_name = dir_name.split(os.sep)
#
#         Command.make_file(Command.folder_name + os.sep + Command.mapped_fragments_folder_name)
#         decoded_mapper = base64.b64decode(mapper)
#
#         if "server_src" not in request.json:
#
#             for file in os.listdir(
#                     Command.data_folder_name + os.sep + Command.folder_name + os.sep + Command.fragments_folder_name):
#                 content = open(
#                     os.path.join(Command.data_folder_name, Command.folder_name, Command.fragments_folder_name,
#                                  file)).readlines()
#                 exec(decoded_mapper)
#                 res = locals()["custom_mapper"](content, field_delimiter, key_delimiter)
#                 with open(
#                         os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name, Command.folder_name,
#                                      Command.mapped_fragments_folder_name, file),
#                         "w+") as f:
#                     f.writelines(res)
#         else:
#             content = open(os.path.join(Command.data_folder_name, request.json["server_src"])).readlines()
#             exec(decoded_mapper)
#             res = locals()["custom_mapper"](content, field_delimiter, key_delimiter)
#             with open(os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name, Command.folder_name,
#                                    Command.mapped_fragments_folder_name, Command.mapped_fragments_folder_name),
#                       "w+") as f:
#                 f.writelines(res)
#
#         cmd.min_max_hash(cmd.hash_keys(Command.mapped_fragments_folder_name), Command.mapped_fragments_folder_name)
#
#         return jsonify({"destination_file": Command.mapped_fragments_folder_name})
#
#     @staticmethod
#     def min_max_hash(hash_key_list, file_name):
#         with open(os.path.join("config", "data_node_info.json")) as f:
#             arbiter_node_data = json.load(f)["arbiter_address"]
#
#         res = [
#             max(hash_key_list),
#             min(hash_key_list)
#         ]
#         url = "http://" + arbiter_node_data
#         diction = {
#             "hash":
#                 {
#                     "list_keys": res,
#                     "file_name": file_name
#                 }
#         }
#         response = requests.post(url, data=json.dumps(diction))
#         return response.json()
#
#     @staticmethod
#     @app.route('/command/clear_data', methods=["POST"])
#     def clear_data():
#
#         folder_name = request.json["folder_name"]
#         remove_all = request.json["remove_all_data"]
#
#         full_folder_name = os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name, Command.folder_name)
#
#         if remove_all:
#             os.remove(os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name, folder_name))
#         shutil.rmtree(full_folder_name)
#
#     @staticmethod
#     @app.route('/command/get_file', methods=['POST'])
#     def get_file():
#
#         path = os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name,
#                             Command.reduced_fragments_folder_name, "result")
#
#         with open(path) as f:
#             data = f.read()
#
#         return data
#
#     @staticmethod
#     @app.route('/command/get_result_of_key', methods=['GET'])
#     def get_result_of_key():
#
#         file_name = request.json["get_result_of_key"]["file_name"].split(os.sep)[-1]
#         key = request.json["get_result_of_key"]["key"]
#
#         path = os.path.join(os.path.dirname(__file__), "..", Command.data_folder_name, file_name)
#         with open(path) as file:
#             for line in file.readlines():
#                 if line.split("^")[0] == key:
#                     return line


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5002)
