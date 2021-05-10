import json
import os
from typing import Union

from fastapi import (
    FastAPI,
)
from fastapi.responses import JSONResponse

from http_communication import shuffle as sf
from receive_commands.receive_commands import Command as cmd

app = FastAPI()

with open(os.path.join(os.path.dirname(__file__), "config", "config.json")) as config_file:
    config = json.load(config_file)


@app.post("/command/create_config_and_filesystem")
async def create_config_and_filesystem(content: dict):
    # file_name = request.json["file_name"]
    cmd.init_folder_variables(content.get("file_name"), content.get("file_id"))
    cmd.create_folders()
    return JSONResponse("Config and filesystem created!")


@app.post("/command/write")
def write(content: dict):
    cmd.write(content)
    return JSONResponse("File segment has been written to data node!")


@app.post("/command/map")
def map(content: dict):
    # deserialized_content = json.loads(content)
    print(35)
    print(content)
    print(type(content))
    print(35)
    # print(deserialized_content)
    # print(type(deserialized_content))
    print(35)
    response = {'mapped_folder_name': cmd.map(content)}
    return JSONResponse(response)


@app.post("/command/shuffle")
def shuffle(content: dict):
    sf.shuffle(content)
    return JSONResponse("Shuffle request has been received by data node!")


@app.post("/command/finish_shuffle")
def finish_shuffle(content: dict):
    cmd.finish_shuffle(content)
    return JSONResponse("Finish shuffle request has been received by data node!")


@app.post("/command/min_max_hash")
def min_max_hash(content: dict):
    field_delimiter = content['field_delimiter']

    cmd.min_max_hash(cmd.hash_keys(field_delimiter), cmd.map_folder_name_path, field_delimiter)

    return JSONResponse("Min max hash request has been received by data node!")


@app.post("/command/clear_data")
def clear_data(content: dict):
    cmd.clear_data(content)

    return JSONResponse("Clear data request has been received by data node!")


@app.post("/command/reduce")
def reduce(content: dict):
    cmd.reduce(content)

    return JSONResponse("Reduce request has been received by data node!")


@app.post('/command/move_file_to_init_folder')
def move_file_to_init_folder():
    cmd.move_file_to_init_folder()
    return JSONResponse("Move file to init folder request has been received by data node!")


@app.post('/command/get_file_from_cluster')
def get_file_from_cluster(content: dict):
    cmd.get_file_from_cluster(content)
    return JSONResponse("Get file from cluster request has been received by data node!")
