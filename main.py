import json
import os
import traceback
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from config.logger import data_node_logger
from http_communication import shuffle as sf
from receive_commands.receive_commands import Command as cmd

logger = data_node_logger.get_logger(__name__)

app = FastAPI()

with open(os.path.join(os.path.dirname(__file__), "config", "config.json")) as config_file:
    config = json.load(config_file)


@app.post("/command/create_config_and_filesystem")
async def create_config_and_filesystem(content: dict):
    try:
        # file_name = request.json["file_name"]
        file_name = content.get("file_name", "")
        file_id = content.get("file_id", "")
        cmd.init_folder_variables(file_name, file_id)
        cmd.create_folders(file_name, file_id)
        return JSONResponse("Config and filesystem created!")
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.post("/command/write")
async def write(content: dict):
    try:
        cmd.write(content)
        return JSONResponse("File segment has been written to data node!")
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.post("/command/map")
async def map(content: dict):
    try:
        response = {'mapped_folder_name': cmd.map(content)}
        return JSONResponse(response)
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.post("/command/shuffle")
async def shuffle(content: dict):
    try:
        sf.shuffle(content)
        return JSONResponse("Shuffle request has been received by data node!")
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.post("/command/finish_shuffle")
async def finish_shuffle(content: dict):
    try:
        cmd.finish_shuffle(content)
        return JSONResponse("Finish shuffle request has been received by data node!")
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.post("/command/min_max_hash")
async def min_max_hash(content: dict):
    try:
        field_delimiter = content['field_delimiter']
        file_id = str(content["file_id"])
        # data_nodes = content["data_nodes"]

        cmd.min_max_hash(cmd.hash_keys(field_delimiter, file_id), file_id, field_delimiter)

        return JSONResponse("Min max hash request has been received by data node!")
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.post("/command/clear_data")
async def clear_data(content: dict):
    try:
        cmd.clear_data(content)

        return JSONResponse("Clear data request has been received by data node!")
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.post("/command/reduce")
async def reduce(content: dict):
    try:
        cmd.reduce(content)

        return JSONResponse("Reduce request has been received by data node!")
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.post('/command/move_file_to_init_folder')
async def move_file_to_init_folder(content: dict):
    try:
        cmd.move_file_to_init_folder(content)
        return JSONResponse("Move file to init folder request has been received by data node!")
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.get('/command/check_if_file_is_on_cluster')
async def check_if_file_is_on_cluster(content: dict):
    try:
        return cmd.check_if_file_is_on_cluster(content)
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.post('/command/get_file_from_cluster')
async def get_file_from_cluster(content: dict):
    try:
        cmd.get_file_from_cluster(content)
        return JSONResponse("Get file from cluster request has been received by data node!")
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()


@app.get('/command/get_file')
async def get_file(content: dict):
    try:
        streaming_file = cmd.get_file(content)
        if not streaming_file:
            raise HTTPException(status_code=404, detail="File not found!")
        return StreamingResponse(streaming_file)
    except Exception as e:
        logger.info("Caught exception!" + e)
        traceback.print_exc()
