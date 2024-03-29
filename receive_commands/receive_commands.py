import base64
import dask.dataframe as dd
import hashlib
import json
import os
import pandas as pd
import requests
import shutil
import tempfile
from fastapi import HTTPException
from pathlib import Path

from config.logger import data_node_logger

with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')) as config_file:
    config = json.load(config_file)

updated_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'updated_config.json')

logger = data_node_logger.get_logger(__name__)


def get_updated_config():
    with open(updated_config_path) as f:
        updated_config = json.load(f)
    return updated_config


def save_changes_to_updated_config(updated_config):
    with open(updated_config_path, 'w') as f:
        json.dump(updated_config, f, indent=4)


def get_file_paths(file_name):
    for item in get_updated_config()['files']:
        if item['file_name'] == os.path.basename(file_name):
            return item


class Command:
    paths_per_file_name = {}
    file_name_path = None
    folder_name_path = None
    init_folder_name_path = None
    reduce_folder_name_path = None
    shuffle_folder_name_path = None
    map_folder_name_path = None
    data_folder_name_path = None

    # TODO: refactor
    @staticmethod
    def init_folder_variables(file_name, file_id):
        try:
            name, extension = os.path.splitext(file_name)
            folder_format = name + config['name_delimiter'] + '{}_' + file_id + extension
            # Command.paths_per_file_name = {}
            Command.paths_per_file_name[file_id] = {
                "data_folder_name_path": config['data_folder_name'] + config['name_delimiter'] + file_id,
                "file_name_path": os.path.join(config['data_folder_name'], name + config['name_delimiter'] + file_id
                                               + extension),
                "folder_name_path": os.path.join(config['data_folder_name'],
                                                 folder_format.format(config['folder_name'])),
            }

            Command.paths_per_file_name[file_id].update(
                {
                    "init_folder_name_path": os.path.join(Command.paths_per_file_name[file_id]["folder_name_path"],
                                                          folder_format.format(config['init_folder_name'])),
                    "reduce_folder_name_path": os.path.join(Command.paths_per_file_name[file_id]["folder_name_path"],
                                                            folder_format.format(config['reduce_folder_name'])),
                    "shuffle_folder_name_path": os.path.join(Command.paths_per_file_name[file_id]["folder_name_path"],
                                                             folder_format.format(config['shuffle_folder_name'])),
                    "map_folder_name_path": os.path.join(Command.paths_per_file_name[file_id]["folder_name_path"],
                                                         folder_format.format(config['map_folder_name'])),
                }
            )

            updated_config = get_updated_config()
            file_paths_info = {
                "file_id": file_id,
                "file_name": file_name,
                "data_folder_name_path": Command.paths_per_file_name[file_id]["data_folder_name_path"],
                "file_name_path": Command.paths_per_file_name[file_id]["file_name_path"],
                "folder_name_path": Command.paths_per_file_name[file_id]["folder_name_path"],
                "init_folder_name_path": Command.paths_per_file_name[file_id]["init_folder_name_path"],
                "reduce_folder_name_path": Command.paths_per_file_name[file_id]["reduce_folder_name_path"],
                "shuffle_folder_name_path": Command.paths_per_file_name[file_id]["shuffle_folder_name_path"],
                "map_folder_name_path": Command.paths_per_file_name[file_id]["map_folder_name_path"],
            }

            if file_paths_info not in updated_config["files"]:
                updated_config['files'].append(file_paths_info)

            save_changes_to_updated_config(updated_config)
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    def create_folders(file_name, file_id):
        try:
            if os.path.exists(Command.paths_per_file_name[file_id]["file_name_path"]):
                Command.clear_data(
                    {
                        "folder_name": os.path.basename(Command.paths_per_file_name[file_id]["file_name_path"]),
                        "remove_all_data": False,
                        "file_id": file_id
                    }
                )
            Command.make_folder(Command.paths_per_file_name[file_id]["folder_name_path"])
            Command.make_folder(Command.paths_per_file_name[file_id]["init_folder_name_path"])
            Command.make_folder(Command.paths_per_file_name[file_id]["map_folder_name_path"])
            Command.make_folder(Command.paths_per_file_name[file_id]["shuffle_folder_name_path"])
            Command.make_folder(Command.paths_per_file_name[file_id]["reduce_folder_name_path"])
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    def make_folder(path):
        if not os.path.isdir(path):
            os.makedirs(path)

    @staticmethod
    def write(content):
        try:
            file_name = content["file_name"]
            file_id = content["file_id"]
            path = os.path.join(Command.paths_per_file_name[file_id]["init_folder_name_path"], file_name)
            with open(path, 'wb+') as f:
                # try:
                #     f.write(str.encode(content["segment"]["headers"]))
                # except Exception as e:
                f.write(str.encode(",".join(content["segment"]["headers"]) + "\n"))
                items = json.loads(content['segment']["items"])
                # f.writelines([str.encode(x) for x in items])
                f.writelines([str.encode(",".join(x) + "\n") for x in items])
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    def hash_f(input):
        try:
            input_hash = hashlib.md5()
            input_hash.update(str(input).encode("utf-8"))
            input_hash_hex = input_hash.hexdigest()
            input_hash_dec = int(input_hash_hex, 16)
            return input_hash_dec
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    async def hash_keys(field_delimiter, file_id):
        try:
            hash_key_list = []
            print(Command.paths_per_file_name)
            for segment in Command.paths_per_file_name[file_id]["segment_list"]:
                segment_folder_path = os.path.join(Command.paths_per_file_name[file_id]["map_folder_name_path"],
                                                   segment)
                for segment_file in os.listdir(segment_folder_path):
                    if os.path.splitext(segment_file)[-1] == ".parquet":
                        data_f = dd.read_parquet(os.path.join(segment_folder_path, segment_file))
                        for i, j in enumerate(data_f.loc[:, "key_column"]):
                            hash_value = Command.hash_f(j)
                            # logger.info(f"{i=}, {j=}, {hash_value=}")
                            hash_key_list.append(hash_value)
                # data_f = dd.read_parquet(os.path.join(Command.paths_per_file_name[file_id]["map_folder_name_path"],
                #                                       segment, "part.0.parquet"))
                # for j in data_f.loc[:, "key_column"]:
                # for j in data_f.loc["key_column"]:
                #     hash_key_list.append(Command.hash_f(j))

            return hash_key_list
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    def reduce(content):
        try:
            reducer = base64.b64decode(content['reducer'])
            field_delimiter = content['field_delimiter']
            file_id = content["file_id"]
            file_name = content["source_file"]

            file_path = Path(Command.paths_per_file_name[file_id]["shuffle_folder_name_path"], "shuffled.csv")

            shuffled_files = [os.path.join(file_path, f) for f in os.listdir(file_path)
                              if os.path.splitext(f)[-1] == ".parquet"]
            first_file_paths = get_file_paths(file_name)
            if ',' in file_name:
                first_file_path, second_file_path = file_name.split(',')

                first_file_paths = get_file_paths(first_file_path)
                first_shuffle_file_path = os.path.join(first_file_paths['shuffle_folder_name_path'], 'shuffled.csv')

                first_shuffled_files = [os.path.join(first_shuffle_file_path, f)
                                        for f in os.listdir(first_shuffle_file_path)
                                        if os.path.splitext(f)[-1] == ".parquet"]

                second_file_paths = get_file_paths(second_file_path)
                second_shuffle_file_path = os.path.join(second_file_paths['shuffle_folder_name_path'], 'shuffled.csv')

                second_shuffled_files = [os.path.join(second_shuffle_file_path, f)
                                         for f in os.listdir(second_shuffle_file_path)
                                         if os.path.splitext(f)[-1] == ".parquet"]

                shuffled_files = zip(first_shuffled_files, second_shuffled_files)

            exec(reducer)

            destination_file_path = os.path.join(first_file_paths["data_folder_name_path"],
                                                 first_file_paths["file_name"])
            # logger.info(f"{shuffled_files=}, {first_file_paths=}, {destination_file_path=}")
            for index, shuffled_file in enumerate(shuffled_files):
                # logger.info(f"{index=}, {shuffled_file=}, {destination_file_path=}")
                locals()['custom_reducer'](shuffled_file, destination_file_path, index == 0)
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    async def finish_shuffle(content):
        try:
            # cols = list(pd.read_json(content['content']).columns)
            # field_delimiter = content['field_delimiter']
            # logger.info(f"{content=}")
            for i in content["data_to_data_node"]:
                data_frame = pd.read_json(i['content'])
                # data_frame = dd.from_pandas(data_frame, npartitions=2)
                data_frame = dd.from_pandas(data_frame, chunksize=i['distribution'])
                if not os.path.isfile(i['file_path']):
                    data_frame.to_parquet(i['file_path'],
                                          write_index=False,
                                          engine="pyarrow",
                                          append=True
                                          )
                else:
                    data_frame.to_csv(i['file_path'],
                                      write_index=False,
                                      engine="pyarrow",
                                      )
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    def map(content):
        try:
            mapper = content['mapper']
            field_delimiter = content['field_delimiter']
            file_id = content["file_id"]

            decoded_mapper = base64.b64decode(mapper)
            Command.paths_per_file_name[file_id]["segment_list"] = []
            for f in os.listdir(Command.paths_per_file_name[file_id]["init_folder_name_path"]):
                if os.path.isfile(os.path.join(Command.paths_per_file_name[file_id]["init_folder_name_path"], f)):
                    exec(decoded_mapper)
                    res = locals()['custom_mapper'](os.path.join(
                        Command.paths_per_file_name[file_id]["init_folder_name_path"], f))
                    # res.to_csv(f"{Command.paths_per_file_name[file_id]['map_folder_name_path']}{os.sep}{f}",
                    #            index=False, mode="w", sep=field_delimiter, single_file=True)
                    res.to_parquet(f"{Command.paths_per_file_name[file_id]['map_folder_name_path']}{os.sep}{f}",
                                   write_index=False,
                                   engine="pyarrow",
                                   append=True
                                   )
                    Command.paths_per_file_name[file_id]["segment_list"].append(f)
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    async def min_max_hash(hash_key_list, file_id, field_delimiter):
        try:
            with open(os.path.join('config', 'data_node_info.json')) as f:
                arbiter_address = json.load(f)['arbiter_address']
            url = f'http://{arbiter_address}/command/hash'
            diction = {
                'min_hash_value': min(hash_key_list),
                'max_hash_value': max(hash_key_list),
                'file_id': file_id,
            }
            response = requests.post(
                url,
                json=diction,
                headers={'Content-type': 'application/json', 'Accept': 'text/plain'}
            )
            return response
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    def clear_data(content):
        try:
            file_name = content['folder_name']
            remove_all = content['remove_all_data']
            file_id = content["file_id"]
            if file_id in Command.paths_per_file_name:
                del Command.paths_per_file_name[file_id]
            updated_config = get_updated_config()

            for item in updated_config['files']:
                if item["file_name"] == file_name:
                    if os.path.exists(item['file_name_path']):
                        if remove_all:
                            # os.remove(item['file_name_path'])
                            shutil.rmtree(item['file_name_path'])
                    else:
                        updated_config['files'].remove(item)
                    if os.path.exists(item['folder_name_path']):
                        shutil.rmtree(item['folder_name_path'])
                    save_changes_to_updated_config(updated_config)
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    def move_file_to_init_folder(content):
        try:
            file_id = content["file_id"]
            if os.path.exists(Command.paths_per_file_name[file_id]["file_name_path"]):
                shutil.move(Command.paths_per_file_name[file_id]["file_name_path"],
                            os.path.join(Command.paths_per_file_name[file_id]["init_folder_name_path"],
                                         os.path.basename(Command.paths_per_file_name[file_id]["file_name_path"])))
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    def check_if_file_is_on_cluster(content):
        return {'is_file_on_data_node': content['file_id'] in Command.paths_per_file_name}

    @staticmethod
    def get_file_from_cluster(context):
        try:
            file_name = context['file_name']
            file_name_path = os.path.join(Command.data_folder_name_path, file_name)
            if os.path.exists(file_name_path):
                content_json = pd.read_csv(file_name_path).to_json()
                with open(os.path.join('config', 'data_node_info.json')) as f:
                    arbiter_address = json.load(f)['arbiter_address']
                    url = f'http://{arbiter_address}/command/finish_get_file_from_cluster'
                    diction = {
                        'content': content_json,
                        'file_name': file_name,
                        'dest_file_name': context['dest_file_name']
                    }
                    response = requests.post(
                        url,
                        json=diction,
                        headers={'Content-type': 'application/json', 'Accept': 'text/plain'})
                return response
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)

    @staticmethod
    def get_file(content: dict):
        try:
            file_name = content['file_name']
            file_id = content['file_id']
            try:
                file_name_path = os.path.join(Command.paths_per_file_name[file_id]["data_folder_name_path"], file_name)
            except KeyError:
                raise HTTPException(status_code=404, detail="File not found!")
            logger.info(f"{file_name_path=}")
            logger.info(f"Searching for file_name_path: {os.path.exists(file_name_path)}")

            if os.path.exists(file_name_path):
                # get file after MR has been done
                # with open(file_name_path, "rb") as csv_file:
                #     yield csv_file.read()
                # logger.info(f"os.listdir(file_name_path) = {os.listdir(file_name_path)}")
                # segments = [f for f in os.listdir(file_name_path) if os.path.splitext(f)[-1] == ".part"]
                with tempfile.TemporaryDirectory() as tmp:
                    df = dd.read_csv(os.path.join(file_name_path, "part.*.csv"))
                    csv_path = os.path.join(tmp, f'{file_name}')
                    df.to_csv(csv_path, single_file=True, index=False)
                    with open(csv_path, "rb") as csv_file:
                        # yield csv_file.read()
                        yield from csv_file
                        # return csv_file.read()
                # segments = [f for f in os.listdir(file_name_path)]
                # for f in segments:
                #     segment_path = str(os.path.abspath(os.path.join(file_name_path, f)))
                #     with tempfile.TemporaryDirectory() as tmp:
                #         df = dd.read_csv(segment_path)
                #         csv_path = os.path.join(tmp, f'{file_name}')
                #         df.to_csv(csv_path, single_file=True, index=False)
                #         with open(csv_path, "rb") as csv_file:
                #             # yield csv_file.read()
                #             yield from csv_file
                #             # return csv_file.read()
            else:
                # get file that has only been pushed on cluster
                init_folder_name_path = Command.paths_per_file_name[file_id]["init_folder_name_path"]
                logger.info(f"{init_folder_name_path=}")
                logger.info(f"Searching for init_folder_name_path: {os.path.exists(init_folder_name_path)}")
                if os.path.exists(init_folder_name_path):
                    # logger.info(f"os.listdir(init_folder_name_path) = {os.listdir(init_folder_name_path)}")
                    for f in os.listdir(init_folder_name_path):
                        segment_path = str(os.path.abspath(os.path.join(init_folder_name_path, f)))
                        with tempfile.TemporaryDirectory() as tmp:
                            df = dd.read_csv(segment_path)
                            csv_path = os.path.join(tmp, f'{file_name}')
                            df.to_csv(csv_path, single_file=True, index=False)
                            with open(csv_path, "rb") as csv_file:
                                # yield csv_file.read()
                                yield from csv_file
                                # return csv_file.read()
                else:
                    logger.info("This method will return NULL!")
        except Exception as e:
            logger.info("Caught exception!" + str(e))
            logger.error(e, exc_info=True)
