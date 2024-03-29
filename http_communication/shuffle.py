import dask.dataframe as dd
import json
import os
import pandas as pd
import requests

from config.logger import data_node_logger
from receive_commands.receive_commands import Command

logger = data_node_logger.get_logger(__name__)

# uncomment for Ubuntu as it runs __file__ as ~
os.chdir(os.path.dirname(os.path.dirname(__file__)))

with open(os.path.join("config", "config.json")) as config_file:
    config = json.load(config_file)

with open(os.path.join('config', 'data_node_info.json')) as arbiter_node_json_data:
    self_node_ip = os.environ['SELF_ADDRESS']


class ShuffleCommand:
    def __init__(self, data, file_path, field_delimiter, distribution):
        self._data = {
            'data_node_ip': data['data_node_ip'],
            'content': data['content'],
            'file_path': file_path,
            'field_delimiter': field_delimiter,
            'distribution': distribution
        }

    async def send(self):
        data = {
            'content': self._data['content'],
            'file_path': self._data['file_path'],
            'field_delimiter': self._data['field_delimiter'],
            'distribution': self._data['distribution']
        }
        url = f'http://{self._data["data_node_ip"]}/command/finish_shuffle'
        try:
            response = requests.post(url, json=data, timeout=100)
            return response
        except requests.exceptions.ReadTimeout:
            pass
        except requests.exceptions.ConnectionError:
            pass


async def shuffle(content):  # noqa: C901
    try:
        file_id = content["file_id"]
        full_file_path = os.path.join(Command.paths_per_file_name[file_id]["shuffle_folder_name_path"], 'shuffled.csv')
        field_delimiter = content['field_delimiter']
        distribution = content['distribution']
        response = {}

        for f in Command.paths_per_file_name[file_id]["segment_list"]:
            segment_folder_path = os.path.join(Command.paths_per_file_name[file_id]["map_folder_name_path"], f)
            for segment_file in os.listdir(segment_folder_path):
                if os.path.splitext(segment_file)[-1] == ".parquet":
                    data_f = pd.read_parquet(os.path.join(segment_folder_path, segment_file))

                    # headers = list(data_f.columns)

                    for i in content['nodes_keys']:
                        index_list = []
                        for index, item in enumerate(data_f.loc[:, 'key_column']):

                            min, max = i["hash_keys_range"]
                            last_node = max == content['max_hash']
                            hash_item = Command.hash_f(item)
                            # logger.info(f"{item=}, {hash_item=}")
                            hash_item_in_range = min <= hash_item < max
                            if hash_item_in_range or (hash_item == max and last_node):
                                index_list.append(index)

                        if index_list:
                            # logger.info(f"{index_list=}, {data_f.iloc[index_list].to_json()=}, {i['data_node_ip']=}")
                            if i['data_node_ip'] == self_node_ip:
                                # dd.from_pandas(data_f.iloc[index_list], npartitions=1).to_parquet(full_file_path,
                                dd.from_pandas(data_f.iloc[index_list], chunksize=distribution).to_parquet(
                                    full_file_path,
                                    write_index=False,
                                    engine="pyarrow",
                                    append=True
                                )
                            else:
                                data = {
                                    'content': data_f.iloc[index_list].to_json(),
                                    'data_node_ip': i['data_node_ip'],
                                    'distribution': distribution,
                                    "file_path": full_file_path,
                                    "field_delimiter": field_delimiter,
                                    "self_node_ip": self_node_ip
                                }

                                # resp = {
                                #     "data": data,
                                #     "full_file_path": full_file_path,
                                #     "field_delimiter": field_delimiter,
                                #     "self_node_ip": self_node_ip
                                # }
                                response[i['data_node_ip']] = response.setdefault(i['data_node_ip'], [])
                                response[i['data_node_ip']].append(data)
                                # return response

                                # sc = ShuffleCommand(data, full_file_path, field_delimiter, distribution=distribution)
                                # await sc.send()
                        else:
                            logger.info("index_list is empty!")
        return response
    except Exception as e:
        logger.info("Caught exception!" + str(e))
        logger.error(e, exc_info=True)
