'''
File where shuffle() method is implemented
'''
import json
import os
import requests
import pandas as pd

from receive_commands.receive_commands import Command

# uncomment for Ubuntu as it runs __file__ as ~
os.chdir(os.path.dirname(os.path.dirname(__file__)))
print(os.listdir(os.curdir))
#

with open(os.path.join("config", "config.json")) as config_file:
    config = json.load(config_file)

with open(os.path.join('config', 'data_node_info.json')) as arbiter_node_json_data:
    self_node_ip = json.load(arbiter_node_json_data)['self_address']


# TODO: simplify dict use
class ShuffleCommand:
    def __init__(self, data, file_path, field_delimiter):
        self._data = {
            'data_node_ip': data['data_node_ip'],
            'content': data['content'],
            'file_path': file_path,
            'field_delimiter': field_delimiter
        }

    def send(self):
        data = {
            'content': self._data['content'],
            'file_path': self._data['file_path'],
            'field_delimiter': self._data['field_delimiter']
        }
        url = f'http://{self._data["data_node_ip"]}/command/finish_shuffle'
        response = requests.post(url, json=data)
        return response


def shuffle(content):
    full_file_path = os.path.join(Command.shuffle_folder_name_path, 'shuffled.csv')
    group_by_key = content['key']
    field_delimiter = content['field_delimiter']

    files = []

    # r=root, d=directories, f = files
    for r, d, f in os.walk(Command.init_folder_name_path):
        for file in f:
            files.append(os.path.join(r, file))

    for f in files:
        data_f = pd.read_csv(f, sep=field_delimiter)
        headers = list(data_f.columns)

        for i in content['nodes_keys']:
            index_list = []
            for index, it in enumerate(data_f.loc[:, group_by_key]):

                if i['hash_keys_range'][1] == content['max_hash']:
                    if i['hash_keys_range'][0] <= Command.hash_f(it) <= i['hash_keys_range'][1]:
                        index_list.append(index)

                else:
                    if i['hash_keys_range'][0] <= Command.hash_f(it) < i['hash_keys_range'][1]:
                        index_list.append(index)

            if i['data_node_ip'] == self_node_ip:
                if not os.path.isfile(full_file_path):
                    data_f.iloc[index_list].to_csv(full_file_path, header=headers, encoding='utf-8', index=False,
                                                   sep=field_delimiter)
                else:
                    data_f.iloc[index_list].to_csv(full_file_path, mode='a', header=False, index=False,
                                                   encoding='utf-8', sep=field_delimiter)
            else:
                data = {'content': data_f.iloc[index_list].to_json(),
                        'data_node_ip': i['data_node_ip']}

                sc = ShuffleCommand(data, full_file_path, field_delimiter)
                sc.send()
