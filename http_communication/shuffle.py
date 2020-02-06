'''
File where shuffle() method is implemented
'''
import json
import os
import requests
import pandas as pd

from receive_commands.receive_commands import Command

with open(os.path.join('config', 'config.json')) as config_file:
    config = json.load(config_file)

with open(os.path.join('config', 'data_node_info.json')) as arbiter_node_json_data:
    self_node_ip = json.load(arbiter_node_json_data)['self_address']


# TODO: simplify dict use
class ShuffleCommand:
    def __init__(self, data, file_path):
        self._data = {
            'data_node_ip': data['data_node_ip'],
            'content': data['content'],
            'file_path': file_path
        }

    def send(self):
        data = {
            'content': self._data['content'],
            'file_path': self._data['file_path']
        }
        url = f'http://{self._data["data_node_ip"]}/command/finish_shuffle'
        response = requests.post(url, json=data)
        return response


def shuffle(content, group_by_key):
    full_file_path = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name,
                                  Command.shuffled_fragments_folder_name,
                                  'shuffled.csv')
    full_init_dir_path = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name,
                                      Command.fragments_folder_name)

    files = []
    # r=root, d=directories, f = files

    for r, d, f in os.walk(full_init_dir_path):
        for file in f:
            files.append(os.path.join(r, file))

    for f in files:
        data_f = pd.read_csv(f)
        headers = list(data_f.columns)

        for i in content['nodes_keys']:
            index_list = []
            for index, it in enumerate(data_f.loc[:, group_by_key['key_name']]):

                if i['hash_keys_range'][1] == content['max_hash']:
                    if i['hash_keys_range'][0] <= Command.hash_f(it) <= i['hash_keys_range'][1]:
                        index_list.append(index)

                else:
                    if i['hash_keys_range'][0] <= Command.hash_f(it) < i['hash_keys_range'][1]:
                        index_list.append(index)

            if i['data_node_ip'] == self_node_ip:
                if not os.path.isfile(full_file_path):
                    data_f.iloc[index_list].to_csv(full_file_path, header=headers, encoding='utf-8', index=False)
                else:
                    data_f.iloc[index_list].to_csv(full_file_path, mode='a', header=False, index=False,
                                                   encoding='utf-8')
            else:
                print(data_f.iloc[index_list].info())
                data = {'content': data_f.iloc[index_list].to_json(),
                        'data_node_ip': i['data_node_ip']}

                sc = ShuffleCommand(data, full_file_path)
                sc.send()
