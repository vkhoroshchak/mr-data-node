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


# TODO: refactor
def shuffle(content, group_by_keys):
    print(content)
    dir_name = content['file_name'].split(os.sep)[-1]
    files = []
    result = {'shuffle_items': [], 'headers': []}

    folder_name = config['name_delimiter'].join(os.path.splitext(dir_name)[0].split(config['name_delimiter'])[:-1]) + \
                  config['name_delimiter'] + config[
                      'folder_name'] + os.path.splitext(dir_name)[1]
    new_dir_name = config['name_delimiter'].join(os.path.splitext(dir_name)[0].split(config['name_delimiter'])[:-1]) + \
                   config['name_delimiter'] + config[
                       'shuffled_fragments_folder_name'] + os.path.splitext(dir_name)[1]

    if not os.path.isfile(folder_name + os.sep + new_dir_name):
        Command.make_file(folder_name + os.sep + new_dir_name)

    for i in content['nodes_keys']:
        result['shuffle_items'].append({'data_node_ip': i['data_node_ip'], 'content': []})
    # r=root, d=directories, f = files

    for r, d, f in os.walk(
            os.path.join(os.path.dirname(__file__), '..', config['data_folder_name'], folder_name, dir_name)):
        for file in f:
            files.append(os.path.join(r, file))

    for f in files:
        index_list = []
        data_f = pd.read_csv(f)
        result['headers'] = list(data_f.columns)
        for g_b_k in group_by_keys:
            for index, it in enumerate(data_f.loc[:, g_b_k['key_name']]):
                for item in content['nodes_keys']:
                    if item['hash_keys_range'][1] == content['max_hash']:
                        if item['hash_keys_range'][0] <= Command.hash_f(it) <= item['hash_keys_range'][1]:
                            index_list.append(index)

                    else:
                        if item['hash_keys_range'][0] <= Command.hash_f(it) < item['hash_keys_range'][
                            1]:
                            index_list.append(index)

        for item in content['nodes_keys']:
            for i in result['shuffle_items']:
                if i['data_node_ip'] == item['data_node_ip']:
                    i['content'] = data_f.iloc[index_list]

        for i in result['shuffle_items']:
            if i['data_node_ip'] == self_node_ip:
                i['content'].to_csv(
                    os.path.join(os.path.dirname(__file__), '..', config['data_folder_name'], folder_name, new_dir_name,
                                 'shuffled.csv'), encoding='utf-8', index=False)
            else:
                sc = ShuffleCommand(i, new_dir_name)
                sc.send()
