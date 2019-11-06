'''
File where shuffle() method is implemented
'''
import json
import os
import requests

from receive_commands.receive_commands import Command

with open(os.path.join('config', 'config.json')) as config_file:
    config = json.load(config_file)

with open(os.path.join('config', 'data_node_info.json')) as arbiter_node_json_data:
    self_node_ip = json.load(arbiter_node_json_data)['self_address']


# TODO: use context managers
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
            'finish_shuffle': {
                'content': self._data['content'],
                'file_path': self._data['file_path']
            }
        }

        response = requests.post('http://' + self._data['data_node_ip'], data=json.dumps(data))
        response.raise_for_status()
        return response.json()


# TODO: refactor
def shuffle(content):
    print(content['file_name'])
    dir_name = content['file_name'].split(os.sep)[-1]
    print(dir_name)
    files = []
    result = {
        'shuffle_items': [
        ]
    }

    folder_name = os.path.splitext(dir_name)[0].split(config['name_delimiter'])[0] + config['name_delimiter'] + config[
        'folder_name'] + os.path.splitext(dir_name)[1]
    new_dir_name = os.path.splitext(dir_name)[0].split(config['name_delimiter'])[0] + config['name_delimiter'] + config[
        'shuffled_fragments_folder_name'] + os.path.splitext(dir_name)[1]

    if not os.path.isfile(folder_name + os.sep + new_dir_name):
        Command.make_file(folder_name + os.sep + new_dir_name)

    for i in content['nodes_keys']:
        result['shuffle_items'].append({'data_node_ip': i['data_node_ip'], 'content': []})
    # r=root, d=directories, f = files
    for r, d, f in os.walk(os.path.join(os.path.dirname(__file__), '..', 'data', folder_name, dir_name)):
        for file in f:
            files.append(os.path.join(r, file))

    for f in files:

        for line in open(f):

            for item in content['nodes_keys']:
                if item['hash_keys_range'][1] == content['max_hash']:
                    if item['hash_keys_range'][0] <= Command.hash_f(line.split('^')[0]) <= item['hash_keys_range'][1]:
                        for i in result['shuffle_items']:
                            if i['data_node_ip'] == item['data_node_ip']:
                                i['content'].append(line)

                else:
                    if item['hash_keys_range'][0] <= Command.hash_f(line.split('^')[0]) < item['hash_keys_range'][1]:
                        for i in result['shuffle_items']:
                            if i['data_node_ip'] == item['data_node_ip']:
                                i['content'].append(line)

    for i in result['shuffle_items']:
        if i['data_node_ip'] == self_node_ip:
            with open(os.path.join(os.path.dirname(__file__), '..', 'data', folder_name, new_dir_name, 'shuffled'),
                      'a+') as f:
                f.writelines(i['content'])
        else:
            sc = ShuffleCommand(i, new_dir_name)
            sc.send()
