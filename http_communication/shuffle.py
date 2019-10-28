'''
File where shuffle() method is implemented
'''
import json
import os
import requests

from receive_commands.receive_commands import hash_f, make_file


# TODO: use literals
# TODO: use context managers
# TODO: simplify dict use
class ShuffleCommand:
    def __init__(self, data, file_path):
        self._data = dict()
        self._data['data_node_ip'] = data['data_node_ip']
        self._data['content'] = data['content']
        self._data['file_path'] = file_path

    def send(self):
        data = dict()
        data['finish_shuffle'] = dict()
        data['finish_shuffle']['content'] = self._data['content']
        data['finish_shuffle']['file_path'] = self._data['file_path']

        response = requests.post \
            ('http://' + self._data['data_node_ip'],
             data=json.dumps(data))
        response.raise_for_status()
        return response.json()


# TODO: refactor
def shuffle(content):
    dir_name = content['file_name'].split(os.sep)[-1]
    arbiter_node_json_data = open(os.path.join('config', 'data_node_info.json'))
    self_node_ip = json.load(arbiter_node_json_data)['self_address']
    files = []
    result = {
        'shuffle_items': [
        ]
    }

    folder_name = dir_name.split(os.sep)[-1].split('.')[0].split('_')[0] + "_folder." + \
                  dir_name.split(os.sep)[-1].split('.')[-1]
    new_dir_name = dir_name.split(os.sep)[-1].split('.')[0].split('_')[0] + '_shuffle' + '.' + \
                   dir_name.split(os.sep)[-1].split('.')[-1]

    if not os.path.isfile(folder_name + os.sep + new_dir_name):
        make_file(folder_name + os.sep + new_dir_name)

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
                    if item['hash_keys_range'][0] <= hash_f(line.split('^')[0]) <= item['hash_keys_range'][1]:
                        for i in result['shuffle_items']:
                            if i['data_node_ip'] == item['data_node_ip']:
                                i['content'].append(line)

                else:
                    if item['hash_keys_range'][0] <= hash_f(line.split('^')[0]) < item['hash_keys_range'][1]:
                        for i in result['shuffle_items']:
                            if i['data_node_ip'] == item['data_node_ip']:
                                i['content'].append(line)

    for i in result['shuffle_items']:
        if i['data_node_ip'] == self_node_ip:
            f = open(os.path.join(os.path.dirname(__file__), '..', 'data', folder_name, new_dir_name, 'shuffled'), 'a+')
            f.writelines(i['content'])
        else:
            sc = ShuffleCommand(i, new_dir_name)
            sc.send()
