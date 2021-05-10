import base64
import json
import os
import requests
import shutil
import pandas as pd

with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')) as config_file:
    config = json.load(config_file)

updated_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'updated_config.json')


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
    file_name_path = None
    folder_name_path = None
    init_folder_name_path = None
    reduce_folder_name_path = None
    shuffle_folder_name_path = None
    map_folder_name_path = None
    data_folder_name_path = None

    @staticmethod
    def init_folder_variables(file_name):
        name, extension = os.path.splitext(file_name)
        folder_format = name + config['name_delimiter'] + '{}' + extension
        Command.data_folder_name_path = config['data_folder_name']
        Command.file_name_path = os.path.join(Command.data_folder_name_path, file_name)
        Command.folder_name_path = os.path.join(Command.data_folder_name_path,
                                                folder_format.format(config['folder_name']))
        Command.init_folder_name_path = os.path.join(Command.folder_name_path,
                                                     folder_format.format(config['init_folder_name']))
        Command.reduce_folder_name_path = os.path.join(Command.folder_name_path,
                                                       folder_format.format(config['reduce_folder_name']))
        Command.shuffle_folder_name_path = os.path.join(Command.folder_name_path,
                                                        folder_format.format(config['shuffle_folder_name']))
        Command.map_folder_name_path = os.path.join(Command.folder_name_path,
                                                    folder_format.format(config['map_folder_name']))
        updated_config = get_updated_config()
        file_paths_info = {
            "file_name": file_name,
            "data_folder_name_path": Command.data_folder_name_path,
            "file_name_path": Command.file_name_path,
            "folder_name_path": Command.folder_name_path,
            "init_folder_name_path": Command.init_folder_name_path,
            "reduce_folder_name_path": Command.reduce_folder_name_path,
            "shuffle_folder_name_path": Command.shuffle_folder_name_path,
            "map_folder_name_path": Command.map_folder_name_path
        }
        if file_paths_info not in updated_config["files"]:
            updated_config['files'].append(file_paths_info)
        save_changes_to_updated_config(updated_config)

    @staticmethod
    def create_folders():
        if os.path.exists(Command.file_name_path):
            Command.clear_data({"folder_name": os.path.basename(Command.file_name_path), "remove_all_data": False})
        Command.make_folder(Command.folder_name_path)
        Command.make_folder(Command.init_folder_name_path)
        Command.make_folder(Command.map_folder_name_path)
        Command.make_folder(Command.shuffle_folder_name_path)
        Command.make_folder(Command.reduce_folder_name_path)

    @staticmethod
    def make_folder(path):
        if not os.path.isdir(path):
            os.makedirs(path)

    @staticmethod
    def write(content):
        file_name = content["file_name"]
        path = os.path.join(Command.init_folder_name_path, file_name)
        # with open(path, 'wb+', encoding='utf-8') as f:
        with open(path, 'wb+') as f:
            f.write(str.encode(content["segment"]["headers"]))
            f.writelines([str.encode(x) for x in content['segment']["items"]])

    @staticmethod
    def hash_f(input):
        return hash(input)

    @staticmethod
    def hash_keys(field_delimiter):
        # r=root, d=directories, f = files
        files = [os.path.join(r, file) for r, d, f in os.walk(Command.map_folder_name_path) for file in f]
        hash_key_list = []
        for f in files:
            data_f = pd.read_csv(f, sep=field_delimiter)

            for j in data_f.loc[:, "key_column"]:
                hash_key_list.append(Command.hash_f(j))

        return hash_key_list

    @staticmethod
    def reduce(content):
        reducer = base64.b64decode(content['reducer'])
        field_delimiter = content['field_delimiter']
        dest = content['destination_file']
        file_path = os.path.join(Command.shuffle_folder_name_path, 'shuffled.csv')
        first_file_paths = get_file_paths(content["source_file"])
        if ',' in content['source_file']:
            first_file_path, second_file_path = content['source_file'].split(',')

            first_file_paths = get_file_paths(first_file_path)

            first_shuffle_file_path = os.path.join(first_file_paths['shuffle_folder_name_path'], 'shuffled.csv')
            second_file_paths = get_file_paths(second_file_path)

            second_shuffle_file_path = os.path.join(second_file_paths['shuffle_folder_name_path'], 'shuffled.csv')
            file_path = (first_shuffle_file_path, second_shuffle_file_path)

        exec(reducer)

        destination_file_path = os.path.join(first_file_paths["data_folder_name_path"], first_file_paths["file_name"])
        locals()['custom_reducer'](file_path, destination_file_path)

    @staticmethod
    def finish_shuffle(content):
        cols = list(pd.read_json(content['content']).columns)
        field_delimiter = content['field_delimiter']

        data_frame = pd.read_json(content['content'])
        if not os.path.isfile(content['file_path']):
            data_frame.to_csv(content['file_path'], header=cols, encoding='utf-8', index=False, sep=field_delimiter)
        else:
            data_frame.to_csv(content['file_path'], mode='a', header=False, index=False, encoding='utf-8',
                              sep=field_delimiter)

    @staticmethod
    def map(content):
        dest = content['destination_file']
        mapper = content['mapper']
        field_delimiter = content['field_delimiter']

        decoded_mapper = base64.b64decode(mapper)
        for f in os.listdir(Command.init_folder_name_path):
            if os.path.isfile(os.path.join(Command.init_folder_name_path, f)):
                exec(decoded_mapper)
                res = locals()['custom_mapper'](os.path.join(Command.init_folder_name_path, f))
                res.to_csv(f"{Command.map_folder_name_path}{os.sep}{f}", index=False, mode="w", sep=field_delimiter)

    @staticmethod
    def min_max_hash(hash_key_list, file_name, field_delimiter):
        with open(os.path.join('config', 'data_node_info.json')) as f:
            arbiter_address = json.load(f)['arbiter_address']

        res = [
            min(hash_key_list),
            max(hash_key_list)
        ]
        url = f'http://{arbiter_address}/command/hash'
        diction = {
            'list_keys': res,
            'file_name': file_name,
            'field_delimiter': field_delimiter
        }
        response = requests.post(url, json=diction)
        return response

    @staticmethod
    def clear_data(content):
        file_name = content['folder_name']
        remove_all = content['remove_all_data']
        updated_config = get_updated_config()

        for item in updated_config['files']:
            if item["file_name"] == file_name:
                if os.path.exists(item['file_name_path']):
                    if remove_all:
                        os.remove(item['file_name_path'])
                else:
                    updated_config['files'].remove(item)
                if os.path.exists(item['folder_name_path']):
                    shutil.rmtree(item['folder_name_path'])
                save_changes_to_updated_config(updated_config)

    @staticmethod
    def move_file_to_init_folder():
        if os.path.exists(Command.file_name_path):
            shutil.move(Command.file_name_path, os.path.join(Command.init_folder_name_path,
                                                             os.path.basename(Command.file_name_path)))

    @staticmethod
    def get_file_from_cluster(context):
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
                response = requests.post(url, json=diction)
            return response
