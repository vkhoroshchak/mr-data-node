"""
File where data node receives requests from management node about:
1) file(segment) creation
2) map() method start
3) shuffle() method start (where necessary)
4) reduce() method start
5) request to return a status(if works) and file size
"""
import base64
import json
import os
import requests
import shutil
import pandas as pd
import moz_sql_parser as sp

with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')) as config_file:
    config = json.load(config_file)


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
        print("I AM IN INIT FOLDER VRIABLES")
        print(file_name)
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
        with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'updated_config.json'), 'w') \
                as updated_config:
            diction = {
                "data_folder_name_path": Command.data_folder_name_path,
                "file_name_path": Command.file_name_path,
                "folder_name_path": Command.folder_name_path,
                "init_folder_name_path": Command.init_folder_name_path,
                "reduce_folder_name_path": Command.reduce_folder_name_path,
                "shuffle_folder_name_path": Command.shuffle_folder_name_path,
                "map_folder_name_path": Command.map_folder_name_path
            }
            json.dump(diction, updated_config, indent=4)

    @staticmethod
    def create_folders():
        print("I AM IN CREATE_FOLDERS")
        print("IS FILE HERE?")
        print(os.path.exists(Command.file_name_path))
        if os.path.exists(Command.file_name_path):
            print("I SHOULD CLEAR FOLDERS FIRST")
            Command.clear_data({"remove_all_data": False})
        print("IS FILE STILL HERE?")
        print(os.path.exists(Command.file_name_path))
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
        file_name = content['file_name'].split(os.sep)[-1]
        path = os.path.join(Command.init_folder_name_path, file_name)
        with open(path, 'w+', encoding='utf-8') as f:
            f.write(content["segment"]["headers"])
            f.writelines(content['segment']["items"])

    @staticmethod
    def hash_f(input):
        return hash(input)

    @staticmethod
    def hash_keys(content, group_by_key):
        # r=root, d=directories, f = files
        files = [os.path.join(r, file) for r, d, f in os.walk(Command.init_folder_name_path) for file in f]
        hash_key_list = []
        for f in files:
            data_f = pd.read_csv(f)

            for j in data_f.loc[:, group_by_key['key_name']]:
                hash_key_list.append(Command.hash_f(j))

        return hash_key_list

    @staticmethod
    def reduce(content):
        reducer = base64.b64decode(content['reducer'])
        key_delimiter = content['key_delimiter']
        dest = content['destination_file']
        parsed_sql = content['parsed_sql']
        parsed_select = parsed_sql['parsed_select']
        parsed_group_by = parsed_sql['parsed_group_by']

        data_frame = pd.read_csv(os.path.join(Command.shuffle_folder_name_path, 'shuffled.csv'))

        exec(reducer)
        for i in parsed_group_by:
            data_frame = locals()['custom_reducer'](data_frame, parsed_select, i['key_name'])

        data_frame.to_csv(os.path.join(Command.reduce_folder_name_path, 'reduced.csv'), index=False)

    @staticmethod
    def finish_shuffle(content):
        cols = list(pd.read_json(content['content']).columns)

        data_frame = pd.read_json(content['content'])
        if not os.path.isfile(content['file_path']):
            data_frame.to_csv(content['file_path'], header=cols, encoding='utf-8', index=False)
        else:
            data_frame.to_csv(content['file_path'], mode='a', header=False, index=False, encoding='utf-8')

    @staticmethod
    def map(content):
        dest = content['destination_file']
        mapper = content['mapper']
        field_delimiter = content['field_delimiter']
        key_delimiter = content['key_delimiter']
        parsed_select = content['parsed_select']
        file = None
        decoded_mapper = base64.b64decode(mapper)
        for f in os.listdir(Command.reduce_folder_name_path):
            if os.path.isfile(os.path.join(Command.reduce_folder_name_path, f)):
                file = f

        content = pd.read_csv(os.path.join(Command.reduce_folder_name_path, file))
        exec(decoded_mapper)
        res = locals()['custom_mapper'](content, parsed_select)
        res.to_csv(Command.file_name_path, index=False, mode="w")

    @staticmethod
    def min_max_hash(hash_key_list, file_name, parsed_group_by):
        with open(os.path.join('config', 'data_node_info.json')) as f:
            arbiter_address = json.load(f)['arbiter_address']

        res = [
            max(hash_key_list),
            min(hash_key_list)
        ]
        url = f'http://{arbiter_address}/command/hash'
        diction = {
            'list_keys': res,
            'file_name': file_name,
            'parsed_group_by': parsed_group_by
        }
        response = requests.post(url, json=diction)
        return response

    @staticmethod
    def clear_data(content):
        print("I AM IN CLEAR_DATA")
        data = content
        remove_all = data['remove_all_data']
        print(remove_all)
        if remove_all:
            print("OH I REMOVE EVERYTHING")
            if os.path.exists(Command.file_name_path):
                os.remove(Command.file_name_path)
        print("REMOVE FOLDERS")
        if os.path.exists(Command.folder_name_path):
            shutil.rmtree(Command.folder_name_path)
        if not os.path.exists(Command.file_name_path):
            os.remove(os.path.join(os.path.dirname(__file__), '..', 'config', 'updated_config.json'))

    # @staticmethod
    # def get_file():
    #     path = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name,
    #                         Command.reduced_fragments_folder_name, 'result')
    #     with open(path) as f:
    #         data = f.read()
    #     return data

    # @staticmethod
    # def get_result_of_key(content):
    #
    #     file_name = content['get_result_of_key']['file_name'].split(os.sep)[-1]
    #     key = content['get_result_of_key']['key']
    #
    #     path = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, file_name)
    #     with open(path) as file:
    #         for line in file.readlines():
    #             if line.split('^')[0] == key:
    #                 return line

    @staticmethod
    def move_file_to_init_folder():
        print("SHOULD I MOVE FILE?")
        if os.path.exists(Command.file_name_path):
            print("LOOKS LIKE I SHOULD AND WILL")
            shutil.move(Command.file_name_path, os.path.join(Command.init_folder_name_path,
                                                             os.path.basename(Command.file_name_path)))
