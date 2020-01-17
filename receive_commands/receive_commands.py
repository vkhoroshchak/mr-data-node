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

with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')) as config_file:
    config = json.load(config_file)


class Command:
    file_name = None
    folder_name = None
    fragments_folder_name = None
    reduced_fragments_folder_name = None
    shuffled_fragments_folder_name = None
    mapped_fragments_folder_name = None
    data_folder_name = None

    @staticmethod
    def init_folder_variables(file_name):
        name, extension = os.path.splitext(file_name)
        folder_format = name + config['name_delimiter'] + '{}' + extension
        Command.file_name = file_name
        Command.folder_name = folder_format.format(config['folder_name'])
        Command.fragments_folder_name = folder_format.format(config['fragments_folder_name'])
        Command.reduced_fragments_folder_name = folder_format.format(config['reduced_fragments_folder_name'])
        Command.shuffled_fragments_folder_name = folder_format.format(config['shuffled_fragments_folder_name'])
        Command.mapped_fragments_folder_name = folder_format.format(config['mapped_fragments_folder_name'])
        Command.data_folder_name = config['data_folder_name']

    @staticmethod
    def create_dest_file(file_name):
        if not os.path.isfile(os.path.join(Command.data_folder_name, file_name)):
            with open(os.path.join(Command.data_folder_name, file_name), 'w+') as f:
                f.close()

        if not os.path.isdir(os.path.join(Command.data_folder_name, Command.folder_name)):
            os.mkdir(os.path.join(Command.data_folder_name, Command.folder_name))

        if not os.path.isdir(
                os.path.join(Command.data_folder_name, Command.folder_name, Command.fragments_folder_name)):
            os.makedirs(os.path.join(Command.data_folder_name, Command.folder_name, Command.fragments_folder_name))

    @staticmethod
    def make_file(path):
        if not os.path.isdir(os.path.join(Command.data_folder_name, path)):
            os.makedirs(os.path.join(Command.data_folder_name, path))

    @staticmethod
    def write(content):
        file_name = content['file_name'].split(os.sep)[-1]
        path = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name,
                            Command.fragments_folder_name, file_name)
        with open(path, 'w+') as f:
            f.writelines(content['segment'])

    @staticmethod
    def hash_f(string):
        res = 545
        return sum([res + ord(i) for i in string])

    @staticmethod
    def hash_keys(content):

        dir_name = content.split(os.sep)[-1]

        path = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name, dir_name)

        # r=root, d=directories, f = files
        files = [os.path.join(r, file) for r, d, f in os.walk(path) for file in f]
        hash_key_list = [Command.hash_f(line.split('^')[0]) for f in files for line in open(f)]
        return hash_key_list

    @staticmethod
    def reduce(content):

        reducer = base64.b64decode(content['reducer'])
        key_delimiter = content['key_delimiter']
        dest = content['destination_file']
        dir_name = os.path.join(Command.data_folder_name, dest)

        with open(os.path.join(os.path.dirname(__file__), '..', 'data', Command.folder_name,
                               Command.shuffled_fragments_folder_name, 'shuffled')) as f:
            shuffle_content = f.readlines()

        exec(reducer)
        result = locals()['custom_reducer'](shuffle_content, key_delimiter)
        with open(os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, dest), 'w+') as f:
            f.writelines(result)
        return result

    @staticmethod
    def finish_shuffle(content):

        data = content['finish_shuffle']
        dir_name = data['file_path']

        full_dir_name = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name,
                                     dir_name)
        if not os.path.isfile(full_dir_name):
            Command.make_file(full_dir_name)
        with open(os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name, dir_name,
                               'shuffled'), 'a+') as f:
            f.writelines(data['content'])

    @staticmethod
    def map(content):

        dest = content['destination_file']
        mapper = content['mapper']
        field_delimiter = content['field_delimiter']
        key_delimiter = content['key_delimiter']

        dir_name = os.path.join(Command.data_folder_name, dest)
        folder_name = dir_name.split(os.sep)

        Command.make_file(Command.folder_name + os.sep + Command.mapped_fragments_folder_name)
        decoded_mapper = base64.b64decode(mapper)

        if 'server_src' not in content:

            for file in os.listdir(
                    Command.data_folder_name + os.sep + Command.folder_name + os.sep + Command.fragments_folder_name):
                content = open(
                    os.path.join(Command.data_folder_name, Command.folder_name, Command.fragments_folder_name,
                                 file)).readlines()
                exec(decoded_mapper)
                res = locals()['custom_mapper'](content, field_delimiter, key_delimiter)
                with open(
                        os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name,
                                     Command.mapped_fragments_folder_name, file),
                        'w+') as f:
                    f.writelines(res)
            return Command.mapped_fragments_folder_name
        else:
            content = open(os.path.join(Command.data_folder_name, content['server_src'])).readlines()
            exec(decoded_mapper)
            res = locals()['custom_mapper'](content, field_delimiter, key_delimiter)
            with open(os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name,
                                   Command.mapped_fragments_folder_name, Command.mapped_fragments_folder_name),
                      'w+') as f:
                f.writelines(res)
            return Command.mapped_fragments_folder_name

    @staticmethod
    def min_max_hash(hash_key_list, file_name):
        with open(os.path.join('config', 'data_node_info.json')) as f:
            arbiter_address = json.load(f)['arbiter_address']

        res = [
            max(hash_key_list),
            min(hash_key_list)
        ]
        url = f'http://{arbiter_address}/command/hash'
        diction = {
            'list_keys': res,
            'file_name': file_name
        }
        response = requests.post(url, json=diction)
        return response.json()

    @staticmethod
    def clear_data(content):

        data = content['clear_data']
        folder_name = data['folder_name']
        remove_all = data['remove_all_data']

        full_folder_name = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name)

        if remove_all:
            os.remove(os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, folder_name))
        shutil.rmtree(full_folder_name)

    @staticmethod
    def get_file(content):

        path = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name,
                            Command.reduced_fragments_folder_name, 'result')

        with open(path) as f:
            data = f.read()

        return data

    @staticmethod
    def get_result_of_key(content):

        file_name = content['get_result_of_key']['file_name'].split(os.sep)[-1]
        key = content['get_result_of_key']['key']

        path = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, file_name)
        with open(path) as file:
            for line in file.readlines():
                if line.split('^')[0] == key:
                    return line
