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
        with open(path, 'w+', encoding='utf-8') as f:
            f.writelines(content['segment'])

    @staticmethod
    def hash_f(string):
        if type(string) is not str:
            string = str(string)
        res = 545
        return sum([res + ord(i) for i in string])

    @staticmethod
    def hash_keys(content, group_by_key):
        dir_name = content.split(os.sep)[-1]

        path = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name, dir_name)

        # r=root, d=directories, f = files
        files = [os.path.join(r, file) for r, d, f in os.walk(path) for file in f]
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
        sql_query = content['sql_query']

        parsed_sql = json.dumps(sp.parse(sql_query))
        json_res = json.loads(parsed_sql)
        s = Command.select_parser(json_res)
        f = Command.from_parser(json_res)
        gb = Command.group_by_parser(json_res)

        data_frame = pd.read_csv(
            os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name,
                         Command.shuffled_fragments_folder_name, 'shuffled.csv'))

        exec(reducer)
        for i in gb:
            data_frame = locals()['custom_reducer'](data_frame, s, i['key_name'])

        data_frame.to_csv(os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, dest), index=False)

    @staticmethod
    def finish_shuffle(content):
        folder_name = config['name_delimiter'].join(
            os.path.splitext(dir_name)[0].split(config['name_delimiter'])[:-1]) + \
                      config['name_delimiter'] + config[
                          'folder_name'] + os.path.splitext(dir_name)[1]
        new_dir_name = config['name_delimiter'].join(
            os.path.splitext(dir_name)[0].split(config['name_delimiter'])[:-1]) + \
                       config['name_delimiter'] + config[
                           'shuffled_fragments_folder_name'] + os.path.splitext(dir_name)[1]
        data = content
        dir_name = data['file_path']

        full_dir_name = os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name,
                                     dir_name)
        if not os.path.isfile(full_dir_name):
            Command.make_file(full_dir_name)
        data['content'].to_csv(
            os.path.join(os.path.dirname(__file__), '..', config['data_folder_name'], folder_name, new_dir_name,
                         'shuffled.csv'), encoding='utf-8', index=False)

    @staticmethod
    def map(content):
        dest = content['destination_file']
        mapper = content['mapper']
        field_delimiter = content['field_delimiter']
        key_delimiter = content['key_delimiter']
        sql_query = content['sql_query']

        dir_name = os.path.join(Command.data_folder_name, dest)
        folder_name = dir_name.split(os.sep)

        Command.make_file(Command.folder_name + os.sep + Command.mapped_fragments_folder_name)
        decoded_mapper = base64.b64decode(mapper)
        parsed_sql = json.dumps(sp.parse(sql_query))
        json_res = json.loads(parsed_sql)
        s = Command.select_parser(json_res)
        f = Command.from_parser(json_res)
        if 'server_src' not in content:
            files = [f for f in os.listdir(Command.data_folder_name) if
                     os.path.isfile(os.path.join(Command.data_folder_name, f))]
            for file in files:
                content = pd.read_csv(
                    os.path.join(Command.data_folder_name, file))
                exec(decoded_mapper)
                res = locals()['custom_mapper'](content, s)
                res.to_csv(os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name,
                                        Command.mapped_fragments_folder_name, file), index=False)
            return Command.mapped_fragments_folder_name
        else:
            content = pd.read_csv(os.path.join(Command.data_folder_name, content['server_src']))
            exec(decoded_mapper)
            res = locals()['custom_mapper'](content, s)

            res.to_csv(os.path.join(os.path.dirname(__file__), '..', Command.data_folder_name, Command.folder_name),
                       index=False)
            return Command.mapped_fragments_folder_name

    @staticmethod
    def min_max_hash(hash_key_list, file_name, sql):
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
            'sql_query': sql
        }
        response = requests.post(url, json=diction)
        return response

    @staticmethod
    def clear_data(content):

        data = content
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

    @staticmethod
    def select_parser(data):
        select_data = data['select']
        res = []
        for i in select_data:
            item_dict = {}
            if type(i['value']) is not dict:
                item_dict['old_name'] = i['value']
                if 'name' in i.keys():
                    item_dict['new_name'] = i['name']
                else:
                    item_dict['new_name'] = i['value']
            elif 'literal' in i['value'].keys():
                item_dict['old_name'] = i['value']['literal']
                if 'name' in i.keys():
                    item_dict['new_name'] = i['name']
                else:
                    item_dict['new_name'] = i['value']['literal']
            elif 'sum' in i['value'].keys():
                item_dict = Command.parse_aggregation_value('sum', i)

            elif 'min' in i['value'].keys():
                item_dict = Command.parse_aggregation_value('min', i)
            elif 'max' in i['value'].keys():
                item_dict = Command.parse_aggregation_value('max', i)
            elif 'avg' in i['value'].keys():
                item_dict = Command.parse_aggregation_value('avg', i)
            elif 'count' in i['value'].keys():
                item_dict = Command.parse_aggregation_value('count', i)

            res.append(item_dict)

        return res

    @staticmethod
    def from_parser(data):
        res = {}
        if type(data['from']) is not dict:
            res['file_name'] = data['from']
        return res

    @staticmethod
    def parse_aggregation_value(name, data):
        res = {'old_name': data['value'][name]}
        if 'name' in data.keys():
            res['new_name'] = f"{data['name']}"
        else:
            res['new_name'] = f"{name.upper()}_{data['value'][name]}"
        res['aggregate_f_name'] = name
        return res

    @staticmethod
    def map_pd(data_frame, col_names):
        old_names = []
        new_names = []
        for i in col_names:
            old_names.append(i['old_name'])
            new_names.append(i['new_name'])

        res = data_frame[old_names].copy()
        res.columns = new_names
        res.to_csv('mapped_csv_data.csv', index=False)
        return res

    @staticmethod
    def reduce_pd(data_frame, col_names, groupby_cols):
        for i in col_names:
            if 'aggregate_f_name' in i.keys():
                data_frame[i['new_name']] = data_frame.groupby(groupby_cols)[i['new_name']].transform(
                    i['aggregate_f_name'])
        res = data_frame.drop_duplicates(groupby_cols)
        res.to_csv('reduced_csv.csv', index=False)

    @staticmethod
    def group_by_parser(data):
        select_data = data['groupby']
        res = []
        if type(select_data) is list:
            for item in select_data:
                item_dict = {}

                if 'literal' in item['value'].keys():
                    item_dict['key_name'] = item['value']['literal']
                else:
                    item_dict['key_name'] = item['value']

                res.append(item_dict)
        else:
            item_dict = {}
            if 'literal' in select_data['value'].keys():
                item_dict['key_name'] = select_data['value']['literal']
            else:
                item_dict['key_name'] = select_data['value']

            res.append(item_dict)
        return res
