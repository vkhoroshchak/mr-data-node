"""
File where data node receives requests from management node about:
1) file(segment) creation
2) map() method start
3) shuffle() method start (where necessary)
4) reduce() method start
5) request to return a status(if works) and file size
"""
import os
import json
import requests
import shutil
import base64


def hash_f(str):
	res = 545
	for i in str:
		res += ord(i)
	return res


def create_dest_file(file_name):
	folder_name_arr = file_name.split(".")
	folder_name = folder_name_arr[0] + "_folder." + folder_name_arr[-1]
	if not os.path.isfile(os.path.join('data', file_name)):
		with open(os.path.join('data', file_name), 'w+') as f:
			f.close()
	if not os.path.isdir(os.path.join('data', folder_name)):
		os.mkdir(os.path.join('data', folder_name))

	dir_name = file_name.split('.')[0] + '_init.' + file_name.split('.')[1]
	if not os.path.isdir(os.path.join('data', folder_name, dir_name)):
		os.makedirs(os.path.join('data', folder_name, dir_name))


def make_file(path):
	if not os.path.isdir(os.path.join('data', path)):
		os.makedirs(os.path.join('data', path))


def write(content):
	dir_name = content['file_name'].split(os.sep)[-2]
	dir_name_arr = dir_name.split('.')
	new_dir_name = dir_name_arr[0] + '_init.' + dir_name_arr[1]
	file_name = content['file_name'].split(os.sep)[-1]
	folder_name_arr = dir_name.split(".")
	folder_name = folder_name_arr[0] + "_folder." + folder_name_arr[-1]
	path = os.path.join(os.path.dirname(__file__), '..', 'data', folder_name, new_dir_name, file_name)
	f = open(path, 'w+')
	f.writelines(content['segment'])
	f.close()


def reduce(content):
	rds = base64.b64decode(content['reducer'])
	kd = content['key_delimiter']
	dest = content['destination_file']
	dir_name = os.path.join('data', dest.split(os.sep)[-1])
	src_dir_name = dir_name.split(os.sep)[-1].split('.')[0] \
				   + '_shuffle' + '.' + dir_name.split(os.sep)[-1].split('.')[-1]
	folder_name_arr = dest.split(".")
	folder_name = folder_name_arr[0] + "_folder." + folder_name_arr[-1]
	shuffle_content = open(
		os.path.join(os.path.dirname(__file__), '..', 'data', folder_name, src_dir_name, 'shuffled')).readlines()
	exec(rds)
	result = locals()['custom_reducer'](shuffle_content,kd)
	f = open(os.path.join(os.path.dirname(__file__), '..', 'data', dest), 'w+')
	f.writelines(result)
	f.close()
	return result


def map(content):
	dest = content['destination_file']
	mapper = content['mapper']
	field_delimiter = content['field_delimiter']
	key = content['key_delimiter']
	dir_name = os.path.join('data', dest.split(os.sep)[-1])
	folder_name = dir_name.split(os.sep)
	folder_name_arr = folder_name[-1].split(".")
	folder_name = folder_name_arr[0] + "_folder." + folder_name_arr[-1]
	new_dir_name = dir_name.split(os.sep)[-1].split('.')[0] \
				   + '_map' + '.' + dir_name.split(os.sep)[-1].split('.')[-1]
	make_file(folder_name + os.sep + new_dir_name)
	decoded_mapper = base64.b64decode(mapper)
	if not 'server_src' in content:
		dir_name_arr = dir_name.split('.')
		dir_name = dir_name_arr[0].split(os.sep)[-1] + '_init.' + dir_name_arr[1]
		for file in os.listdir('data' + os.sep + folder_name + os.sep + dir_name):
			content = open(os.path.join('data', folder_name, dir_name, file)).readlines()
			exec(decoded_mapper)
			res = locals()['custom_mapper'](content, field_delimiter, key)
			print ("RESRESRESRESRESRES")
			print (res)
			print ("RESRESRESRESRESRES")
			f = open(os.path.join(os.path.dirname(__file__), '..', 'data', folder_name, new_dir_name, file), 'w+')
			f.writelines(res)
			f.close()
		return new_dir_name
	else:
		content = open(os.path.join('data', content['server_src'])).readlines()
		exec(decoded_mapper)
		res = locals()['custom_mapper'](content, field_delimiter, key)
		f = open(os.path.join(os.path.dirname(__file__), '..', 'data', folder_name, new_dir_name, new_dir_name), 'w+')
		f.writelines(res)
		f.close()
		return new_dir_name


def hash_keys(content):
	dir_name = content.split(os.sep)[-1]
	folder_name_arr = dir_name.split(".")
	folder_name = folder_name_arr[0].split("_")[0] + "_folder." + folder_name_arr[1]
	path = os.path.join(os.path.dirname(__file__), '..', 'data', folder_name, dir_name)
	files = []
	hash_key_list = list()
	# r=root, d=directories, f = files
	for r, d, f in os.walk(path):
		for file in f:
			files.append(os.path.join(r, file))
	for f in files:
		for line in open(f):
			hash_key_list.append(hash_f(line.split('^')[0]))
	return hash_key_list


def min_max_hash(hash_key_list, file_name):
	arbiter_node_json_data = open(os.path.join('config', 'data_node_info.json'))
	arbiter_node_data = json.load(arbiter_node_json_data)['arbiter_address']
	res = list()
	res.append(max(hash_key_list))
	res.append(min(hash_key_list))
	url = 'http://' + arbiter_node_data
	diction = {
		'hash':
			{
				'list_keys': res,
				'file_name': file_name
			}
	}
	response = requests.post(url, data=json.dumps(diction))
	return response.json()


def finish_shuffle(content):
	data = content['finish_shuffle']
	dir_name = data['file_path']
	folder_name_arr = dir_name.split(".")
	folder_name = folder_name_arr[0].split("_")[0] + "_folder." + folder_name_arr[-1]
	full_dir_name = os.path.join(os.path.dirname(__file__), '..', 'data', folder_name, dir_name)
	if not os.path.isfile(full_dir_name):
		make_file(full_dir_name)
	f = open(os.path.join(os.path.dirname(__file__), '..', 'data', folder_name, dir_name, 'shuffled'), 'a+')
	f.writelines(data['content'])
	f.close()


def clear_data(content):
	data = content['clear_data']
	folder_name = data['folder_name']
	remove_all = data['remove_all_data']
	folder_name_arr = folder_name.split(".")
	folder_name_full = folder_name_arr[0] + "_folder." + folder_name_arr[-1]
	full_folder_name = os.path.join(os.path.dirname(__file__), '..', 'data', folder_name_full)

	if remove_all:
		os.remove(os.path.join(os.path.dirname(__file__), '..', 'data', folder_name))
	shutil.rmtree(full_folder_name)


# os.mkdir(full_folder_name)

def get_file(content):
	file_name = content['file_name'].split(os.sep)[-1]
	dir_name = file_name.split('.')[0] + '_reduce.' + file_name.split('.')[1]
	path = os.path.join(os.path.dirname(__file__), '..', 'data', dir_name, 'result')
	data = open(path).read()
	return data


def get_result_of_key(content):
	file_name = content['get_result_of_key']['file_name'].split(os.sep)[-1]
	key = content['get_result_of_key']['key']
	field_delimiter = content['get_result_of_key']['field_delimiter']
	dir_name = file_name.split('.')[0]+'_folder.'+file_name.split('.')[1]
	path = os.path.join(os.path.dirname(__file__), '..', 'data',file_name)
	file = open(path)
	for line in file.readlines():
		if line.split('^')[0] == key:
			return line
