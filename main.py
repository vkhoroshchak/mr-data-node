from http import server
import json
from multiprocessing import Process
from receive_commands import receive_commands as rc
from http_communication import shuffle


class Handler(server.BaseHTTPRequestHandler):
	def do_POST(self):
		self.send_response(200)
		self.end_headers()
		body_length = int(self.headers['content-length'])
		request_body_json_string = self.rfile.read(body_length).decode('utf-8')

		# Printing  some info to the server console
		#print('Server on port ' + str(self.server.server_port) + ' - request body: ' + request_body_json_string)

		json_data_obj = json.loads(request_body_json_string)
		json_data_obj['SEEN_BY_THE_SERVER'] = 'Yes'

		#print(request_body_json_string)

		self.wfile.write(bytes(json.dumps(self.recognize_command(json_data_obj)), 'utf-8'))

	# TODO: try use **kwargs
	def recognize_command(self, content):
		json_data_obj = dict()
		if 'make_file' in content:
			json_data_obj = content['make_file']
			rc.create_dest_file(json_data_obj["file_name"])
		elif 'write' in content:
			json_data_obj = content['write']
			rc.write(json_data_obj)
		elif 'map' in content:
			json_data_obj = content['map']
			json_data_obj['destination_file'] = rc.map(json_data_obj)
			rc.min_max_hash(rc.hash_keys(json_data_obj['destination_file']), json_data_obj['destination_file'])
		elif 'shuffle' in content:
			shuffle.shuffle(content['shuffle'])
		elif 'reduce' in content:
			rc.reduce(content['reduce'])
		elif 'finish_shuffle' in content:
			rc.finish_shuffle(content)
		elif 'clear_data' in content:
			rc.clear_data(content)
		elif 'get_file' in content:
			json_data_obj.clear()
			json_data_obj['file']=rc.get_file(content['get_file'])
		elif 'get_hash_of_key' in content:
			json_data_obj.clear()
			json_data_obj['key_hash']=rc.hash_f(content['get_hash_of_key'])
		elif 'get_result_of_key' in content:
			json_data_obj.clear()
			json_data_obj['result']=rc.get_result_of_key(content['get_result_of_key'])
		return json_data_obj


def start_server(server_address):
	my_server = server.ThreadingHTTPServer(server_address, Handler)
	print(str(server_address) + ' Waiting for POST requests...')
	my_server.serve_forever()


def start_local_server_on_port(port):
	p = Process(target=start_server, args=(('127.0.0.1', port),))
	p.start()


if __name__ == '__main__':
	start_local_server_on_port(8014)
