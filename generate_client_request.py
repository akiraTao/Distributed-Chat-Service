#!/usr/bin/env python3

import json
import click
from collections import OrderedDict


@click.command()
@click.option('--request', '-r', type=(str, str), multiple=True,
              help='Specify the replica i add message j by --request i j')
def generate_client_request(request):
	# Read file contents if already exist
	file_content = ''
	with open('client_requests.json', 'r') as file_handle:
		file_content = file_handle.read()

	client_requests = {}

	try:
		client_requests = json.loads(file_content)

	except json.decoder.JSONDecodeError:
		pass

	for i in range(len(request)):
		if request[i][0] not in client_requests:
			# { client_id : { request_id : value } }
			client_requests[request[i][0]] = {}

		next_request_id = 1
		if client_requests[request[i][0]].keys():
			int_request_id_list = [ int(_id) for _id in client_requests[request[i][0]].keys() ]
			next_request_id = str(max(int_request_id_list) + 1)

		client_requests[request[i][0]][next_request_id] = request[i][1]
		client_requests[request[i][0]] = OrderedDict(sorted(client_requests[request[i][0]].items()))

	data = json.dumps(client_requests, indent=4)

	with open('client_requests.json', 'w') as file_handle:
		file_handle.write(data)


if __name__ == '__main__':
	generate_client_request()
	