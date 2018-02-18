#!/usr/env/bin python3

import json
import time
import click
import socket


def send_client_request(client_id, my_request_list, replica_config):
    # { request_no : message }
    my_request_list = my_request_list
    # { replica_id : { 'ip' : '', 'port' : '' } }
    replica_config = replica_config

    for request_no, value in my_request_list.items():
        message = {
            'message_type' : 'client_request',
            'client_id' : client_id,
            'client_request_no' : request_no,
            'value' : value
        }

        data = json.dumps(message)

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(('localhost', 6000))
        client_socket.sendall(str.encode(data))
        client_socket.close()

        time.sleep(1)


@click.command()
@click.argument('client_id')
@click.argument('client_request_file')
@click.argument('replica_config_file')
def main(client_id, client_request_file, replica_config_file):
    client_id = int(client_id)
    # Read necessary information from config_file
    client_request_file_handle = open(client_request_file, 'r')
    client_request_data = client_request_file_handle.read()
    client_request_file_handle.close()
    replica_config_file_handle = open(replica_config_file, 'r')
    replica_config_data = replica_config_file_handle.read()
    replica_config_file_handle.close()

    replica_config_list = json.loads(replica_config_data).get('replica_list')

    # { request_no : message }
    my_request_to_send = json.loads(client_request_data).get(str(client_id))
    my_request_list = {}
    for request_no, value in my_request_to_send.items():
        my_request_list[int(request_no)] = value

    # { replica_id : { 'ip' : '', 'port' : '' } }
    replica_config = {}
    for raw_config in replica_config_list:
        replica_config[raw_config['id']] = {
            'ip' : raw_config['ip'],
            'port' : raw_config['port']
        }

    del replica_config[client_id]

    send_client_request(client_id, my_request_list, replica_config) 


if __name__ == '__main__':
    main()
