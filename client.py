#!/usr/env/bin python3

import json
import time
import click
import socket


def send_client_request(my_id, my_ip, my_port, my_request_list, replica_config):
    # { request_no : message }
    my_request_list = my_request_list
    # { replica_id : { 'ip' : '', 'port' : '' } }
    replica_config = replica_config

    # Build the socket to receive external messages
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    my_socket.bind((my_ip, my_port))
    my_socket.listen(5)

    for request_no, value in my_request_list.items():
        message = {
            'message_type' : 'client_request',
            'client_id' : my_id,
            'client_request_no' : request_no,
            'value' : value
        }

        data = json.dumps(message)

        sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender_socket.connect(('localhost', 6000))
        sender_socket.sendall(str.encode(data))
        sender_socket.close()

        time.sleep(1)

    my_socket.close()


@click.command()
@click.argument('client_id')
@click.argument('client_request_file')
@click.argument('replica_config_file')
def main(client_id, client_request_file, replica_config_file):
    my_id = int(client_id)
    # Read necessary information from config_file
    client_request_file_handle = open(client_request_file, 'r')
    client_request_data = client_request_file_handle.read()
    client_request_file_handle.close()
    replica_config_file_handle = open(replica_config_file, 'r')
    replica_config_data = replica_config_file_handle.read()
    replica_config_file_handle.close()

    replica_config_list = json.loads(replica_config_data).get('replica_list')

    # { request_no : message }
    my_request_to_send = json.loads(client_request_data).get(str(my_id))
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

    my_ip = 'localhost'
    my_port = 8000

    send_client_request(my_id, my_ip, my_port, my_request_list, replica_config) 


if __name__ == '__main__':
    main()
