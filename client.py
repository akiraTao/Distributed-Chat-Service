#!/usr/env/bin python3

import json
import time
import click
import socket


def send_client_request(my_id, my_ip, my_port, replica_config):
    # { request_no : message }
    # { replica_id : { 'ip' : '', 'port' : '' } }
    t_replica_config = replica_config

    # Build the socket to receive external messages
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    my_socket.bind((my_ip, my_port))
    my_socket.listen(5)
    request_id = 0
    leader_id = 0

    message = {
        'message_type' : 'client_request',
        'client_id' : my_id,
        'client_request_no' : request_no,
        'value' : value
    }
    while 1:
        # get user's message
        message.value = input("Enter your message: ")
        message.client_request_no = request_id
        request_id += 1 
        data = json.dumps(message)
        # send parsed messages to assuemed leader
        sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender_socket.connect((t_replica_config[leader_id]['ip'], t_replica_config[leader_id]['port']))
        sender_socket.sendall(str.encode(data))
        sender_socket.close()
        # accept reply from replicas
        time_recorder = time.time()
        while 1:
            try:
                # accept messages from replicas
                sender_socket = my_socket.accept()[0]
                sender_socket.sendtimeout(3)
                data = sender_socket.recv(1024)
                sender_socket.close()

            except sender_socket.timeout:
                # when timeout, send view change to all
                timeout_message = {
                    'message_type' : 'client_timeout',
                    'client_id' : my_id,
                    'client_request_no' : message.client_request_no 
                }
                data = json.dumps(timeout_message)
                print('Client {} send timout message to all\n'.format(my_id))
                for i in range(len(t_replica_config)): 
                    sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sender_socket.connect((t_replica_config[i]['ip'], t_replica_config[i]['port']))
                    sender_socket.sendall(str.encode(data))
                    sender_socket.close()
                time_recorder = time.time()

            reply_message = json.loads(data.decode('utf-8'))
            message_type = reply_message['message_type']

            if message_type == 'ack_client':
                if reply_message['request_number'] == message['client_request_no']:
                    print ('Message Recorded!\n')
                    break
                continue
            elif message_type == 'new_leader_to_client'
                leader_id = reply_message['leader']
                # send messages to new leader
                data = json.dumps(message)
                # send parsed messages to assuemed leader
                sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sender_socket.connect((t_replica_config[leader_id]['ip'], t_replica_config[leader_id]['port']))
                sender_socket.sendall(str.encode(data))
                sender_socket.close()
                time_recorder = time.time()
                print ('Change client {}th leader to {}\n'.format(my_id, reply_message['leader']))
            if time.time()-time_recorder >= 3:
                # when timeout, send timeout messages to all
                timeout_message = {
                    'message_type' : 'client_timeout',
                    'client_id' : my_id,
                    'client_request_no' : message.client_request_no 
                }
                data = json.dumps(timeout_message)
                print('Client {} send timout message to all'.format(my_id))
                for i in range(len(t_replica_config)): 
                    sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sender_socket.connect((t_replica_config[i]['ip'], t_replica_config[i]['port']))
                    sender_socket.sendall(str.encode(data))
                    sender_socket.close()
                time_recorder = time.time()
    my_socket.close()


@click.command()
@click.argument('client_id')
@click.argument('client_request_file')
@click.argument('replica_config_file')
def main(client_id, replica_config_file):
    my_id = int(client_id)
    # Read necessary information from config_file
    replica_config_file_handle = open(replica_config_file, 'r')
    replica_config_data = replica_config_file_handle.read()
    replica_config_file_handle.close()

    replica_config_list = json.loads(replica_config_data).get('replica_list')

    # { replica_id : { 'ip' : '', 'port' : '' } }
    replica_config = {}
    for raw_config in replica_config_list:
        replica_config[raw_config['id']] = {
            'ip' : raw_config['ip'],
            'port' : raw_config['port']
        }

    my_ip = 'localhost'
    my_port = 8000

    send_client_request(my_id, my_ip, my_port, replica_config) 


if __name__ == '__main__':
    main()
