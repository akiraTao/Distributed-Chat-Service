#!/usr/env/bin python3

import json
import time
import click
import socket
from paxos_util import paxos_client_request, get_id


def send_client_request(my_id, my_ip, my_port, replica_config):
    # { replica_id : { 'ip' : '', 'port' : '' } }
    replica_config = replica_config

    # Number of replicas
    replica_len = len(replica_config)

    # Build the socket to receive external messages
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    my_socket.bind((my_ip, my_port))
    my_socket.listen(5)

    # Every client retains its own sequence number
    request_no = 0

    # The id of the leader who I believe is the leader
    leader_propose_no = 0

    while True:
        # Let client choose what to do
        user_command = input('Enter your command\n (s) send messages, (p) print log, (e) end client: ')

        if user_command == 's':
            # Get user's message from command line
            value = input('Enter your message: ')

            # Send parsed message to assumed leader
            paxos_client_request(my_id,
                                 my_ip,
                                 my_port,
                                 request_no,
                                 leader_propose_no,
                                 value,
                                 replica_config)

            # Start recording the time for timeout
            time_recorder = time.time()

            while True:
                data = None
                try:
                    print(1)
                    # Accept message from replicas
                    my_socket.settimeout(3)
                    sender_socket = my_socket.accept()[0]
                    data = sender_socket.recv(1024)
                    sender_socket.close()

                except sender_socket.timeout:
                    print(2)
                    # when timeout, send view change to all
                    timeout_message = {
                        'message_type' : 'client_timeout',
                        'client_id' : my_id,
                        'client_request_no' : message['client_request_no'],
                        'propose_no' : leader_propose_no
                    }

                    data = json.dumps(timeout_message)

                    print('Client {} send timeout message to all'.format(my_id))

                    for replica_addr in replica_config.values(): 
                        receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        receiver_socket.connect((replica_addr['ip'], replica_addr['port']))
                        receiver_socket.sendall(str.encode(data))
                        receiver_socket.close()

                    # Reinitialize the timer
                    time_recorder = time.time()
                    continue

                print(3)

                reply_message = json.loads(data.decode('utf-8'))
                message_type = reply_message['message_type']

                if message_type == 'ack_client':
                    if reply_message['request_no'] == request_no:
                        print ('Message Recorded!')

                        # Increment request_no for next request
                        request_no += 1

                        break

                elif message_type == 'new_leader_to_client':
                    leader_propose_no = reply_message['leader_propose_no']
                    if reply_message['propose_no'] > leader_propose_no:
                        # prepare message and destination to be resent
                        leader_propse_no = reply_message['propose_no']
                        leader_id = get_id(leader_propose_no, replica_len)
                        message['propose_no'] = leader_propose_no
                        data = json.dumps(message)
                        # send parsed messages to assuemed leader
                        sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sender_socket.connect((replica_config[leader_id]['ip'], replica_config[leader_id]['port']))
                        sender_socket.sendall(str.encode(data))
                        sender_socket.close()
                        # Reinitialize the timer
                        time_recorder = time.time()
                        print ('Change client {}th leader to {}'.format(my_id, get_id(reply_message['proposal_no'],replica_len)))
                    continue

                if (time.time() - time_recorder) >= 3:
                    # when timeout, send timeout messages to all
                    timeout_message = {
                        'message_type' : 'client_timeout',
                        'client_id' : my_id,
                        'client_request_no' : message['client_request_no'],
                        'propose_no' : leader_propose_no
                    }
                    data = json.dumps(timeout_message)

                    print('Client {} send timout message to all'.format(my_id))

                    for replica_addr in replica_config.values(): 
                        receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        receiver_socket.connect((replica_addr['ip'], replica_addr['port']))
                        receiver_socket.sendall(str.encode(data))
                        receiver_socket.close()
        if user_command == 'e':
            break

    # Close the socket for completeness
    my_socket.close()


@click.command()
@click.argument('client_id')
@click.argument('my_ip')
@click.argument('my_port')
@click.argument('replica_config_file')
def main(client_id, my_ip, my_port, replica_config_file):
    # Convert to int which is required by repica_config
    my_id = int(client_id)
    # Convert to int which is required by socket bind
    my_port = int(my_port)
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

    send_client_request(my_id, my_ip, my_port, replica_config) 


if __name__ == '__main__':
    main()
