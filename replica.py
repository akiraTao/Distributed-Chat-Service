#!/usr/env/bin python3

import json
import time
import click
import socket
from multiprocessing import Process

def handle_replica(replica_id, replica_list):
    # The replica_id of myself
    my_id = replica_id
    # The replica_id of whom I think is the leader
    leader_id = 0
    # { slot_no : count of ack message from other replicas }
    ack_msg_count = {}
    # [ (slot_no) : value ]
    accepted = []
    # [ (slot_no) : propose_no ]
    proposer = []
    # Check which slot has been learned
    learned = set()
    # The curent round number used to construct (curr_rnd, my_id) tuple
    curr_rnd = 0
    # The next slot used to propose if I am leader
    next_slot = 0
    # The first slot that I am sure hasn't been accepted
    first_unaccepted = 0
    # The first slot that I am sure hasn't been chosen
    first_unchosen = 0
    # { client_id : newest_request_id }
    client_request = {}

    # Propose No. used when propose if I am leader
    propose_no = (curr_rnd, my_id)

    # { replica_id : { 'ip': '', 'port': num } } excluding myself
    replica_config = {}
    # repica_data e.g. {'id': 0, 'ip': 'localhost', 'port': 6000}
    for replica_data in replica_list:
        replica_config[replica_data['id']] = {
            'ip' : replica_data['ip'],
            'port' : replica_data['port']
        }

    # Build the socket to receive external messages
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    my_socket.bind((replica_config[my_id]['ip'], replica_config[my_id]['port']))
    my_socket.listen(5)

    # My own information is no longer needed
    del replica_config[my_id]

    # If I am the leader at the very beginning
    if leader_id == my_id:
        # TODO: This is to ensure every other process is up (not safe)
        time.sleep(1)

        message = {
            'message_type' : 'propose',
            'proposer' : propose_no,
            'slot' : next_slot
        }
        data = json.dumps(message).encode('utf-8')

        for replica_id, replica_addr in replica_config.items():
            receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            receiver_socket.connect((replica_addr['ip'], replica_addr['port']))
            receiver_socket.sendall(data)
            receiver_socket.close()
        
    while True:
        sender_socket = my_socket.accept()[0]
        data = sender_socket.recv(1024)
        sender_socket.close()

        message = json.loads(data.decode('utf-8'))
        print(my_id, message)

    my_socket.close()


@click.command()
@click.argument('config_file')
def main(config_file):
    # Extract configuration data from specified config file
    config_str = ''
    with open(config_file, 'r') as config_handle:
        config_str = config_handle.read()
    # config_data stores the raw config data
    config_data = json.loads(config_str)
    # Call top-level Replica process which will produce subprocesses
    replica_list = config_data['replica_list']
    replica_num = len(replica_list)

    # Spew out replica_num subprocesses
    for replica_id in range(replica_num):
        p = Process(target=handle_replica, args=(replica_id, replica_list))
        p.start()

if __name__ == '__main__':
    main()
