#!/usr/env/bin python3

import json
import time
import click
import socket
from paxos_util import paxos_client_request, paxos_client_timeout, u_get_id


def send_client_request(my_id, my_ip, my_port, replica_config):
    '''The actual client request execution interface.'''

    # { replica_id : { 'ip' : '', 'port' : '' } }
    s_replica_config = replica_config
    # Number of replicas
    c_replica_num = len(s_replica_config)
    # Every client retains its own request sequence number
    s_request_no = 0
    # The id of the leader who I believe is the leader
    s_leader_propose_no = 0
    # Initial timeout time
    time_gap = 1

    # Build the socket to receive external messages
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    my_socket.bind((my_ip, my_port))

    # TODO: Client timeout should be dynamic or increasing
    my_socket.settimeout(time_gap)

    while True:
        command_str = 'Enter your command' + '\n' +\
                      '(s) send message, (p) print log, (e) end client: '
        # Let client choose what to do
        user_command = input(command_str)

        if user_command == 's':
            # Get user's message from command line
            value = input('Enter your message: ')

            # Send parsed message to assumed leader
            paxos_client_request(my_id,
                                 my_ip,
                                 my_port,
                                 s_request_no,
                                 s_leader_propose_no,
                                 value,
                                 s_replica_config)

            # Start recording the time for timeout
            time_recorder = time.time()

            while True:
                data = None

                try:
                    # Accept message from replicas
                    # This is a blocking call, but the client has set timeout
                    data = my_socket.recvfrom(1024)[0]

                except socket.timeout:
                    print('Client {} send timeout message to all'.format(my_id))
                    print('Client {} increases its timeout time')
                    time_gap += 0.1
                    # when timeout, increase 
                    # when timeout, send timeout messages to all
                    paxos_client_timeout(my_id,
                                         my_ip,
                                         my_port,
                                         s_request_no,
                                         s_leader_propose_no,
                                         s_replica_config)

                    # Reinitialize the timer
                    time_recorder = time.time()
                    continue

                reply_message = json.loads(data.decode('utf-8'))

                message_type = reply_message['message_type']

                if message_type == 'ack_client':
                    if reply_message['request_no'] == s_request_no:
                        print ('Message Recorded!')
                        # Increment request_no for next request
                        s_request_no += 1
                        break

                elif message_type == 'new_leader_to_client':
                    # This is the propose_no of new leader
                    new_leader_propose_no = reply_message['propose_no']

                    # By the construction of the protocol, this must hold true
                    if new_leader_propose_no > s_leader_propose_no:

                        print('Change client {}\'s leader to {}'.\
                              format(my_id, u_get_id(new_leader_propose_no,
                                                     c_replica_num)))
                        # prepare message and destination to be resent
                        s_leader_propose_no = new_leader_propose_no

                        # Resend the client request
                        paxos_client_request(my_id,
                                             my_ip,
                                             my_port,
                                             s_request_no,
                                             s_leader_propose_no,
                                             value,
                                             s_replica_config)

                        # Reinitialize the timer
                        time_recorder = time.time()
                        continue

                if (time.time() - time_recorder) >= time_gap:
                    print('Client {} send timout message to all'.format(my_id))
                    print('Client {} increases its timeout time')
                    time_gap += 0.1
                    # when timeout, send timeout messages to all
                    paxos_client_timeout(my_id,
                                         my_ip,
                                         my_port,
                                         s_request_no,
                                         s_leader_propose_no,
                                         s_replica_config)

                    # Reinitialize the timer
                    time_recorder = time.time()

        elif user_command == 'e':
            break

    # Close the socket for completeness
    my_socket.close()


@click.command()
@click.argument('client_id')
@click.argument('my_ip')
@click.argument('my_port')
@click.argument('replica_config_file')
def main(client_id, my_ip, my_port, replica_config_file):
    '''Main function is used for data preprocessing.'''

    # Convert to int which is required by repica_config
    my_id = int(client_id)
    # Convert to int which is required by socket bind
    my_port = int(my_port)
    # Read necessary information from config_file
    replica_config_file_handle = open(replica_config_file, 'r')
    replica_config_data = replica_config_file_handle.read()
    replica_config_file_handle.close()

    replica_config_list = json.loads(replica_config_data).get('replica_list')

    # Desired format is: { replica_id : { 'ip' : '', 'port' : '' } }
    replica_config = {}
    for raw_config in replica_config_list:
        replica_config[raw_config['id']] = {
            'ip' : raw_config['ip'],
            'port' : raw_config['port']
        }

    # Call the actual client request execution
    send_client_request(my_id, my_ip, my_port, replica_config) 


if __name__ == '__main__':
    main()
