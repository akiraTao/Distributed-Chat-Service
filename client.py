#!/usr/env/bin python3

import json
import time
import click
import socket
from paxos_util import paxos_client_request, paxos_client_timeout, u_get_id,\
                       paxos_print_log


def send_client_request(my_id, my_ip, my_port, replica_config, my_drop_rate):
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
    time_gap_interval = 0.1

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
            paxos_client_request(my_id, my_ip, my_port, s_request_no,
                                 s_leader_propose_no, value,
                                 s_replica_config, my_drop_rate)

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
                    # print('Client {} increases its timeout time'.format(my_id))

                    # Increase the timeout gap
                    time_gap += time_gap_interval
                    my_socket.settimeout(time_gap)

                    # when timeout, send timeout messages to all
                    paxos_client_timeout(my_id, my_ip, my_port, s_request_no,
                                         s_leader_propose_no, s_replica_config,
                                         my_drop_rate)

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

                    # It is possible that I have sent multiple same timeout messages
                    #   if the destination is on Mars
                    if new_leader_propose_no > s_leader_propose_no:
                        print('Change client {}\'s leader to {}'.\
                              format(my_id, u_get_id(new_leader_propose_no,
                                                     c_replica_num)))
                        # prepare message and destination to be resent
                        s_leader_propose_no = new_leader_propose_no

                        # Resend the client request
                        paxos_client_request(my_id, my_ip, my_port, s_request_no,
                                             s_leader_propose_no, value,
                                             s_replica_config, my_drop_rate)

                        # Reinitialize the timer
                        time_recorder = time.time()
                        continue

                if (time.time() - time_recorder) >= time_gap:
                    print('Client {} send timout message to all'.format(my_id))
                    # print('Client {} increases its timeout time'.format(my_id))

                    # Increase the timeout gap
                    time_gap += time_gap_interval
                    my_socket.settimeout(time_gap)

                    # when timeout, send timeout messages to all
                    paxos_client_timeout(my_id, my_ip, my_port, s_request_no,
                                         s_leader_propose_no, s_replica_config,
                                         my_drop_rate)

                    # Reinitialize the timer
                    time_recorder = time.time()


        elif user_command == 'p':
            paxos_print_log(my_id, replica_config)


        elif user_command == 'e':
            break

    # Close the socket for completeness
    my_socket.close()


@click.command()
@click.argument('client_id')
@click.argument('my_ip')
@click.argument('my_port')
@click.argument('replica_config_file')
@click.argument('client_drop_rate')
def main(client_id, my_ip, my_port, replica_config_file, client_drop_rate):
    '''Main function is used for data preprocessing.'''

    # Convert to int which is required by repica_config
    my_id = int(client_id)
    # Convert to int which is required by socket bind
    my_port = int(my_port)
    # Convert drop_rate to int
    my_drop_rate = int(client_drop_rate)
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
    send_client_request(my_id, my_ip, my_port, replica_config, my_drop_rate) 


if __name__ == '__main__':
    main()
