#!/usr/env/bin python3

import sys
import copy
import json
import time
import click
import socket
from multiprocessing import Process
from collections import OrderedDict
from paxos_util import paxos_prepare, paxos_ack_prepare, paxos_propose,\
                       paxos_accept, paxos_debug


def handle_replica(replica_id, replica_config_list):
    # Propose No. used when propose if I am leader
    # The tuple consists of (round_no, s_my_id)
    s_my_propose_no = [0, replica_id]
    # Propose No. used to see who is the current leader
    # The tuple consists of (round_no, s_leader_id)
    s_leader_propose_no = [0, 0]
    # Used by the leader. Possible states are 'prepare', 'established'
    s_leader_state = 'prepare'
    # { slot_no : count of ack message from other replicas }
    s_ack_msg_count = {}
    # { slot_no : count of accept message from other replicas }
    s_accept_msg_count = {}
    # { slot_no : value }
    s_accepted = {}
    # { slot_no : propose_no }
    s_proposer = {}
    # { slot_no : (client_id, request_id) }
    s_client_request = {}
    # Check which slot has been learned
    s_learned = set()
    # The next slot used to propose if I am leader
    s_next_slot = 0
    # The first slot that I am sure hasn't been accepted
    s_first_unaccepted = 0
    # The first slot that I am sure hasn't been chosen
    s_first_unchosen = 0
    # set(tuple(client_id, newest_request_id))
    # Updated only when the client request has been chosen
    s_chosen_client_request = set()

    # The queue that buffers client request message
    s_request_queue = []

    # { replica_id : { 'ip': '', 'port': num } }
    # TODO: Add slot_num and drop_rate feature
    s_replica_config = {}
    # repica_data e.g. {'id': 0, 'ip': 'localhost', 'port': 6000}
    for replica_data in replica_config_list:
        s_replica_config[replica_data['id']] = {
            'ip' : replica_data['ip'],
            'port' : replica_data['port']
        }

    # Majority number of replicas
    c_majority_num = (len(replica_config_list) + 1) / 2

    # Build the socket to receive external messages
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    my_socket.bind((s_replica_config[get_id(s_my_propose_no)]['ip'],
                    s_replica_config[get_id(s_my_propose_no)]['port']))
    my_socket.listen(5)


    # The below information is only for debugging
    d_my_ip = s_replica_config[get_id(s_my_propose_no)]['ip']
    d_my_port = s_replica_config[get_id(s_my_propose_no)]['port']
    # My own information is no longer needed
    del s_replica_config[get_id(s_my_propose_no)]

    # If I am the leader at the very beginning
    if get_id(s_leader_propose_no) == get_id(s_my_propose_no):
        # TODO: This is to ensure every other process is up (not safe)
        time.sleep(1)
        # Increment round number
        s_my_propose_no[0] += 1
        s_leader_propose_no[0] += 1
        # Propose to every other replica
        paxos_prepare(s_my_propose_no,
                      s_next_slot,
                      s_replica_config)
    
    # Used to store the temporarily most recent value, propose_no, 
    #   (client_id, request_id) pair and no_more_accepted, used in ack_prepare
    t_value = None
    t_propose_no = [0, 0]
    t_client_request = None
    t_no_more_accepted = True

    # Boolean variable denoting whether client request should be immediately applied
    s_waiting_client = False

    # TODO: Probably can be replaced with some sort of shutdown message
    # This basically constantly fetched the next message in the socket
    #   buffer and take action according to the message type
    while True:
        sender_socket = my_socket.accept()[0]
        data = sender_socket.recv(1024)
        sender_socket.close()

        message = json.loads(data.decode('utf-8'))
        message_type = message['message_type']


        if message_type == 'prepare':
            proposed_no = message['proposer']
            proposed_slot = message['slot']

            # If the proposal is old, just ignore it
            if proposed_no <= s_leader_propose_no:
                continue

            # Update the leader proposal number
            s_leader_propose_no = copy.deepcopy(proposed_no)

            # If I haven't used this slot before, initialize with None
            if proposed_slot not in s_accepted:
                s_accepted[proposed_slot] = None
                s_proposer[proposed_slot] = None
                s_client_request[proposed_slot] = None

            no_more_accepted = (proposed_slot >= s_first_unaccepted)
            
            # Send ack_propose message back to the leader
            paxos_ack_prepare(s_accepted[proposed_slot],
                              s_proposer[proposed_slot],
                              s_client_request[proposed_slot],
                              no_more_accepted,
                              proposed_slot,
                              get_id(s_leader_propose_no),
                              s_replica_config)


        elif message_type == 'ack_prepare':
            acked_value = message['accepted']
            acked_propose_no = message['proposer']
            acked_client_request = message['client_request']
            acked_no_more_accepted = message['no_more_accepted']
            acked_slot = message['slot']

            # If I am not the leader or the round number is old, ignore the message
            if s_my_propose_no != s_leader_propose_no:
                continue

            # If this is the remaining  possible ack from previous prepare, or
            # If the slot is already proposed, ignore further ack_prepare
            if acked_slot != s_next_slot or\
                    (acked_slot in s_ack_msg_count and s_ack_msg_count[acked_slot] == c_majority_num):
                continue

            # Initialize s_ack_msg_count
            if acked_slot not in s_ack_msg_count:
                s_ack_msg_count[acked_slot] = 0

            # Always retain the most recent value
            if acked_propose_no is not None and acked_propose_no > t_propose_no:
                t_value = acked_value
                t_propose_no = copy.deepcopy(acked_propose_no)
                t_client_request = copy.deepcopy(acked_client_request)

            # Check the no_more_accepted flag
            t_no_more_accepted = t_no_more_accepted & acked_no_more_accepted

            # Increment the acknowledge count
            s_ack_msg_count[acked_slot] += 1
            if s_ack_msg_count[acked_slot] == c_majority_num:
                # If all replicas reply with no_more_accepted, then 'established'
                if t_no_more_accepted:
                    s_leader_state = 'established'
                # If the majority contains value, propose that value
                if t_value is not None:
                    # First the leader itself should accept the value
                    s_accepted[s_next_slot] = t_value
                    s_proposer[s_next_slot] = s_my_propose_no
                    s_client_request[s_next_slot] = copy.deepcopy(t_client_request)

                    # Increment the s_accept_msg_count
                    if s_next_slot not in s_accept_msg_count:
                        s_accept_msg_count[s_next_slot] = 0
                    s_accept_msg_count[s_next_slot] += 1

                    # Update first_unaccepted
                    # TODO: Check this! May be buggy
                    s_first_unaccepted += 1
                    # s_next_slot should be incremented since leader cannot propose two values in the same slot
                    s_next_slot += 1

                    paxos_propose(t_value, t_propose_no, t_client_request,
                                  s_first_unchosen, s_next_slot-1, s_replica_config)
                else:
                    # Propose the client request, or declare waiting_client
                    if s_request_queue != []:
                        client_message = s_request_queue.pop(0)
                        value = client_message['value']
                        propose_no = copy.deepcopy(s_my_propose_no)
                        client_request = [client_message['client_id'],
                                          client_message['client_message_no']]

                        # First the leader itself should accept the value
                        s_accepted[s_next_slot] = value
                        s_proposer[s_next_slot] = propose_no
                        s_client_request[s_next_slot] = client_request

                        # Increment the s_accept_msg_count
                        if s_next_slot not in s_accept_msg_count:
                            s_accept_msg_count[s_next_slot] = 0
                        s_accept_msg_count[s_next_slot] += 1

                        # Update first_unaccepted
                        s_first_unaccepted += 1
                        # s_next_slot should be incremented since leader cannot propose two values in the same slot
                        s_next_slot += 1

                        paxos_propose(value, s_my_propose_no, client_request,
                                      s_first_unchosen, s_next_slot-1, s_replica_config)
                    else:
                        s_waiting_client = True
                
                # Clear the temp variables for future usage
                t_value = None
                t_propose_no = [0, 0]
                t_client_request = None
                t_no_more_accepted = True


        elif message_type == 'propose':
            prop_value = message['to_accept']
            prop_proposed_no = message['proposer']
            prop_client_request = message['client_request']
            prop_first_unchosen = message['first_unchosen']
            prop_slot = message['slot']

            # If this is the decree from old leader, just ignore it
            # TODO: Do I need to reject old leader?
            if prop_proposed_no < s_leader_propose_no:
                continue

            # Update the leader proposal number
            s_leader_propose_no = copy.deepcopy(prop_proposed_no)

            # Accept the proposed value
            s_accepted[prop_slot] = prop_value
            s_proposer[prop_slot] = copy.deepcopy(prop_proposed_no)
            s_client_request[prop_slot] = copy.deepcopy(prop_client_request)

            # Increment the s_accept_msg_count
            if prop_slot not in s_accept_msg_count:
                s_accept_msg_count[prop_slot] = 0
            s_accept_msg_count[prop_slot] += 1

            # Update first_unaccepted to the most correct value
            while s_first_unaccepted <= prop_slot and s_accepted.get(s_first_unaccepted, None) != None:
                s_first_unaccepted += 1

            paxos_accept(s_accepted[prop_slot],
                         s_proposer[prop_slot],
                         s_client_request[prop_slot],
                         prop_slot,
                         s_replica_config)

            # TODO: send help message to the leader based on first_unchosen


        elif message_type == 'accept':
            accept_value = message['accepted']
            accept_propose_no = message['proposer']
            accept_client_request = message['client_request']
            accept_slot = message['slot']

            # If I have already chosen in this slot, just ignore the message
            if accept_slot in s_learned:
                continue

            # Initialize s_accept_msg_count with new slot
            if accept_slot not in s_accept_msg_count:
                s_accept_msg_count[accept_slot] = 0

            # Ignore the old proposal no
            if accept_propose_no < s_leader_propose_no:
                continue
            # If find a newer accept message, clear the count and updates leader
            elif accept_propose_no > s_leader_propose_no:
                # Update the leader
                s_leader_propose_no = copy.deepcopy(accept_propose_no)
                # Clear the count and initialize with one
                s_accept_msg_count[accept_slot] = 1
            # If the accept message is just about this propose_no:
            else:
                s_accept_msg_count[accept_slot] += 1
                # If the majority accept arrives, learn the value
                if s_accept_msg_count[accept_slot] == c_majority_num:
                    # Learn the accepted value
                    s_learned.add(accept_slot)
                    # Once the client request has been learnt, it should never be executed again
                    s_chosen_client_request.add(tuple(accept_client_request))
                    # Update first_unchosen
                    while s_first_unchosen <= accept_slot and s_first_unchosen in s_learned:
                        s_first_unchosen += 1

                    # TODO: tell the client the job has been done
                    print('Replica {} done with slot {}, value {}'.format(get_id(s_my_propose_no), accept_slot, s_accepted[accept_slot]))

                    # If I am the leader, need to process another message
                    if get_id(s_leader_propose_no) == get_id(s_my_propose_no):
                        if s_leader_state == 'established':
                            # Propose the client request, or declare waiting_client
                            # s_next_slot has already been pointed to next slot
                            if s_request_queue != []:
                                client_message = s_request_queue.pop(0)
                                value = client_message['value']
                                propose_no = copy.deepcopy(s_my_propose_no)
                                client_request = [client_message['client_id'],
                                                  client_message['client_message_no']]

                                # First the leader itself should accept the value
                                s_accepted[s_next_slot] = value
                                s_proposer[s_next_slot] = propose_no
                                s_client_request[s_next_slot] = client_request

                                # Increment the s_accept_msg_count
                                if s_next_slot not in s_accept_msg_count:
                                    s_accept_msg_count[s_next_slot] = 0
                                s_accept_msg_count[s_next_slot] += 1

                                # Update first_unaccepted
                                s_first_unaccepted += 1
                                # s_next_slot should be incremented since leader cannot propose two values in the same slot
                                s_next_slot += 1

                                paxos_propose(value, s_my_propose_no, client_request,
                                              s_first_unchosen, s_next_slot-1, s_replica_config)
                            else:
                                s_waiting_client = True

                        else: # if s_leader_state == 'prepare'
                            # s_next_slot has already been pointed to next slot
                            paxos_prepare(s_my_propose_no,
                                          s_next_slot,
                                          s_replica_config)


        elif message_type == 'client_request':
            # Cannot process duplicate client requests
            if (message['client_id'], message['client_request_no']) in s_chosen_client_request:
                continue

            # Append the client request message to queue
            s_request_queue.append(message)

            # TODO: If I am not the leader, tell client the leader

            # If a job is already waiting for client message
            if s_waiting_client:
                client_message = s_request_queue.pop(0)
                value = client_message['value']
                propose_no = s_my_propose_no
                client_request = [client_message['client_id'],
                                  client_message['client_request_no']]

                # First the leader itself should accept the value
                s_accepted[s_next_slot] = value
                s_proposer[s_next_slot] = propose_no
                s_client_request[s_next_slot] = client_request

                # Increment the s_accept_msg_count
                if s_next_slot not in s_accept_msg_count:
                    s_accept_msg_count[s_next_slot] = 0
                s_accept_msg_count[s_next_slot] += 1

                # Update first_unaccepted
                s_first_unaccepted += 1
                # s_next_slot should be incremented since leader cannot propose two values in the same slot
                s_next_slot += 1

                paxos_propose(value, propose_no, client_request,
                              s_first_unchosen, s_next_slot-1, s_replica_config)

                # Initialize s_waiting_client for future usage
                s_waiting_client = False


        elif message_type == 'debug':
            accepted_printer = OrderedDict(sorted(s_accepted.items()))
            for index, accepted_value in accepted_printer.items():
                print(get_id(s_my_propose_no))
                print(index, end='\t')
                print('A: {}, P: {}, C: {}'.format(accepted_value,
                                                   s_my_propose_no[index],
                                                   s_client_request[index]), end=' ')
                if index in s_learned:
                    print('L')
                print('\n')

        # TODO: Add other message necessary
        else:
            pass


    my_socket.close()


# Returns the leader id of the replica
def get_id(s_propose_no):
    return s_propose_no[1]


@click.command()
@click.argument('config_file')
def main(config_file):
    # Extract configuration data from specified config file
    config_str = ''
    with open(config_file, 'r') as config_handle:
        config_str = config_handle.read()
    # config_data stores the raw config data
    config_data = json.loads(config_str)

     # Fetch the information of all replicas
    replica_config_list = config_data['replica_list']
    replica_num = len(replica_config_list)

    # Depending on the mode, we do differnt things
    mode = config_data['mode']

    if mode == 'script':
        # Spew out replica_num subprocesses
        for replica_id in range(replica_num):
            p = Process(target=handle_replica, args=(replica_id, replica_config_list))
            p.start()

    elif mode == 'manual':
        # TODO : Implement manual mode (spew single process)
        pass

    else:
        print('Mode can only be either script or manual')
        sys.exit(1)


if __name__ == '__main__':
    main()
