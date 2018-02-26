#!/usr/env/bin python3

import os
import sys
import copy
import json
import time
import click
import socket
from multiprocessing import Process
from collections import OrderedDict
from paxos_util import paxos_prepare, paxos_ack_prepare, paxos_propose,\
                       paxos_accept, paxos_ack_client, u_get_id,\
                       paxos_tell_client_new_leader, paxos_help_me_choose,\
                       paxos_you_can_choose


def handle_replica(replica_id, replica_config_list):
    # Propose No. used to see who is the current leader
    s_leader_propose_no = 0
    # Used by the leader. Possible states are 'prepare', 'established', 'dictated'
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
    # { slot_no : (client_ip, client_port) }
    s_client_addr = {}
    # Check which slot has been learned
    s_learned = set()
    # The next slot used to propose if I am leader
    s_next_slot = 0
    # The last slot that I am sure has been accepted
    s_last_accepted = 0
    # The first slot that I am sure hasn't been chosen
    s_first_unchosen = 0
    # set(tuple(client_id, newest_request_id))
    # Updated only when the client request has been chosen
    s_chosen_client_request = set()

    # The queue that buffers client request message
    s_request_queue = []

    # The queue that buffers empty slot message in the prepare phase of leader
    s_slot_buffer_queue = []

    # { replica_id : { 'ip': '', 'port': num } }
    s_replica_config = {}
    # repica_data e.g. {'id': 0, 'ip': 'localhost', 'port': 6000}
    for replica_data in replica_config_list:
        s_replica_config[replica_data['id']] = {
            'ip' : replica_data['ip'],
            'port' : replica_data['port'],
            'skip_slot' : replica_data['skip_slot'],
            'drop_rate' : replica_data['drop_rate']
        }

    # Boolean variable denoting whether client request should be immediately applied
    s_waiting_client = False

    # Number of replicas
    c_replica_num = len(replica_config_list)

    # Majority number of replicas
    c_majority_num = (c_replica_num + 1) / 2

    # The ip address and port information
    c_my_ip = s_replica_config[replica_id]['ip']
    c_my_port = s_replica_config[replica_id]['port']
    c_my_skip_slot = s_replica_config[replica_id]['skip_slot']
    c_my_drop_rate = s_replica_config[replica_id]['drop_rate']

    # Let s_next_slot skip the skip slots
    while s_next_slot in c_my_skip_slot:
        s_next_slot += 1

    # Build the socket to receive external message
    # UDP socket also has buffer to store incoming messages
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    my_socket.bind((c_my_ip, c_my_port))

    # My own information is no longer needed
    del s_replica_config[replica_id]

    # Used to store the temporarily most recent value, propose_no, 
    #   (client_id, request_id) pair and no_more_accepted, used in ack_prepare
    t_value = None
    t_propose_no = 0
    t_client_request = None
    t_client_addr = None
    t_no_more_accepted = True

    # If I am the leader at the very beginning
    if u_get_id(s_leader_propose_no, c_replica_num) == replica_id:
        # TODO: This is to ensure every other process is up (not safe)
        time.sleep(1)
        # sys.exit(1)

        # Initialize temp variables with leader's own slot
        # It is guaranteed to be default valur though, just for consistency
        t_value = s_accepted.get(s_next_slot, None)
        t_propose_no = s_proposer.get(s_next_slot, 0)
        t_client_request = s_client_request.get(s_next_slot, None)
        t_client_addr = s_client_addr.get(s_next_slot, None)

        # Before sending prepare msg, the leader already has one ack (itself)
        s_ack_msg_count[s_next_slot] = 1

        assert( s_request_queue == [] )
        assert( s_slot_buffer_queue == [] )

        # Propose to every other replica
        paxos_prepare(replica_id, s_leader_propose_no, s_next_slot,
                      s_replica_config, c_my_drop_rate)


    # This basically constantly fetched the next message in the socket
    #   buffer and take action according to the message type
    while True:
        # This is a blocking call
        data = my_socket.recvfrom(1024)[0]

        message = json.loads(data.decode('utf-8'))
        message_type = message['message_type']

        # Debugging message
        print('Replica {} received message {}'.format(replica_id, message_type))

        if message_type == 'prepare':
            proposed_no = message['proposer']
            proposed_slot = message['slot']

            # If the proposal is old, just ignore it
            if proposed_no < s_leader_propose_no:
                continue

            # Update the leader proposal number
            s_leader_propose_no = proposed_no

            # Clear the variables that are specific for leaders (in case it was leader)
            # if s_leader_propose_no (prev) == s_my_propose_no:
            s_leader_state = 'prepare'
            s_ack_msg_count[proposed_slot] = 0
            s_accept_msg_count[proposed_slot] = 0

            # If I haven't used this slot before, initialize with None
            if proposed_slot not in s_accepted:
                s_accepted[proposed_slot] = None
                s_proposer[proposed_slot] = 0
                s_client_request[proposed_slot] = None
                s_client_addr[proposed_slot] = None

            no_more_accepted = (proposed_slot >= s_last_accepted)
            
            # Send ack_propose message back to the leader
            paxos_ack_prepare(replica_id,
                              s_accepted[proposed_slot],
                              s_proposer[proposed_slot],
                              s_client_request[proposed_slot],
                              s_client_addr[proposed_slot],
                              no_more_accepted,
                              proposed_slot,
                              s_leader_propose_no,
                              u_get_id(s_leader_propose_no, c_replica_num),
                              s_replica_config,
                              c_my_drop_rate)


        elif message_type == 'ack_prepare':
            acked_value = message['accepted']
            acked_propose_no = message['value_proposer']
            acked_client_request = message['client_request']
            acked_client_addr = message['client_addr']
            acked_no_more_accepted = message['no_more_accepted']
            acked_slot = message['slot']
            acked_leader_propose_no = message['proposer']

            # If I am not the leader or the round number is old, ignore the message
            if u_get_id(acked_leader_propose_no, c_replica_num) != replica_id:
                continue

            assert ( acked_leader_propose_no <= s_leader_propose_no )
            if acked_leader_propose_no < s_leader_propose_no:
                continue

            # If this is the remaining  possible ack from previous prepare, or
            # If the slot is already proposed, ignore further ack_prepare
            if acked_slot != s_next_slot or\
                    (s_ack_msg_count.get(acked_slot, None) == c_majority_num):
                continue

            assert ( s_leader_state == 'prepare' )
            assert ( s_ack_msg_count[acked_slot] > 0 ) 

            # Always retain the most recent value
            if acked_propose_no is not None and acked_propose_no > t_propose_no:
                t_value = acked_value
                t_propose_no = acked_propose_no
                t_client_request = copy.deepcopy(acked_client_request)
                t_client_addr = copy.deepcopy(acked_client_addr)

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
                    # Cannot process duplicate client requests
                    if tuple(t_client_request) in s_chosen_client_request:
                        # But tell client that message has already been learnt
                        paxos_ack_client(replica_id, t_client_request[1],
                                          (t_client_addr[0], t_client_addr[1]), c_my_drop_rate)

                    else:
                        # First the leader itself should accept the value
                        s_accepted[s_next_slot] = t_value
                        s_proposer[s_next_slot] = s_leader_propose_no
                        s_client_request[s_next_slot] = copy.deepcopy(t_client_request)
                        s_client_addr[s_next_slot] = copy.deepcopy(t_client_addr)

                        # Increment the s_accept_msg_count
                        s_accept_msg_count[s_next_slot] = 1

                        # Update last_accepted if necessary
                        if s_next_slot > s_last_accepted: 
                            s_last_accepted = s_next_slot

                        paxos_propose(replica_id, t_value, s_leader_propose_no,
                                      t_client_request, t_client_addr, 
                                      s_first_unchosen, s_next_slot,
                                      s_replica_config, c_my_drop_rate)

                        paxos_accept(replica_id,
                                     s_accepted[acked_slot],
                                     s_proposer[acked_slot],
                                     s_client_request[acked_slot],
                                     s_client_addr[acked_slot],
                                     acked_slot, s_replica_config, c_my_drop_rate)

                        # Clear the temp variables for future usage
                        t_value = None
                        t_propose_no = 0
                        t_client_request = None
                        t_client_addr = None
                        t_no_more_accepted = True

                        continue

                # Add this slot to slot_buffer_queue
                s_slot_buffer_queue.append(acked_slot)

                if s_leader_state == 'established':
                    while s_slot_buffer_queue != []:
                        if s_request_queue == []:
                            s_waiting_client = True
                            break

                        else:
                            client_message = s_request_queue.pop(0)
                            value = client_message['value']
                            propose_no = s_leader_propose_no
                            client_request = [client_message['client_id'],
                                              client_message['client_request_no']]
                            client_addr = [client_message['client_ip'],
                                           client_message['client_port']]

                            if tuple(client_request) in s_chosen_client_request:
                                paxos_ack_client(replica_id, client_request[1],
                                                 (client_addr[0], client_addr[1]), c_my_drop_rate)
                                continue

                            slot_to_fill = s_slot_buffer_queue.pop(0)

                            # First the leader itself should accept the value
                            s_accepted[slot_to_fill] = value
                            s_proposer[slot_to_fill] = propose_no
                            s_client_request[slot_to_fill] = client_request
                            s_client_addr[slot_to_fill] = client_addr

                            # Increment the s_accept_msg_count
                            s_accept_msg_count[slot_to_fill] = 1

                            # Update last_accepted
                            if slot_to_fill > s_last_accepted: 
                                s_last_accepted = slot_to_fill

                            paxos_propose(replica_id, value, s_leader_propose_no, client_request, client_addr,
                                          s_first_unchosen, slot_to_fill, s_replica_config, c_my_drop_rate)

                            paxos_accept(replica_id,
                                         s_accepted[slot_to_fill],
                                         s_proposer[slot_to_fill],
                                         s_client_request[slot_to_fill],
                                         s_client_addr[slot_to_fill],
                                         slot_to_fill,
                                         s_replica_config,
                                         c_my_drop_rate)

                            # TODO: check whether this is correct
                            s_waiting_client = False

                    # Clear the temp variables for future usage
                    t_value = None
                    t_propose_no = 0
                    t_client_request = None
                    t_client_addr = None
                    t_no_more_accepted = True

                    # If the s_slot_buffer_queue is exhausted, need to enter 'dictated' stage
                    if s_slot_buffer_queue == []:
                        s_leader_state = 'dictated'

                        # Now s_next_slot is independent of s_first_unchosen
                        s_next_slot += 1
                        # Let s_next_slot skip the skip slots
                        while s_next_slot in c_my_skip_slot:
                            s_next_slot += 1

                        while s_request_queue != []:
                            client_message = s_request_queue.pop(0)
                            value = client_message['value']
                            propose_no = s_leader_propose_no
                            client_request = [client_message['client_id'],
                                              client_message['client_request_no']]
                            client_addr = [client_message['client_ip'],
                                           client_message['client_port']]

                            if tuple(client_request) in s_chosen_client_request:
                                paxos_ack_client(replica_id, client_request[1],
                                                 (client_addr[0], client_addr[1]), c_my_drop_rate)
                                continue

                            # First the leader itself should accept the value
                            s_accepted[s_next_slot] = value
                            s_proposer[s_next_slot] = propose_no
                            s_client_request[s_next_slot] = client_request
                            s_client_addr[s_next_slot] = client_addr

                            # Increment the s_accept_msg_count
                            s_accept_msg_count[s_next_slot] = 1

                            # Update last_accepted
                            if s_next_slot > s_last_accepted: 
                                s_last_accepted = s_next_slot

                            paxos_propose(replica_id, value, s_leader_propose_no, client_request, client_addr,
                                          s_first_unchosen, s_next_slot, s_replica_config, c_my_drop_rate)

                            paxos_accept(replica_id,
                                         s_accepted[s_next_slot],
                                         s_proposer[s_next_slot],
                                         s_client_request[s_next_slot],
                                         s_client_addr[s_next_slot],
                                         s_next_slot,
                                         s_replica_config,
                                         c_my_drop_rate)

                            # Now s_next_slot is independent of s_first_unchosen
                            s_next_slot += 1

                            # Let s_next_slot skip the skip slots
                            while s_next_slot in c_my_skip_slot:
                                s_next_slot += 1

                        # After exhausting the request queue, have to wait
                        s_waiting_client = True    

                else: # If the leader state is 'prepare'
                    assert ( s_leader_state == 'prepare' )
                    # Just jump to the next slot
                    s_next_slot += 1

                    # Let s_next_slot skip the skip slots
                    while s_next_slot in c_my_skip_slot:
                        s_next_slot += 1

                    # Initialize temp variables with leader's own slot
                    t_value = s_accepted.get(s_next_slot, None)
                    t_propose_no = s_proposer.get(s_next_slot, 0)
                    t_client_request = s_client_request.get(s_next_slot, None)
                    t_client_addr = s_client_addr.get(s_next_slot, None)

                    # Before sending prepare msg, the leader already has one ack (itself)
                    s_ack_msg_count[s_next_slot] = 1

                    # Just an initialization for safety
                    s_leader_state = 'prepare'
                    s_accept_msg_count[s_next_slot] = 0
                    s_waiting_client = False

                    paxos_prepare(replica_id,
                                  s_leader_propose_no,
                                  s_next_slot,
                                  s_replica_config,
                                  c_my_drop_rate)


        elif message_type == 'propose':
            prop_value = message['to_accept']
            prop_proposed_no = message['proposer']
            prop_client_request = message['client_request']
            prop_client_addr = message['client_addr']
            prop_first_unchosen = message['first_unchosen']
            prop_slot = message['slot']

            # If this is the decree from old leader, just ignore it
            if prop_proposed_no < s_leader_propose_no:
                continue

            # It is possible that the replica first receives 'accept' then this 'propose',
            #   in which case s_accept_msg_count[prop_slot] is at least 2
            if prop_proposed_no == s_leader_propose_no and\
                    s_accept_msg_count.get(prop_slot, 0) > 1:
                continue

            # Update the leader proposal number
            s_leader_propose_no = prop_proposed_no

            # Clear the variables that are specific for leaders (in case it was leader)
            # if s_leader_propose_no (prev) == s_my_propose_no:
            s_leader_state = 'prepare'
            s_ack_msg_count[prop_slot] = 0

            # Accept the proposed value
            s_accepted[prop_slot] = prop_value
            s_proposer[prop_slot] = prop_proposed_no
            s_client_request[prop_slot] = copy.deepcopy(prop_client_request)
            s_client_addr[prop_slot] = copy.deepcopy(prop_client_addr)
            # Initialize the s_accept_msg_count
            s_accept_msg_count[prop_slot] = 1

            # Update last_accepted to the most correct value
            if prop_slot > s_last_accepted: 
                s_last_accepted = prop_slot

            paxos_accept(replica_id,
                         s_accepted[prop_slot],
                         s_proposer[prop_slot],
                         s_client_request[prop_slot],
                         s_client_addr[prop_slot],
                         prop_slot, s_replica_config, c_my_drop_rate)

            # TODO: send help message to the leader based on first_unchosen
            if prop_first_unchosen > s_first_unchosen:
                leader_id = u_get_id(s_leader_propose_no, c_replica_num)
                paxos_help_me_choose(replica_id, s_first_unchosen, leader_id,
                                     s_replica_config, c_my_drop_rate)


        elif message_type == 'accept':
            accept_value = message['accepted']
            accept_propose_no = message['proposer']
            accept_client_request = message['client_request']
            accept_client_addr = message['client_addr']
            accept_slot = message['slot']

            # If I have already chosen in this slot, just ignore the message
            if accept_slot in s_learned:
                continue

            # Ignore the old proposal no
            if accept_propose_no < s_leader_propose_no:
                continue
            # If find a newer accept message, clear the count and updates leader
            elif accept_propose_no > s_leader_propose_no:
                # Update the leader
                s_leader_propose_no = accept_propose_no
                # Even if it originally is leader, it shouldn't be now
                # if s_leader_propose_no (prev) == s_my_propose_no:
                s_leader_state = 'prepare'
                s_ack_msg_count[accept_propose_no] = 0

                # Accept the new value from newer leader
                s_accepted[accept_slot] = accept_value
                s_proposer[accept_slot] = accept_propose_no
                s_client_request[accept_slot] = accept_client_request
                s_client_addr[accept_slot] = accept_client_addr
                # This includes myself and the one sends accept to me
                s_accept_msg_count[accept_slot] = 2

                # Update last_accepted if necessary
                if accept_slot > s_last_accepted: 
                    s_last_accepted = accept_slot

                paxos_accept(replica_id,
                             s_accepted[accept_slot],
                             s_proposer[accept_slot],
                             s_client_request[accept_slot],
                             s_client_addr[accept_slot],
                             accept_slot, s_replica_config, c_my_drop_rate)
            # If the accept message is just this propose_no:
            else:
                # If the acceptor didn't get propose message but get accept message:
                # It is possible that a replica only received prepare but not propose
                if (accept_slot not in s_accept_msg_count) or\
                        (s_accept_msg_count[accept_slot] == 0):
                        # (accept_propose_no > s_proposer[accept_slot]) or\
                    # This should not happen when I am leader
                    assert ( u_get_id(s_leader_propose_no, c_replica_num) != replica_id ) 
                    s_accepted[accept_slot] = accept_value
                    s_proposer[accept_slot] = accept_propose_no
                    s_client_request[accept_slot] = accept_client_request
                    s_client_addr[accept_slot] = accept_client_addr
                    # This includes myself and the one sends accept to me
                    s_accept_msg_count[accept_slot] = 2

                    # Update last_accepted if necessary
                    if accept_slot > s_last_accepted: 
                        s_last_accepted = accept_slot

                    paxos_accept(replica_id,
                                 s_accepted[accept_slot],
                                 s_proposer[accept_slot],
                                 s_client_request[accept_slot],
                                 s_client_addr[accept_slot],
                                 accept_slot, s_replica_config, c_my_drop_rate)
                    
                    continue

                assert ( s_accept_msg_count[accept_slot] > 0 )

                s_accept_msg_count[accept_slot] += 1

                # If the majority accept arrives, learn the value
                if s_accept_msg_count[accept_slot] == c_majority_num:
                    # If the value has already been learnt in another slot:
                    if tuple(accept_client_request) in s_chosen_client_request:
                        # Learn NoOp in this slot (in fact this is impossible)
                        print(replica_id, 'slot_no', accept_slot)
                        print(replica_id, 's_accepted', s_accepted)
                        print(replica_id, 'accept_value', accept_value)
                        print(replica_id, 's_proposer', s_proposer)
                        print(replica_id, 'accept_propose_no',  accept_propose_no)
                        print(replica_id, 's_client_request', s_client_request)
                        print(replica_id, 's_chosen_client_request', s_chosen_client_request)
                        print(replica_id, 'accept_client_request', accept_client_request)
                        print(replica_id, 's_learned', s_learned)
                        print(replica_id, 'curr_leader', u_get_id(s_leader_propose_no, c_replica_num))
                        assert ( 0 )

                    # Learn the accepted value
                    s_learned.add(accept_slot)
                    # Once the client request has been learnt, it should never be executed again
                    s_chosen_client_request.add(tuple(accept_client_request))
                    # Update first_unchosen to the most correct value
                    if s_first_unchosen == accept_slot:
                        while s_first_unchosen in s_learned:
                            s_first_unchosen += 1


                    print('Replica {} done with slot {}, value {}'.\
                        format(replica_id, accept_slot, s_accepted[accept_slot]))

                    paxos_ack_client(replica_id,
                                     s_client_request[accept_slot][1],
                                     s_client_addr[accept_slot],
                                     c_my_drop_rate)

                    # If I am the leader, potentially need to process another message
                    if u_get_id(s_leader_propose_no, c_replica_num) == replica_id:

                        # 'dictate' state means no need for prepare
                        # Now s_next_slot is independent of s_first_unchosen
                        if s_leader_state == 'dictated':
                            print('kkkk', replica_id, s_next_slot, s_first_unchosen)
                            assert( s_next_slot >= s_first_unchosen )
                            continue

                        # 'established' state is the final round of 'prepare' state
                        elif s_leader_state == 'established':
                            while s_slot_buffer_queue != []:
                                if s_request_queue == []:
                                    s_waiting_client = True
                                    break

                                else:
                                    client_message = s_request_queue.pop(0)
                                    value = client_message['value']
                                    propose_no = s_leader_propose_no
                                    client_request = [client_message['client_id'],
                                                      client_message['client_request_no']]
                                    client_addr = [client_message['client_ip'],
                                                   client_message['client_port']]

                                    if tuple(client_request) in s_chosen_client_request:
                                        paxos_ack_client(replica_id, client_request[1],
                                                         (client_addr[0], client_addr[1]), c_my_drop_rate)
                                        continue

                                    slot_to_fill = s_slot_buffer_queue.pop(0)

                                    # First the leader itself should accept the value
                                    s_accepted[slot_to_fill] = value
                                    s_proposer[slot_to_fill] = propose_no
                                    s_client_request[slot_to_fill] = client_request
                                    s_client_addr[slot_to_fill] = client_addr

                                    # Increment the s_accept_msg_count
                                    s_accept_msg_count[slot_to_fill] = 1

                                    # Update last_accepted
                                    if slot_to_fill > s_last_accepted: 
                                        s_last_accepted = slot_to_fill

                                    paxos_propose(replica_id, value, s_leader_propose_no, client_request, client_addr,
                                                  s_first_unchosen, slot_to_fill, s_replica_config, c_my_drop_rate)

                                    paxos_accept(replica_id,
                                                 s_accepted[slot_to_fill],
                                                 s_proposer[slot_to_fill],
                                                 s_client_request[slot_to_fill],
                                                 s_client_addr[slot_to_fill],
                                                 slot_to_fill,
                                                 s_replica_config,
                                                 c_my_drop_rate)

                                    # TODO: check whether this is correct
                                    s_waiting_client = False

                            # If the s_slot_buffer_queue is exhausted, need to enter 'dictated' stage
                            if s_slot_buffer_queue == []:
                                s_leader_state = 'dictated'
                                # Now s_next_slot is independent of s_first_unchosen
                                s_next_slot += 1
                                # Let s_next_slot skip the skip slots
                                while s_next_slot in c_my_skip_slot:
                                    s_next_slot += 1

                                while s_request_queue != []:
                                    client_message = s_request_queue.pop(0)
                                    value = client_message['value']
                                    propose_no = s_leader_propose_no
                                    client_request = [client_message['client_id'],
                                                      client_message['client_request_no']]
                                    client_addr = [client_message['client_ip'],
                                                   client_message['client_port']]

                                    if tuple(client_request) in s_chosen_client_request:
                                        paxos_ack_client(replica_id, client_request[1],
                                                         (client_addr[0], client_addr[1]), c_my_drop_rate)
                                        continue

                                    # First the leader itself should accept the value
                                    s_accepted[s_next_slot] = value
                                    s_proposer[s_next_slot] = propose_no
                                    s_client_request[s_next_slot] = client_request
                                    s_client_addr[s_next_slot] = client_addr

                                    # Increment the s_accept_msg_count
                                    s_accept_msg_count[s_next_slot] = 1

                                    # Update last_accepted
                                    if s_next_slot > s_last_accepted: 
                                        s_last_accepted = s_next_slot

                                    paxos_propose(replica_id, value, s_leader_propose_no, client_request, client_addr,
                                                  s_first_unchosen, s_next_slot, s_replica_config, c_my_drop_rate)

                                    paxos_accept(replica_id,
                                                 s_accepted[s_next_slot],
                                                 s_proposer[s_next_slot],
                                                 s_client_request[s_next_slot],
                                                 s_client_addr[s_next_slot],
                                                 s_next_slot,
                                                 s_replica_config,
                                                 c_my_drop_rate)

                                    # Now s_next_slot is independent of s_first_unchosen
                                    s_next_slot += 1

                                    # Let s_next_slot skip the skip slots
                                    while s_next_slot in c_my_skip_slot:
                                        s_next_slot += 1

                                # After exhausting the request queue, have to wait
                                s_waiting_client = True    

                        else: # if s_leader_state == 'prepare'
                            # The feature of skip_slot means that s_next_slot is independent of s_first_unchosen
                            assert ( s_next_slot != s_first_unchosen )
                            s_next_slot = s_first_unchosen if (s_next_slot <= s_first_unchosen) else (s_next_slot + 1)

                            # Let s_next_slot skip the skip slots
                            while s_next_slot in c_my_skip_slot:
                                s_next_slot += 1

                            # Initialize temp variables with leader's own slot
                            t_value = s_accepted.get(s_next_slot, None)
                            t_propose_no = s_proposer.get(s_next_slot, 0)
                            t_client_request = s_client_request.get(s_next_slot, None)
                            t_client_addr = s_client_addr.get(s_next_slot, None)

                            # Before sending prepare msg, the leader already has one ack (itself)
                            s_ack_msg_count[s_next_slot] = 1

                            # Just an initialization for safety
                            s_leader_state = 'prepare'
                            s_accept_msg_count[s_next_slot] = 0
                            s_waiting_client = False

                            paxos_prepare(replica_id,
                                          s_leader_propose_no,
                                          s_next_slot,
                                          s_replica_config,
                                          c_my_drop_rate)

                    # If I am not the leader, just follow first_unchosen
                    else:
                        # The feature of skip_slot means that s_next_slot is independent of s_first_unchosen
                        s_next_slot = s_first_unchosen if (s_next_slot <= s_first_unchosen) else (s_next_slot + 1)
                        # Let s_next_slot skip the skip slots
                        while s_next_slot in c_my_skip_slot:
                            s_next_slot += 1


        elif message_type == 'client_request':
            client_id = message['client_id']
            client_ip = message['client_ip']
            client_port = message['client_port']
            client_request_no = message['client_request_no']
            client_think_propose_no = message['propose_no']

            # Cannot process duplicate client requests
            if (client_id, client_request_no) in s_chosen_client_request:
                # But tell client that message has already been learnt
                paxos_ack_client(replica_id, client_request_no,
                                 (client_ip, client_port), c_my_drop_rate)
                continue

            # If the client-believed leader is older, tell it the correct leader
            if client_think_propose_no < s_leader_propose_no:
                paxos_tell_client_new_leader(replica_id, s_leader_propose_no,
                                             client_ip, client_port,
                                             c_my_drop_rate)
                continue

            # If the client-believed leader is newer, I become the new leader
            elif client_think_propose_no > s_leader_propose_no:
                assert ( u_get_id(client_think_propose_no, c_replica_num) ==\
                         replica_id )
                # Initialize temp variables with leader's own slot
                t_value = s_accepted.get(s_next_slot, None)
                t_propose_no = s_proposer.get(s_next_slot, 0)
                t_client_request = s_client_request.get(s_next_slot, None)
                t_client_addr = s_client_addr.get(s_next_slot, None)

                # Before sending prepare msg, the leader already has one ack (itself)
                s_ack_msg_count[s_next_slot] = 1

                # Update own view of leader
                s_leader_propose_no = client_think_propose_no
                # Just an initialization for safety
                s_leader_state = 'prepare'
                s_accept_msg_count[s_next_slot] = 0
                s_waiting_client = False

                # Empty the request queue
                del s_request_queue[:]
                del s_slot_buffer_queue[:]

                paxos_prepare(replica_id, s_leader_propose_no, s_next_slot,
                              s_replica_config, c_my_drop_rate)

            # Append the client request message to queue
            s_request_queue.append(message)

            print('dfdf', s_leader_state)
            # If a job is already waiting for client message
            while (s_waiting_client == True) and (s_request_queue != []):
                if s_leader_state == 'established':
                    while s_slot_buffer_queue != []:
                        if s_request_queue == []:
                            s_waiting_client = True
                            break

                        else:
                            client_message = s_request_queue.pop(0)
                            value = client_message['value']
                            propose_no = s_leader_propose_no
                            client_request = [client_message['client_id'],
                                              client_message['client_request_no']]
                            client_addr = [client_message['client_ip'],
                                           client_message['client_port']]

                            if tuple(client_request) in s_chosen_client_request:
                                paxos_ack_client(replica_id, client_request[1],
                                                 (client_addr[0], client_addr[1]), c_my_drop_rate)
                                continue

                            slot_to_fill = s_slot_buffer_queue.pop(0)

                            # First the leader itself should accept the value
                            s_accepted[slot_to_fill] = value
                            s_proposer[slot_to_fill] = propose_no
                            s_client_request[slot_to_fill] = client_request
                            s_client_addr[slot_to_fill] = client_addr

                            # Increment the s_accept_msg_count
                            s_accept_msg_count[slot_to_fill] = 1

                            # Update last_accepted
                            if slot_to_fill > s_last_accepted: 
                                s_last_accepted = slot_to_fill

                            paxos_propose(replica_id, value, s_leader_propose_no, client_request, client_addr,
                                          s_first_unchosen, slot_to_fill, s_replica_config, c_my_drop_rate)

                            paxos_accept(replica_id,
                                         s_accepted[slot_to_fill],
                                         s_proposer[slot_to_fill],
                                         s_client_request[slot_to_fill],
                                         s_client_addr[slot_to_fill],
                                         slot_to_fill,
                                         s_replica_config,
                                         c_my_drop_rate)

                            # TODO: check whether this is correct
                            s_waiting_client = False

                    # If the s_slot_buffer_queue is exhausted, need to enter 'dictated' stage
                    if s_slot_buffer_queue == []:
                        s_leader_state = 'dictated'

                        # Now s_next_slot is independent of s_first_unchosen
                        s_next_slot += 1
                        # Let s_next_slot skip the skip slots
                        while s_next_slot in c_my_skip_slot:
                            s_next_slot += 1


                        while s_request_queue != []:
                            client_message = s_request_queue.pop(0)
                            value = client_message['value']
                            propose_no = s_leader_propose_no
                            client_request = [client_message['client_id'],
                                              client_message['client_request_no']]
                            client_addr = [client_message['client_ip'],
                                           client_message['client_port']]

                            if tuple(client_request) in s_chosen_client_request:
                                paxos_ack_client(replica_id, client_request[1],
                                                 (client_addr[0], client_addr[1]), c_my_drop_rate)
                                continue

                            # First the leader itself should accept the value
                            s_accepted[s_next_slot] = value
                            s_proposer[s_next_slot] = propose_no
                            s_client_request[s_next_slot] = client_request
                            s_client_addr[s_next_slot] = client_addr

                            # Increment the s_accept_msg_count
                            s_accept_msg_count[s_next_slot] = 1

                            # Update last_accepted
                            if s_next_slot > s_last_accepted: 
                                s_last_accepted = s_next_slot

                            paxos_propose(replica_id, value, s_leader_propose_no, client_request, client_addr,
                                          s_first_unchosen, s_next_slot, s_replica_config, c_my_drop_rate)

                            paxos_accept(replica_id,
                                         s_accepted[s_next_slot],
                                         s_proposer[s_next_slot],
                                         s_client_request[s_next_slot],
                                         s_client_addr[s_next_slot],
                                         s_next_slot,
                                         s_replica_config,
                                         c_my_drop_rate)

                            # Now s_next_slot is independent of s_first_unchosen
                            s_next_slot += 1

                            # Let s_next_slot skip the skip slots
                            while s_next_slot in c_my_skip_slot:
                                s_next_slot += 1

                        # After exhausting the request queue, have to wait
                        s_waiting_client = True    

                elif s_leader_state == 'dictated':
                    client_message = s_request_queue.pop(0)
                    value = client_message['value']
                    propose_no = s_leader_propose_no
                    client_request = [client_message['client_id'],
                                      client_message['client_request_no']]
                    client_addr = [client_message['client_ip'],
                                   client_message['client_port']]

                    if tuple(client_request) in s_chosen_client_request:
                        paxos_ack_client(replica_id, client_request[1],
                                         (client_addr[0], client_addr[1]), c_my_drop_rate)
                        continue
                    # First the leader itself should accept the value
                    s_accepted[s_next_slot] = value
                    s_proposer[s_next_slot] = propose_no
                    s_client_request[s_next_slot] = client_request
                    s_client_addr[s_next_slot] = client_addr

                    # Initialize the s_accept_msg_count
                    s_accept_msg_count[s_next_slot] = 1

                    # Update last_accepted
                    if s_next_slot > s_last_accepted: 
                        s_last_accepted = s_next_slot

                    paxos_propose(replica_id, value, propose_no, client_request, 
                                  client_addr, s_first_unchosen, s_next_slot,
                                  s_replica_config, c_my_drop_rate)

                    paxos_accept(replica_id,
                                 s_accepted[s_next_slot],
                                 s_proposer[s_next_slot], 
                                 s_client_request[s_next_slot],
                                 s_client_addr[s_next_slot],
                                 s_next_slot, s_replica_config, c_my_drop_rate)

                    s_next_slot += 1
                    print('rtrt', s_next_slot)

                    # Let s_next_slot skip the skip slots
                    while s_next_slot in c_my_skip_slot:
                        s_next_slot += 1

                else:
                    # Other state is not expected
                    print('invalid state {}'.format(s_leader_state))
                    sys.exit(1)


        elif message_type == 'client_timeout':
            client_ip = message['client_ip']
            client_port = message['client_port']
            client_think_propose_no = message['propose_no']

            # Client is newer, meaning I should listen to the client
            if client_think_propose_no >= s_leader_propose_no:
                s_leader_propose_no = client_think_propose_no + 1

                # If I happened to be the new leader
                if u_get_id(s_leader_propose_no, c_replica_num) == replica_id:
                    # Initialize temp variables with leader's own slot
                    t_value = s_accepted.get(s_next_slot, None)
                    t_propose_no = s_proposer.get(s_next_slot, 0)
                    t_client_request = s_client_request.get(s_next_slot, None)
                    t_client_addr = s_client_addr.get(s_next_slot, None)

                    # Before sending prepare msg, the leader already has one ack (itself)
                    s_ack_msg_count[s_next_slot] = 1

                    # Just an initialization for safety
                    s_leader_state = 'prepare'
                    s_accept_msg_count[s_next_slot] = 0
                    s_waiting_client = False

                    # Empty the request queue
                    del s_request_queue[:]
                    del s_slot_buffer_queue[:]

                    paxos_prepare(replica_id, s_leader_propose_no,
                                  s_next_slot, s_replica_config, c_my_drop_rate)

            # If I am newer, meaning client missed something, just tell it mine
            # Do nothing in this case

            paxos_tell_client_new_leader(replica_id, s_leader_propose_no,
                                         client_ip, client_port, c_my_drop_rate)


        elif message_type == 'print_log':
            base_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log')
            if not os.path.isdir(base_dir):
                os.makedirs(base_dir)

            log_file_name = os.path.join(base_dir, 'replica_{}.log'.format(replica_id))
            with open(log_file_name, 'w') as log_file_handle:
                for i in range(s_first_unchosen):
                    log_file_handle.write(str(str(i) + ' ' + s_accepted[i]) + '\n')

            print(replica_id, 'next_slot', s_next_slot)
            print(replica_id, 's_accepted', s_accepted)
            print(replica_id, 's_proposer', s_proposer)
            print(replica_id, 's_client_request', s_client_request)
            print(replica_id, 's_chosen_client_request', s_chosen_client_request)
            print(replica_id, 's_learned', s_learned)
            print(replica_id, 'curr_leader', u_get_id(s_leader_propose_no, c_replica_num))


        elif message_type == 'help_me_choose':
            # Indeed, it does not matter whether I am leader right now
            sender_id = message['replica_id']
            sender_first_unchosen = message['first_unchosen']

            assert ( sender_first_unchosen < s_first_unchosen )
            for i in range(sender_first_unchosen, s_first_unchosen):
                assert ( i in s_learned )

            receiver_ip = s_replica_config[sender_id]['ip']
            receiver_port = s_replica_config[sender_id]['port']

            paxos_you_can_choose(replica_id, sender_first_unchosen,
                                 s_first_unchosen, s_accepted, s_proposer,
                                 s_client_request, s_client_addr,
                                 receiver_ip, receiver_port, c_my_drop_rate)            


        elif message_type == 'you_can_choose':
            # The given range of slots must have been chosen by the other side
            start_slot = message['start_slot']
            end_slot = message['end_slot']
            accepted = message['accepted']
            proposer = message['proposer']
            client_request = message['client_request']
            client_addr = message['client_addr']

            for idx in range(start_slot, end_slot):
                ref_idx = idx - start_slot
                # Before learn, accept the value
                s_accepted[idx] = accepted[ref_idx]
                s_proposer[idx] = proposer[ref_idx]
                s_client_request[idx] = client_request[ref_idx]
                s_client_addr[idx] = client_addr[ref_idx]
                # Learn the value
                s_accept_msg_count[idx] = c_majority_num
                s_learned.add(idx)
                s_chosen_client_request.add(tuple(client_request[ref_idx]))

            # Update s_last_accepted
            if (end_slot - 1) > s_last_accepted:
                s_last_accepted = (end_slot - 1)

            # Update s_first_unchosen
            s_first_unchosen = end_slot
            while s_first_unchosen in s_learned:
                s_first_unchosen += 1

            # Update s_next_slot
            s_next_slot = s_first_unchosen if (s_next_slot <= s_first_unchosen) else s_next_slot

            # Let s_next_slot skip the skip slots
            while s_next_slot in c_my_skip_slot:
                s_next_slot += 1


        else:
            print('Replica {} received an erroneous message {}'.\
                format(replica_id))
            


    my_socket.close()


@click.command()
@click.argument('config_file')
@click.option('--replica_id', '-i', type=int,
              help='specify replica_id in manual mode')
def main(config_file, replica_id):
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
            p = Process(target=handle_replica,
                        args=(replica_id, replica_config_list))
            p.start()

    elif mode == 'manual':
        if replica_id is None:
            print('In manual mode, need to specify replica_id by -i id')
            sys.exit(1)
        p = Process(target=handle_replica,
                    args=(replica_id, replica_config_list))
        p.start()

    else:
        print('Mode can only be either script or manual')
        sys.exit(1)


if __name__ == '__main__':
    main()
