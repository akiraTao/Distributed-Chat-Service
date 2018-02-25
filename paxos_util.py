#!/usr/env/bin python3

'''
    The functions prepended by paxos_ are the ones involving message delivery
    during the protocol. The functions prepended by u_ are general utilities.
'''

import json
import socket
import random
 

# This function is used in Paxos prepare stage (leader -> replicas)
def paxos_prepare(my_id,
                  propose_no,
                  slot,
                  replica_config,
                  drop_rate):
    # Send 'propose' message to every other replica
    message = {
        'message_type' : 'prepare',
        'proposer' : propose_no,
        'slot' : slot
    }

    for replica_addr in replica_config.values():
        u_send_message(message, replica_addr['ip'],
                                replica_addr['port'], my_id, drop_rate)  


# This function is used in Paxos prepare ack stage (replica -> leader)
def paxos_ack_prepare(my_id,
                      value,
                      propose_no,
                      client_request,
                      client_addr,
                      no_more_accepted,
                      slot,
                      leader_id,
                      replica_config,
                      drop_rate):
    # Send acknowledge to 'propose' message to the leader
    message = {
        'message_type' : 'ack_prepare',
        'accepted' : value,
        'proposer' : propose_no,
        'client_request' : client_request,
        'client_addr' : client_addr,
        'no_more_accepted' : no_more_accepted,
        'slot' : slot
    }

    u_send_message(message, replica_config[leader_id]['ip'], 
                            replica_config[leader_id]['port'], my_id, drop_rate)


# This function is used in Paxos propose stage (leader -> replicas)
def paxos_propose(my_id,
                  value,
                  propose_no,
                  client_request,
                  client_addr,
                  first_unchosen,
                  slot,
                  replica_config,
                  drop_rate):
    message = {
        'message_type' : 'propose',
        'to_accept' : value,
        'proposer' : propose_no,
        'client_request' : client_request,
        'client_addr' : client_addr,
        'first_unchosen' : first_unchosen,
        'slot' : slot
    }

    for replica_addr in replica_config.values():
        u_send_message(message, replica_addr['ip'],
                                replica_addr['port'], my_id, drop_rate) 


# This function is used in Paxos accept stage (replica -> replicas)
def paxos_accept(my_id,
                 value,
                 propose_no,
                 client_request,
                 client_addr,
                 slot,
                 replica_config,
                 drop_rate):
    message = {
        'message_type' : 'accept',
        'accepted' : value,
        'proposer' : propose_no,
        'client_request' : client_request,
        'client_addr' : client_addr,
        'slot' : slot
    }

    for replica_addr in replica_config.values():
        u_send_message(message, replica_addr['ip'],
                                replica_addr['port'], my_id, drop_rate)


# This function is used by replicas to send ack to the client when the value is learned
# client_addr[0]: ip, client_addr[1]: port
def paxos_ack_client(my_id,
                     request_no,
                     client_addr,
                     drop_rate):
    message = {
        'message_type' : 'ack_client',
        'request_no' : request_no
    }

    u_send_message(message, client_addr[0], 
                            client_addr[1], my_id, drop_rate)


# This function is used by replicas to tell the client the new leader
def paxos_tell_client_new_leader(my_id,
                                 propose_no,
                                 client_ip,
                                 client_port,
                                 drop_rate):
    message = {
        'message_type' : 'new_leader_to_client',
        'propose_no' : propose_no
    }

    u_send_message(message, client_ip,
                            client_port, my_id, drop_rate)


# This function is used by client to send request to replicas 
def paxos_client_request(my_id,
                         my_ip,
                         my_port,
                         request_no,
                         leader_propose_no,
                         value,
                         replica_config,
                         drop_rate):

    message = {
        'message_type' : 'client_request',
        'client_id' : my_id,
        'client_ip' : my_ip,
        'client_port' : my_port,
        'client_request_no' : request_no,
        'propose_no' : leader_propose_no,
        'value' : value
    }

    # Get the leader id
    replica_num = len(replica_config)
    leader_id = u_get_id(leader_propose_no, replica_num)

    u_send_message(message, replica_config[leader_id]['ip'],
                            replica_config[leader_id]['port'], my_id, drop_rate)


# This function is used by client to send timeout to replicas
def paxos_client_timeout(my_id,
                         my_ip,
                         my_port,
                         request_no,
                         leader_propose_no,
                         replica_config,
                         drop_rate):
    message = {
        'message_type' : 'client_timeout',
        'client_id' : my_id,
        'client_ip' : my_ip,
        'client_port' : my_port,
        'client_request_no' : request_no,
        'propose_no' : leader_propose_no
    }

    for replica_addr in replica_config.values():
        u_send_message(message, replica_addr['ip'],
                                replica_addr['port'], my_id, drop_rate)


# This function is used by client to trigger log printing
def paxos_print_log(my_id,
                    replica_config):
    message = {
        'message_type' : 'print_log'
    }

    for replica_addr in replica_config.values():
        u_send_message(message, replica_addr['ip'],
                                replica_addr['port'], my_id, 0)


# This function is used by replicas to catch up with chosen values
def paxos_help_me_choose(my_id,
                         my_first_unchosen,
                         leader_id,
                         replica_config,
                         drop_rate):
    message = {
        'message_type' : 'help_me_choose',
        'replica_id' : my_id,
        'first_unchosen' : my_first_unchosen
    }


    u_send_message(message, replica_config[leader_id]['ip'],
                            replica_config[leader_id]['port'], my_id, drop_rate)


# This function is sent to help others catch up with chosen values
# start_slot is inclusive, end_slot is exclusive
def paxos_you_can_choose(my_id,
                         start_slot,
                         end_slot,
                         s_accepted,
                         s_proposer,
                         s_client_request,
                         s_client_addr,
                         receiver_ip,
                         receiver_port,
                         drop_rate):
    
    accepted = []
    proposer = []
    client_request = [] 
    client_addr = []

    for i in range(start_slot, end_slot):
        accepted.append(s_accepted[i])
        proposer.append(s_proposer[i])
        client_request.append(s_client_request[i])
        client_addr.append(s_client_addr[i])

    message = {
        'message_type' : 'you_can_choose',
        'start_slot' : start_slot,
        'end_slot' : end_slot,
        'accepted' : accepted,
        'proposer' : proposer,
        'client_request' : client_request,
        'client_addr' : client_addr
    }

    u_send_message(message, receiver_ip,
                            receiver_port, my_id, drop_rate)


# General routine for sending message to the receiver
def u_send_message(message_body,
                   receiver_ip,
                   receiver_port,
                   my_id,
                   drop_rate):

    random_num = random.randint(1, 100)

    if random_num <= drop_rate:
        print('Replica {} dropped message'.format(my_id))
        return

    # Serialize message_body to proper format
    data = json.dumps(message_body).encode('utf-8')

    # Send the message using UDP socket for non-blocking
    receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver_socket.sendto(data, (receiver_ip, receiver_port))
    receiver_socket.close()


# Returns the leader id of the replica
# propose_no is a monotically increasing number containing the round info
def u_get_id(propose_no, replica_num):
    return (propose_no % replica_num)
