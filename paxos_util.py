#!/usr/env/bin python3

import json
import socket


# This function is used in Paxos prepare stage (leader -> replicas)
# Two scenarios this may get called:
#   1. In the very beginning of the protocol by default leader
#   2. When the view changes new leader needs to prepare
def paxos_prepare(propose_no,
                  slot,
                  replica_config):
    # Send 'propose' message to every other replica
    message = {
        'message_type' : 'prepare',
        'proposer' : propose_no,
        'slot' : slot
    }

    for replica_addr in replica_config.values():
        send_message(message, replica_addr['ip'],
                              replica_addr['port'])  


# This function is used in Paxos prepare ack stage (replica -> leader)
def paxos_ack_prepare(value,
                      proposer,
                      client_request,
                      client_addr,
                      no_more_accepted,
                      slot_no,
                      leader_id,
                      replica_config):
    # Send acknowledge to 'propose' message to the leader
    message = {
        'message_type' : 'ack_prepare',
        'accepted' : value,
        'proposer' : proposer,
        'client_request' : client_request,
        'client_addr' : client_addr,
        'no_more_accepted' : no_more_accepted,
        'slot' : slot_no
    }

    send_message(message, replica_config[leader_id]['ip'], 
                          replica_config[leader_id]['port'])


# This function is used in Paxos propose stage (leader -> replicas)
def paxos_propose(value,
                  propose_no,
                  client_request,
                  client_addr,
                  first_unchosen,
                  slot,
                  replica_config):
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
        send_message(message, replica_addr['ip'],
                              replica_addr['port']) 


# This function is used in Paxos accept stage (replica -> replicas)
def paxos_accept(value,
                 propose_no,
                 client_request,
                 client_addr,
                 slot,
                 replica_config):
    message = {
        'message_type' : 'accept',
        'accepted' : value,
        'proposer' : propose_no,
        'client_request' : client_request,
        'client_addr' : client_addr,
        'slot' : slot
    }

    for replica_addr in replica_config.values():
        send_message(message, replica_addr['ip'],
                              replica_addr['port'])


# This function is used by replicas to send ack to the client when the value is learned
# client_addr[0]: ip, client_addr[1]: port
def paxos_ack_client(request_no,
                     client_addr):
    message = {
        'message_type' : 'ack_client',
        'request_no' : request_no
    }

    send_message(message, client_addr[0], 
                          client_addr[1])


# General routine for sending message to the receiver
# receiver_addr[0]: ip, receiver_addr[1]: port
def send_message(message_body,
                 receiver_ip,
                 receiver_port):
    # Serialize message_body to proper format
    data = json.dumps(message_body).encode('utf-8')

    # Send the message
    receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    receiver_socket.connect((receiver_ip, receiver_port))
    receiver_socket.sendall(data)
    receiver_socket.close()


# Returns the leader id of the replica
# propose_no is a monotically increasing number containing the information of round
def get_id(propose_no, replica_num):
    return (propose_no % replica_num)
