#!/usr/env/bin python3
import json
import socket

# This function is used in Paxos prepare stage (leader -> replicas)
# Two scenarios this may get called:
#   1. In the very beginning of the protocol by default leader
#   2. When the view changes new leader needs to prepare
def paxos_prepare(propose_no,
                  next_slot,
                  replica_config):
    # Send 'propose' message to every other replica
    message = {
        'message_type' : 'prepare',
        'proposer' : propose_no,
        'slot' : next_slot
    }
    data = json.dumps(message).encode('utf-8')

    for replica_id, replica_addr in replica_config.items():
        receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        receiver_socket.connect((replica_addr['ip'], replica_addr['port']))
        receiver_socket.sendall(data)
        receiver_socket.close()    


# This function is used in Paxos prepare ack stage (replica -> leader)
def paxos_ack_prepare(accepted_value, 
                      accepted_proposer,
                      accepted_client_request,
                      no_more_accepted,
                      slot_no,
                      leader_id, 
                      replica_config):
    # Send acknowledge to 'propose' message to the leader
    message = {
        'message_type' : 'ack_prepare',
        'accepted' : accepted_value,
        'proposer' : accepted_proposer,
        'client_request' : accepted_client_request,
        'no_more_accepted' : no_more_accepted,
        'slot' : slot_no
    }
    data = json.dumps(message).encode('utf-8')

    receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    receiver_socket.connect((replica_config[leader_id]['ip'], replica_config[leader_id]['port']))
    receiver_socket.sendall(data)
    receiver_socket.close()


# This function is used in Paxos propose stage (leader -> replicas)
def paxos_propose(value,
                  propose_no,
                  client_request,
                  first_unchosen,
                  slot,
                  replica_config):
    message = {
        'message_type' : 'propose',
        'to_accept' : value,
        'proposer' : propose_no,
        'client_request' : client_request,
        'first_unchosen' : first_unchosen,
        'slot' : slot
    }
    data = json.dumps(message).encode('utf-8')

    for replica_id, replica_addr in replica_config.items():
        receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        receiver_socket.connect((replica_addr['ip'], replica_addr['port']))
        receiver_socket.sendall(data)
        receiver_socket.close()  


# This function is used in Paxos accept stage (replica -> replicas)
def paxos_accept(value,
                 propose_no,
                 client_request,
                 slot,
                 replica_config):
    message = {
        'message_type' : 'accept',
        'accepted' : value,
        'proposer' : propose_no,
        'client_request' : client_request,
        'slot' : slot
    }
    data = json.dumps(message).encode('utf-8')

    for replica_id, replica_addr in replica_config.items():
        receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        receiver_socket.connect((replica_addr['ip'], replica_addr['port']))
        receiver_socket.sendall(data)
        receiver_socket.close()
