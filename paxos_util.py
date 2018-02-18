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


# Propose the client request, or declare waiting_client
def propose_client_request(s_request_queue,
                           s_my_propose_no,
                           s_next_slot,
                           s_accepted,
                           s_proposer,
                           s_client_request,
                           s_accept_msg_count,
                           s_first_unaccepted,
                           s_first_unchosen):
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


# Pretty print the status of each replica for debugging
def paxos_debug(ip, port):
    message = {
        'message_type': 'debug'
    }
    data = json.dumps(message).encode('utf-8')

    receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    receiver_socket.connect((ip, port))
    receiver_socket.sendall(data)
    receiver_socket.close()
