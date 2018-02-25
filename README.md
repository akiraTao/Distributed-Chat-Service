{
    'message_type' : 'prepare',
    'proposer' : propose_no,
    'slot' : slot
}

{
    'message_type' : 'ack_prepare',
    'accepted' : value,
    'proposer' : propose_no,
    'client_request' : (client_id, newest_request_id),
    'client_addr' : (ip, port),
    'no_more_accepted' : Bool,
    'slot' : slot_no
}

{
    'message_type' : 'propose',
    'to_accept' : value,
    'proposer'  : propose_no,
    'client_request' : (client_id, newest_request_id),
    'client_addr' : (ip, port),
    'first_unchosen' : first_unchosen_index,
    'slot': slot_no
}

{
    'message_type' : 'accept',
    'accepted' : value,
    'proposer'  : propose_no,
    'client_request' : (client_id, newest_request_id),
    'client_addr' : (ip, port),
    'slot' : slot_no
}


{
    'message_type' : 'ack_client'
    'request_no' : request_no
}


{
    'message_type' : 'new_leader_to_client',
    'propose_no' : new_leader_propose_no
}


{
    'message_type' : 'client_request',
    'client_id' : my_id,
    'client_ip' : my_ip,
    'client_port' : my_port,
    'client_request_no' : request_no,
    'propose_no' : leader_propose_no,
    'value' : ''
}


{
    'message_type' : 'client_timeout',
    'client_id' : my_id,
    'client_ip' : my_ip,
    'client_port' : my_port,
    'client_request_no' : request_no,
    'propose_no' : leader_propose_no
}

{
    'message_type' : 'print_log'
}

{
    'message_type' : 'help_me_choose',
    'replica_id' : replica_id,
    'first_unchosen' : first_unchosen_index
}

{
    'message_type' : 'you_can_choose',
    'range' : (your_first_unchosen, my_first_unchosen),
    'value' : [] 
}
