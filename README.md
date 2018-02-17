{
    'message_type' : 'prepare',
    'proposer' : propose_no,
    'slot' : next_slot,
}

{
    'message_type' : 'ack_prepare',
    'accepted' : value,
    'proposer' : propose_no,
    'client_request' : client_id.newest_request_id
    'no_more_accepted' : Bool,
    'slot' : slot_no
}

{
    'message_type' : 'propose',
    'to_accept' : value,
    'proposer'  : propose_no,
    'client_request' : client_id.newest_request_id,
    'first_unchosen_index' : first_unchosen_index
}

{
    'message_type' : 'accept',
    'accepted' : value,
    'proposer'  : propose_no,
    'client_request' : client_id.newest_request_id
}

{
    'message_type' : 'help_me_choose',
    'replica_id' : replica_id,
    'first_unchosen_index' : first_unchosen_index
}

{
    'message_type' : 'rescue_choose',
    'range' : (your_first_unchosen, my_first_unchosen),
    'value' : [] 
}

{
    'message_type' : 'client_request',
    'client_id' : Int,
    'client_request_no' : Int,
    'value' : value 
}

{
    'message_type' : 'client_timeout',
    'client_id' : Int,
    'client_request_no' : Int,
    'value' : value
}

{
    'message_type' : 'view_change',
    'client_id' : Int,
    'client_request_no' : Int,
    'message' : value,
    'new_proposer' : proposer_no
}

{
    'message_type' : 'ack_client'
}
