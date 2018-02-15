{
	'message_type' : 'propose',
	'proposer' : propose_no,
	'slot' : next_slot,
}

{
	'message_type' : 'ack_propose',
	'accepted' : value,
	'proposer' : propose_no,
	'noMoreAccepted' : Bool
}

{
	'message_type' : 'accept',
	'to_accept' : value,
	'proposer'  : propose_no,
	'client_request' : client_id.newest_request_id
}

{
	'message_type' : 'ack_accept',
	'accepted' : value,
	'proposer'  : propose_no,
	'client_request' : client_id.newest_request_id
}

{
	'message_type' : 'client_request'
	'client_id' : Int,
	'client_sequence_no' : Int,
}

{
	'message_type' : 'client_timeout'
}

{
	'message_type' : 'view_change',
}

{
	'message_type' : 'ack_client',
	
}
