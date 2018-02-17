#!/usr/env/bin python3

import json
import socket


def main():
    message = {
        'message_type' : 'client_request',
        'client_id' : 2,
        'client_request_no' : 5,
        'value' : 'gtmd'
    } 
    data = json.dumps(message)

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 6000))
    client_socket.sendall(str.encode(data))
    client_socket.close()


if __name__ == '__main__':
    main()
