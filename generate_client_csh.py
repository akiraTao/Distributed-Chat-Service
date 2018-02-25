#!/usr/bin/env python3

import click


@click.command()
@click.argument('client_no')
@click.argument('msg_no')
@click.argument('client_drop_rate')
def main(client_no, msg_no, client_drop_rate):
    shell_file = open('clients.csh','w')

    for client_i in range(int(client_no)):
    	shell_file.write('python client.py {} localhost {} replica_config.json {} < messages_{}.txt &\n'.\
    					 format(client_i, 8000+client_i, client_drop_rate, client_i))

    	messagetxt = open('message_{}.txt'.format(client_i),'w')

    	for i in range(int(msg_no)):
    		messagetxt.write('s\n')
    		messagetxt.write('message_{}_{}\n'.format(client_i, i))

    shell_file.close()


if __name__ == '__main__':
    main()