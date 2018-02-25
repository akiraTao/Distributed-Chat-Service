#!/usr/bin/env python3

import os
import click
import shutil


@click.command()
@click.argument('client_no')
@click.argument('msg_no')
@click.argument('client_drop_rate')
def main(client_no, msg_no, client_drop_rate):

    base_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'msg')
    if os.path.isdir(base_dir):
        shutil.rmtree(base_dir)
    os.makedirs(base_dir)

    shell_file = open('clients.csh', 'w')

    for client_i in range(int(client_no)):
        msg_filename = os.path.join(base_dir, 'messages_{}.txt'.format(client_i))

        shell_file.write('python client.py {} localhost {} replica_config.json {} < {} &\n'.\
                         format(client_i, 8000+client_i, client_drop_rate, msg_filename))

        messagetxt = open(msg_filename, 'w')

        for i in range(int(msg_no)):
            messagetxt.write('s\n')
            messagetxt.write('message_{}_{}\n'.format(client_i, i))

        messagetxt.write('p\n')
        messagetxt.write('e\n')

    shell_file.close()


if __name__ == '__main__':
    main()