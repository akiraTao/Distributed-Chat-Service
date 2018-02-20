import click
@click.command()
@click.argument('client_no')
def main(client_no):
    shell_file = open('clients.csh','w')
    for client_i in range(int(client_no)):
    	shell_file.write('python client.py {} localhost {} replica_config.json < messages.txt &\n'.format(client_i, 8000+client_i))


if __name__ == '__main__':
    main()