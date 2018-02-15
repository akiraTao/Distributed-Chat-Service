#!/usr/bin/env python3
import json
import click

@click.command()
@click.argument('f', nargs=1, type=int)
def generate_config(f):
	config_data = {}
	config_data['f'] = f
	config_data['replica_list'] = []

	port_base = 6000
	replica_num = 2 * f + 1
	for replica_id in range(replica_num):
		replica_config = {
			'id' : replica_id,
			'ip' : 'localhost',
			'port' : port_base + replica_id
		}
		config_data['replica_list'].append(replica_config)

	config_file = 'config.json'
	with open(config_file, 'w') as config_handle:
		config_handle.write(json.dumps(config_data, indent=4))

if __name__ == '__main__':
	generate_config()
