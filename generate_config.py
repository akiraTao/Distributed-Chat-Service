#!/usr/bin/env python3

import json
import click

@click.command()
@click.argument('f', nargs=1, type=int)
@click.option('--manual', '-m', is_flag=True,
              help='Set this flag to use manual mode')
@click.option('--skip', '-s', type=(int, int), multiple=True,
              help='Specify the replica i skip the slot j by --skip i j')
@click.option('--prob', '-p', type=(int, int), multiple=True,
              help='Specify the replica i drop j% message by --prob i j')
@click.option('--proball', '-pa', type=int,
              help='Specify all replicas drop j% message by --proball j')
def generate_config(f, manual, skip, prob, proball):
    """Generate config file for Paxos replicas tolerating F benign failures"""
    config_data = {}
    config_data['f'] = f
    config_data['mode'] = 'manual' if (manual == True) else 'script' 
    config_data['replica_list'] = []

    port_base = 6000
    replica_num = 2 * f + 1
    # Configure 2f+1 replica information
    for replica_id in range(replica_num):
        replica_config = {
            'id' : replica_id,
            'ip' : 'localhost',
            'port' : port_base + replica_id,
            'skip_slot' : [],
            'drop_rate' : 0
        }
        # Specify all replica as same drop_rate
        if proball:
            replica_config['drop_rate'] = proball

        config_data['replica_list'].append(replica_config)

    # Just a local variable for cleaner access
    replicas = config_data['replica_list']

    # Configure optional parameters
    for replica_id, slot in skip:
        # Ignore invalid parameters
        if replica_id >= replica_num:
            continue
        if slot in replicas[replica_id]['skip_slot']:
            continue
        # Add skipped slot in replica config
        replicas[replica_id]['skip_slot'].append(slot)

    for replica_id, drop_rate in prob:
        # Ignore invalid parameters
        if replica_id >= replica_num:
            continue
        if drop_rate < 0:
            drop_rate = 0
        if drop_rate > 100:
            drop_rate = 100
        # Add drop_rate info in replica config
        replicas[replica_id]['drop_rate'] = drop_rate

    # Write the config to config.json
    config_file = 'replica_config.json'
    with open(config_file, 'w') as config_handle:
        config_handle.write(json.dumps(config_data, indent=4))

if __name__ == '__main__':
    generate_config()
