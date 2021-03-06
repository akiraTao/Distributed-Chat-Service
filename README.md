Reference: https://ramcloud.stanford.edu/~ongaro/userstudy/paxos.pdf


Overview:

The chat service consists of server-side: replica.py and client-side: client.py.
The information of all replicas (including replica id, ip, port, etc.) are stored
in replica_config.json, which can be generated by generate_config.py script. 
Both replica.py and client.py needs replica_config.json to run.

The service provided both script mode and manual mode. In script mode, running
replica.py will spawn 2F+1 subprocesses representing 2F+1 replicas internally.
In manual mode, executing one script will spawn 1 replica process, so one should
manually spawn 2F+1 replicas in 2F+1 terminals for simulation.


Usage:

1. First we need to produce replica_config.json for configuring all replicas,
which is required before running both replica.py and client.py.
For example, if one wants to produce a service tolerating 5 failures (running
11 machines) in script mode, one would type :

                    python3 generate_config.py 5

which produces replica_config.json in the same directory of generate_config.py.
** Currently generate_config.py specifies all replicas to be run on localhost,
port 6000+replica_id. Specifying hostname using this script would not add much
convenience, and therefore one needs to manually change replica_config.json if needed.

In order to support various simulations, generate_config.py also supports 
different optional flags for different settings. These are listed below:

a) If one wants to run manual mode instead of script mode, use flag -m:
                    
                    python3 generate_config.py 5 -m

b) If one wants to specify all replicas' message drop_rate as, say, 10%, use flag -pa:

                    python3 generate_config.py 5 -pa 10

c) If one wants to specify individual replica's message drop_rate, say, configure
replica 1's message drop rate as 20% use flag -p:

                    python3 generate_config.py 5 -p 1 20 

This would override the message drop rate specified by -pa flag

d) If one wants to specify that the replica 1 skips slot 2, use flag -s:
            
                    python3 generate_config.py 5 -s 1 2 

More details can be found by running python3 generate_config.py --help.

** Notice that the configuration of replicas are all included in the generated replica_config.json.


2. After one has replica_config.json ready, one can run following to run in script mode:

                    python3 replica.py replica_config.json

For script mode (without specifying -m when generating config), it will spawn 2F+1 replicas automatically.

For manual mode, one need to specify replica id by flag -i [replica_id]. e.g.,
                    
                     python3 replica.py replica_config.json -i 0

So one have to run above command 2F+1 times in different terminal windows, each with
replica_id 0...2F, for manual mode.


3. Then one can run client.py using following command:
    
    python3 client.py [client_id] [client_host] [client_port] replica_config.json [msg_drop_rate]

For example,
    
                python3 client.py 0 localhost 8000 replica_config.json 10

Then it will show an interactive interface for client to use.
The possible commands are:
a) s [press enter] MESSAGE: send MESSAGE to be logged to replicas.
b) p [press enter]:         let replicas write their logs to actual log file
c) e [press enter]:         send the client process

Now you can communicate with chat service!

Notice that replicas won't add their learnt values into logged files until
the client enters p command. In other words, client's p command will trigger logging action.


4. The above way of running client cannot simulate the sitution where multiple
clients are sending messages to the system concurrently. Therefore, we also
implemented a script generate_client_csh.py to facilitate this procedure.
One can run

    python3 generate_client_csh.py [CLIENT_NUM] [MSG_NUM] [MSG_DROP_RATE],

which will generate clients.csh script file. Running ./clients.csh will emulate
the situation where CLIENT_NUM clients, each sending MSG_NUM messages to chat
servers, with MSG_DROP_RATE% drop rate. Therefore, there would be altogether
CLIENT_NUM * MSG_NUM messages sent to the chat server.


5. For each p command from the client, the replica will flush its log file.
One can see that in the working directory, a subdirectory called /log is generated,
which includes log file for each replica.
