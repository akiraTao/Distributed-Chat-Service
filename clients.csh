python client.py 0 localhost 8000 replica_config.json < messages.txt &
python client.py 1 localhost 8001 replica_config.json < messages.txt &
python client.py 2 localhost 8002 replica_config.json < messages.txt &
python client.py 3 localhost 8003 replica_config.json < messages.txt &
