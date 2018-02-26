python client.py 0 localhost 8000 replica_config.json 30 < /Users/taosixu/Desktop/P1/msg/messages_0.txt &
python client.py 1 localhost 8001 replica_config.json 30 < /Users/taosixu/Desktop/P1/msg/messages_1.txt &
python client.py 2 localhost 8002 replica_config.json 30 < /Users/taosixu/Desktop/P1/msg/messages_2.txt &
