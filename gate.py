from kafka import KafkaProducer 
import numpy as np              
from sys import argv, exit
from time import time, sleep


if len(argv) != 3:
	print("please provide a valid CUSTOMER name:")
	# print(f"\nformat: {argv[0]} DEVICE_NAME")
	exit(1)

profile_id = argv[1]
charge = float(argv[2])

#make a kafka producer sending data to localhost:9092 and topic dmrc
producer = KafkaProducer(bootstrap_servers='localhost:9092')


	
msg = f'{profile_id},{charge}'
producer.send('dmrc', bytes(msg, encoding='utf8'))
producer.flush()
print(f'sending data to kafka: {msg}')