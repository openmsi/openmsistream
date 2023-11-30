#!/usr/bin/python3
import msgpack
import logging
from sys import argv, exit
from time import time, sleep
from kafkacrypto import KafkaProducer, KafkaCryptoStore
import os

# Process configuration file
if len(argv) != 3:
  exit('Invalid command line. Usage: reproduce-encrypted-messages.py file.config encrypted_messages_dir')
kcs = KafkaCryptoStore(argv[1])

# Setup Kafka Producer *without* crypto (de)ser (messages are all ready appropriately serialized)
kafka_config = kcs.get_kafka_config('producer')
producer = KafkaProducer(**kafka_config)

os.chdir(argv[2])
files = sorted(filter(os.path.isfile, os.listdir('.')), key=os.path.getmtime)

for f in files:
  if f.lower().startswith('encrypted_message_from_') and f.lower().endswith('_key.bin'):
    tpc = f.split('_')[3] # topic from whence this came -- which cannot have underscores in it
    if True:
      with open(f, mode='rb') as ipf: # read key
        nkv = ipf.read()
      with open(f[:-8]+'_value.bin', mode='rb') as ipf: # read value
        nvv = ipf.read()
      print("Reproducing " + f[:-8] + "[_key.bin/_value.bin] (" + str(len(nkv)) + "," + str(len(nvv)) + ") to " + tpc)
      producer.send(tpc,key=nkv,value=nvv).get()
      producer.poll()
      sleep(0.2)
print("Waiting for producer to send all files to Broker...")
producer.close(1200)
