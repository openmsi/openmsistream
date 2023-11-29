#!/usr/bin/python3

"""Reads still-encrypted message keys and values from files on disk and re-produces them
to the topic from which they came
"""

# imports
import os, pathlib
from sys import argv
from time import sleep
from argparse import ArgumentParser
from kafkacrypto import KafkaProducer, KafkaCryptoStore
from openmsitoolbox.argument_parsing.parser_callbacks import existing_file, existing_dir

def main(given_args=None):
  # Define and get command line arguments
  parser = ArgumentParser()
  parser.add_argument("config", type=existing_file, help="Path to the config file to use")
  parser.add_argument("encrypted_messages_dir", type=existing_dir, help="Path to the directory containining encrypted message key and value files written out by a DataFileDownloadDirectory")
  args = parser.parse_args(given_args)
  # Process configuration file
  kcs = KafkaCryptoStore(str(args.config))
  # Setup Kafka Producer *without* crypto (de)ser (messages are all ready appropriately serialized)
  kafka_config = kcs.get_kafka_config('producer')
  producer = KafkaProducer(**kafka_config)
  # Get all of the files in the encrypted files directory
  os.chdir(str(args.encrypted_messages_dir))
  files = sorted(filter(os.path.isfile, os.listdir('.')), key=os.path.getmtime)
  # Produce a message for each pair of encrypted key/value files
  for f in files:
    namestart = 'encrypted_message_from_'
    keynameend = "_key.bin"
    valuenameend = '_value.bin'
    if f.lower().startswith(namestart) and f.lower().endswith(keynameend):
      descriptor = None
      possibilities = ("_consumed_", "_produced_", "_received_")
      for poss in possibilities:
        if poss in f:
          descriptor = poss
          break
      if descriptor is None:
        raise ValueError(f"ERROR: unrecognized filename format for file {f}")
      tpc = f[len(namestart):f.index(descriptor)] # topic from whence this came -- which cannot have the strings "consumed", "produced", or "received" in it
      with open(f, mode='rb') as ipf: # read key
        nkv = ipf.read()
      with open(f[:-len(keynameend)]+valuenameend, mode='rb') as ipf: # read value
        nvv = ipf.read()
      print(f"Reproducing {f[:-len(keynameend)]}[{keynameend}/{valuenameend}] ({len(nkv)},{len(nvv)}) to {tpc}")
      producer.send(tpc,key=nkv,value=nvv).get()
      producer.poll()
      sleep(0.2)
  print("Waiting for producer to send all files to Broker...")
  producer.close()
  kcs.close()
  print("Done!")

if __name__=="__main__":
  main()

# # Process configuration file
# if len(argv) != 3:
#   exit('Invalid command line. Usage: reproduce-encrypted-messages.py file.config encrypted_messages_dir')
# kcs = KafkaCryptoStore(argv[1])
# 
# # Setup Kafka Producer *without* crypto (de)ser (messages are all ready appropriately serialized)
# kafka_config = kcs.get_kafka_config('producer')
# producer = KafkaProducer(**kafka_config)
# 
# os.chdir(argv[2])
# files = sorted(filter(os.path.isfile, os.listdir('.')), key=os.path.getmtime)
# 
# for f in files:
#   if f.lower().startswith('encrypted_message_from_') and f.lower().endswith('_key.bin'):
#     tpc = f.split('_')[3] # topic from whence this came -- which cannot have underscores in it
#     if True:
#       with open(f, mode='rb') as ipf: # read key
#         nkv = ipf.read()
#       with open(f[:-8]+'_value.bin', mode='rb') as ipf: # read value
#         nvv = ipf.read()
#       print("Reproducing " + f[:-8] + "[_key.bin/_value.bin] (" + str(len(nkv)) + "," + str(len(nvv)) + ") to " + tpc)
#       producer.send(tpc,key=nkv,value=nvv).get()
#       producer.poll()
#       sleep(0.2)
# print("Waiting for producer to send all files to Broker...")
# producer.close(1200)
