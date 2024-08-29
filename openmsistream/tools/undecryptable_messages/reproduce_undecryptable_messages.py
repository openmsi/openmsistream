#!/usr/bin/python3

"""Reads still-encrypted message keys and values from files on disk and re-produces them
to the topic from which they came
"""

# imports
import os
from time import sleep
from argparse import ArgumentParser
from kafkacrypto import KafkaProducer, KafkaCryptoStore
from openmsitoolbox.argument_parsing.parser_callbacks import existing_file, existing_dir


def main(given_args=None):
    "Re-produce binary contents of encrypted messages to the topic from which they came"
    # Define and get command line arguments
    parser = ArgumentParser()
    parser.add_argument(
        "config", type=existing_file, help="Path to the config file to use"
    )
    parser.add_argument(
        "encrypted_messages_dir",
        type=existing_dir,
        help=(
            "Path to the directory containining encrypted message key and value files "
            "written out by a DataFileDownloadDirectory"
        ),
    )
    args = parser.parse_args(given_args)
    # Process configuration file
    kcs = KafkaCryptoStore(str(args.config), conf_global_logger=False)
    # Setup Kafka Producer *without* crypto ser (msgs are already serialized)
    kafka_config = kcs.get_kafka_config("producer")
    producer = KafkaProducer(**kafka_config)
    # Get all of the files in the encrypted files directory
    return_dir = os.getcwd()
    os.chdir(str(args.encrypted_messages_dir))
    files = sorted(filter(os.path.isfile, os.listdir(".")), key=os.path.getmtime)
    # Produce a message for each pair of encrypted key/value files
    for filename in files:
        namestart = "encrypted_message_from_"
        keyapp = "_key.bin"
        valueapp = "_value.bin"
        if filename.startswith(namestart) and filename.endswith(keyapp):
            descriptor = None
            possibilities = ("_consumed_", "_produced_", "_received_")
            for poss in possibilities:
                if poss in filename:
                    descriptor = poss
                    break
            if descriptor is None:
                raise ValueError(
                    f"ERROR: unrecognized filename format for file {filename}"
                )
            # Determine the topic from which the msg came
            # topic name cannot contain "consumed", "produced", or "received"
            tpc = filename[len(namestart) : filename.index(descriptor)]
            with open(filename, mode="rb") as ipf:  # read key
                nkv = ipf.read()
            with open(
                filename[: -len(keyapp)] + valueapp, mode="rb"
            ) as ipf:  # read value
                nvv = ipf.read()
            msg = (
                f"Reproducing {filename[:-len(keyapp)]}[{keyapp}/{valueapp}] "
                f"({len(nkv)},{len(nvv)}) to {tpc}"
            )
            print(msg)
            producer.send(tpc, key=nkv, value=nvv).get()
            producer.poll()
            sleep(0.2)
    os.chdir(return_dir)
    print("Waiting for producer to send all files to Broker...")
    producer.flush()
    producer.close()
    kcs.close()
    print("Done!")


if __name__ == "__main__":
    main()
