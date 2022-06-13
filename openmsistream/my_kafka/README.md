## More details on configuration files

All available programs depend on configuration files to define which kafka brokers they should connect to and how they should produce to/consume from topics known to those brokers. This section gives a few more details about how these files can be formatted, the recognized sections they can contain, and options you can change using them.

In general, a configuration file is a text file with one or more distinct and named sections. Comments can be added by using lines starting with "`#`", and other whitespace in general is ignored. Each section begins with a heading line like "`[section_name]`" (with square brackets included), and beneath that heading different parameters are supplied using lines like "`key = value`". If any parameter `value`s begin with the "`$`" character, the configuration file parser will attempt to expand those values as environment variables (this is useful to, for example, store usernames or passwords as environment variables instead of plain text in the repository).

The different sections recognized by the `openmsistream` code are:
1. `[broker]` to configure which Kafka broker should be used by a program and how to connect to it. You can add here any [parameters recognized by Kafka brokers](https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html) in general, but common parameters here include:
    - `bootstrap.servers` to detail the server on which the broker is hosted
    - `sasl.mechanism` and `security.protocol` to describe how programs are authenticated to interact with the broker
    - `sasl.username` and `sasl.password` to provide the key and secret of an API key created for the broker
1. `[producer]` to configure a Producer used by a program. You can add here any [parameters recognized by Kafka Producers](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html) in general, but some of the most useful are:
    - `batch.size` to control the maximum number of messages in each batch sent to the broker
    - `retries` to control how many times a failed message should be retried before throwing a fatal error and moving on
    - `linger.ms` to change how long a batch of messages should wait to become as full as possible before being sent to the broker 
    - `compression.type` to add or change how batches of messages are compressed before being produced (and decompressed afterward)
    - `key.serializer` and `value.serializer` to change methods used to convert message keys and values (respectively) to byte arrays. The `openmsistream` code provides an additional option called [`DataFileChunkSerializer`](./serialization.py#L94-L119) as a message value serializer to pack chunks of data files.
1. `[consumer]` to configure a Consumer used by a program. Again here any [parameters recognized by Kafka Consumers](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html) in general are valid, but some of the most useful are:
    - `group.id` to group Consumers amongst one another. Giving "`new`" for this parameter will create a new group ID every time the code is run.
    - `auto.offset.reset` to tell the Consumer where in the log to start consuming messages if no previously-committed offset for the consumer group can be found. "`earliest`" will start at the beginning of the topic and "`latest`" will start at the end. Giving "`none`" for this parameter will remove it from the configs, and an error will be thrown if no previously-committed offset for the consumer group can be found.
    - `enable.auto.commit` to tell the Consumer whether or not to automatically commit offsets. Some portions of the code manually commit offsets, and if this config is left as its default value (True) a Warning will be logged stating that the "at least once" guarantee is not valid unless you set `enable.auto.commit = False`.
    - `fetch.min.bytes` to change how many bytes must accumulate before a batch of messages is consumed from the topic (consuming batches of messages is also subject to a timeout, so changing this parameter will only ever adjust the tradeoff between throughput and latency, but will not prevent any messages from being consumed in general)
    - `key.deserializer` and `value.deserializer` to change methods used to convert message keys and values (respectively) from byte arrays to objects. The `openmsistream` code provides an additional option called [`DataFileChunkDeserializer`](./serialization.py#L121-L163) to convert a chunk of a data file as a byte array to a [`DataFileChunk` object](../data_file_io/data_file_chunk.py).

## Message Encryption

Encryption of messages sent and received through Kafka is implemented in `OpenMSIStream` using a Kafka wrapper library called [KafkaCrypto](https://github.com/tmcqueen-materials/kafkacrypto). KafkaCrypto provides message-layer encryption for Kafka assuming an untrusted broker, meaning that the keys and values of messages stored within topics are encrypted. The initial encryption is performed before production using a custom Serializer, and decryption is performed after consumption using a custom Deserializer. Please see the documentation for KafkaCrypto for more information.

KafkaCrypto has been wrapped in `OpenMSIStream` to minimize setup and maximize flexibility. Successfully producing/consuming encrypted messages requires just a few steps after installing `OpenMSIStream`:
1. Create a new topic to hold encrypted messages. (It is not possible to mix unencrypted and encrypted messages within a topic.)
1. Create additional key-passing topics (if automatic topic creation is turned off). These new topics are "sub-topics" of the topic created in the previous step, and have special names to reflect this. If the newly-created topic that will hold encrypted messages is called "topic," for example, then topics called "topic.keys", "topic.reqs", and "topic.subs" must also be created. These sub-topics can be compacted.
1. Provision each node that will act as a producer to, or consumer from, a topic containing encrypted messages (more details below)
1. Add a `[kafkacrypto]` section to the config file(s) you use for producers and consumers that will handle encrypted messages (more details below)

Lastly, note that **consuming encrypted messages requires that the producer that produced them is actively running** so that encryption keys can be validated. Users should not, therefore, use `OpenMSIStream` programs such as `UploadDataFile` that automatically shut down producers without user input to produce encrypted messages and should instead use longer-running programs like `DataFileUploadDirectory`.

### Provisioning a node

KafkaCrypto manages which producers and consumers are permitted to send and receive messages to which topics, and keeps these producers and consumers interacting with one another through the key exchange sub-topics. Setting up a set of producers/consumers for a particular topic or set of topics is called "provisioning".

KafkaCrypto provides [a Python script](https://raw.githubusercontent.com/tmcqueen-materials/kafkacrypto/master/tools/simple-provision.py) to walk users through this process. You can invoke the provisioning script in `OpenMSIStream` using the command:

`ProvisionNode`

and following the prompts (the defaults are sensible). If, for any reason, the `ProvisionNode` command can't find the `simple-provision.py` script, you can download it from the link above and rerun the command while providing its location like:

`ProvisionNode --script-path [path_to_simple_provision_script]`

(But OpenMSIStream should be able to do this on its own in most installation contexts.)

Some applications of OpenMSIStream will need to use a different or unique script for provisioning nodes; in those cases you can download the script and provide the path to it as an argument to the `ProvisionNode` command as shown above.

For any other issues with provisioning please refer to KafkaCrypto's documentation.

### Additional configurations needed

Successfully running the `ProvisionNode` command will create a new subdirectory in the [`config_files` directory](./config_files) with a name corresponding to the node ID, containing a `my-node-id.config file`, a `my-node-id.crypto` file, and a `my-node-id.seed` file. KafkaCrypto needs the `my-node-id.config` file to setup producers and consumers, and that file is not secret. The `my-node-id.crypto` and `my-node-id.seed` files, however, should never be saved or transmitted plaintext (files with this pattern [are in the `.gitignore`](../../.gitignore) so if everything is running in the expected way this won't be an issue). An example of one of these created directories can be found [here](./config_files/testing_node) with all files intact because they're used for testing.

To point OpenMSIStream to the config file that's created, one of two options must be added to the config file passed as discussed in the documentation [here](../data_file_io) (for example). Both options list a single new parameter under a heading called `[kafkacrypto]`. 

The first option is to add just the node id, like `node_id = my-node-id`, which works if a directory called `my-node-id` exists in the location expected from running the `ProvisionNode` command. 

If provisioning has been performed without using the `ProvisionNode` command, or if the config, crypto, and seed files are in some location other than a new directory within the config files directory, then the second option should be used, where instead of the `node_id` a parameter `config_file = path_to_config_file` is added, where `path_to_config_file` is the path to the `my-node-id.config` file.

An example of a config file used to set up producers/consumers passing encrypted messages can be found [here](./config_files/test_encrypted.config).

### Undecryptable messages for DataFileDownloadDirectory

If any messages cannot be successfully decrypted by a `DataFileDownloadDirectory` for any reason, the binary contents of their encrypted keys and values will be written out to timestamped files in a subdirectory called "`ENCRYPTED_MESSAGES`" inside the reconstruction directory. One file will be written for the encrypted key and another will be written for the encrypted value. These files can be decrypted later if necessary.
