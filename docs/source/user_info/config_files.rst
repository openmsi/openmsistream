Configuration files
-------------------

Working with any of the programs in OpenMSIStream requires creating at least one configuration file that will tell the Kafka backend how to connect to the broker and configure the main Producer(s)/Consumer(s) it uses. Information in configuration files is also used to point a Consumer to an S3 bucket to the correct endpoint and connect to it, and to supply information necessary for working with data encrypted using KafkaCrypto. 

Structure
^^^^^^^^^

In general, a configuration file is a text file with one or more distinct and named sections. Comments can be added by using lines starting with "``#``", and other whitespace in general is ignored. Each section begins with a heading line like "``[section_name]``" (with square brackets included), and beneath that heading different parameters are supplied using lines like "``key = value``". The structure of the files is that expected by the Python :mod:`configparser` module; check the docs there for more information if you'd like.

If any parameter's ``value`` begins with the "``$``" character, the configuration file parser will attempt to expand that values as an environment variable. This is a useful way to, for example, store secrets like usernames or passwords as environment variables instead of plain text. You can set these environment variables in a shell ``.rc`` or ``.profile`` file if running on Linux or Mac OS. On Windows you can set them as machine environment variables using commands like::

    [Environment]::SetEnvironmentVariable("NAME","VALUE",[EnvironmentVariableTarget]::Machine)

You can find a simple example configuration file `in the OpenMSIStream code here <https://github.com/openmsi/openmsistream/blob/main/openmsistream/kafka_wrapper/config_files/test.config>`_. That file shows how to connect to the Confluent Cloud broker used for testing, with the username and password for the cluster referenced as environment variables.

"``[broker]``" section
^^^^^^^^^^^^^^^^^^^^^^

Options listed under the ``[broker]`` section heading configure which Kafka broker should be used by a program and how to connect to it. You can add here any `parameters recognized by Kafka brokers <https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html>`_ in general, but common parameters here include:

* ``bootstrap.servers`` to detail the server on which the broker is hosted
* ``sasl.mechanism`` and ``security.protocol`` to describe how programs are authenticated to interact with the broker
* ``sasl.username`` and ``sasl.password`` to provide the key and secret of an API key created for the broker

"``[producer]``" section
^^^^^^^^^^^^^^^^^^^^^^^^

Options in the ``[producer]`` section configure the Producer (or group of Producers) used by a program. You can add here any `parameters recognized by Kafka Producers <https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html>`_ in general, but some of the most useful are:

* ``message.max.bytes`` to configure the maximum size of individual messages
* ``batch.size`` to control the maximum number of messages in each batch sent to the broker
* ``linger.ms`` to change how long a batch of messages should wait to become as full as possible before being sent to the broker 
* ``compression.type`` to add or change how batches of messages are compressed before being produced (and decompressed afterward)

Additionally, the ``key.serializer`` and ``value.serializer`` configs allow users to change methods used to convert message keys and values (respectively) to byte arrays. The OpenMSIStream code provides an additional option called ``DataFileChunkSerializer`` that you'll want to use in just about every case as a message value serializer to pack chunks of data files.

"``[consumer]``" section
^^^^^^^^^^^^^^^^^^^^^^^^

Options in the ``[consumer]`` section configure the Consumer (or group of Consumers) used by a program. Again here any `parameters recognized by Kafka Consumers <https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html>`_ in general are valid, but some of the most useful/important for OpenMSIStream are:

* ``group.id`` to group Consumers amongst one another. Giving "``create_new``" for this parameter will create a new group ID every time the code is run. (This config may be overridden by a value from the command line in some cases.)
* ``auto.offset.reset`` to tell the Consumer where in the log to start consuming messages if no previously-committed offset for the consumer group can be found. "``earliest``" will start at the beginning of the topic and "``latest``" will start at the end. Giving "``none``" for this parameter will remove it from the configs, and an error will be thrown if no previously-committed offset for the consumer group can be found.
* ``enable.auto.commit`` to tell the Consumer whether or not to automatically commit offsets. Some portions of the code manually commit offsets, and if this config is left as its default value (True) a Warning will be logged stating that the "at least once" guarantee is not valid unless you set ``enable.auto.commit = False``.
* ``fetch.min.bytes`` to change how many bytes must accumulate before a batch of messages is consumed from the topic (consuming batches of messages is also subject to a timeout, so changing this parameter will only ever adjust the tradeoff between throughput and latency, but will not prevent any messages from being consumed in general)

Additionally, the ``key.deserializer`` and ``value.deserializer`` configs allow users to change methods used to convert message keys and values (respectively) from byte arrays to objects. The OpenMSIStream code provides an additional option called ``DataFileChunkDeserializer`` that you'll want to use in just about every case to convert a chunk of a data file as a byte array to a ``DataFileChunk`` object.
