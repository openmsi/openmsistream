Configuration files
-------------------

Working with any of the programs in OpenMSIStream requires creating at least one configuration file that will tell the Kafka backend how to connect to the broker and configure the main Producer(s)/Consumer(s) it uses. Information in configuration files is also used to point a Consumer to an S3 bucket to the correct endpoint and connect to it, and to supply information necessary for working with data encrypted using KafkaCrypto. 

Location and Usage
^^^^^^^^^^^^^^^^^^

Programs accept the "``--config``" command line argument to specify the configuration file they should use. The value of that argument can either be the path to the file to use, or the name of a file in the default location. The default location is initially set to the `openmsistream/kafka_wrapper/config directory <https://github.com/openmsi/openmsistream/tree/main/openmsistream/kafka_wrapper/config_files>`_ directory in the installed repository, but you can change its location by setting the ``OPENMSISTREAM_CONFIG_FILE_DIR`` environment variable on your system (note that setting this variable without copying the files from the default location will break the tutorials and CI tests on your system).

You can see what the default config file location is at any time by running, for example::

    UploadDataFile -h

and checking the output message for the "``--config``" argument, if you'd like to put files there and reference them by name instead of by their path in some different location.

The default value of the "``--config``" argument is "``test``" which points to `the test.config file <https://github.com/openmsi/openmsistream/blob/main/openmsistream/kafka_wrapper/config_files/test.config>`_ that comes installed with OpenMSIStream. That file includes "``[broker]``", "``[producer]``", and "``[consumer]``" sections with reasonable all-around values in them, and environment variable references to specify the broker connection. If your broker has plaintext SASL authentication enabled and you set values for the "``KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS``", "``KAFKA_TEST_CLUSTER_USERNAME``", and "``KAFKA_TEST_CLUSTER_PASSWORD``" environment variables, then you can use the default configuration file to get started moving files right away (without encryption or interaction with an S3 bucket). 

In practice, however, different setups benefit from using bespoke configuration settings based on the sizes of files involved and the type of program being run. You can use the example files as starting points to adjust your configuration as necessary, pointing the programs you run to your custom file using the "``--config``" command line argument. In addition to the information below, Confluent has released `a white paper about optimizing Kafka deployments <https://assets.confluent.io/m/6b6d4f8910691700/original/20190626-WP-Optimizing_Your_Apache_Kafka_Deployment.pdf>`_ for throughput, latency, durability, and availability that includes configuration recommendations that you could try out in your own use cases.

Structure
^^^^^^^^^

In general, a configuration file is a text file with one or more distinct and named sections. Comments can be added by using lines starting with "``#``", and other whitespace in general is ignored. Each section begins with a heading line like "``[section_name]``" (with square brackets included), and beneath that heading different parameters are supplied using lines like "``key = value``". The structure of the files is that expected by the Python :mod:`configparser` module; check the docs there for more information if you'd like.

If any parameter's ``value`` begins with the "``$``" character, the configuration file parser will attempt to expand it as an environment variable. This is a useful way to, for example, store secrets like usernames or passwords as environment variables instead of plain text. You can set these environment variables in a shell ``.rc`` or ``.profile`` file if running on Linux or Mac OS. On Windows you can set them as machine environment variables using PowerShell commands like::

    [Environment]::SetEnvironmentVariable("NAME","VALUE",[EnvironmentVariableTarget]::Machine)

"``[broker]``" section
^^^^^^^^^^^^^^^^^^^^^^

Options listed under the ``[broker]`` section heading configure which Kafka broker should be used by a program and how to connect to it. You can add here any parameters recognized by Kafka brokers either in `Confluent Platform <https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html>`_ or in the `librdkafka backend <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_, but common parameters here include:

* ``bootstrap.servers`` to detail the server on which the broker is hosted
* ``sasl.mechanism`` and ``security.protocol`` to describe how programs are authenticated to interact with the broker
* ``sasl.username`` and ``sasl.password`` to provide the key and secret of an API key created for the broker

"``[producer]``" section
^^^^^^^^^^^^^^^^^^^^^^^^

Options in the ``[producer]`` section configure the Producer (or group of Producers) used by a program. You can add here any parameters recognized by Kafka Producers either in `Confluent Platform <https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html>`_ or in the `librdkafka backend <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_, but some of the most useful are:

* ``message.max.bytes`` to configure the maximum size of individual messages
* ``batch.size`` to control the maximum number of messages in each batch sent to the broker
* ``linger.ms`` to change how long a batch of messages should wait to become as full as possible before being sent to the broker 
* ``compression.type`` to add or change how batches of messages are compressed before being produced (and decompressed afterward)

Additionally, the ``key.serializer`` and ``value.serializer`` configs allow users to change methods used to convert message keys and values (respectively) to byte arrays. The OpenMSIStream code provides an additional option called ``DataFileChunkSerializer`` that you'll want to use if you're producing chunks of data files.

"``[consumer]``" section
^^^^^^^^^^^^^^^^^^^^^^^^

Options in the ``[consumer]`` section configure the Consumer (or group of Consumers) used by a program. Again here any parameters recognized by Kafka Consumers either in `Confluent Platform <https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html>`_ or in the `librdkafka backend <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_ are valid, but some of the most useful/important for OpenMSIStream are:

* ``group.id`` to group Consumers amongst one another. Giving "``create_new``" for this parameter will create a new group ID every time the code is run. (This config may be overridden by a value from the command line in some cases.)
* ``auto.offset.reset`` to tell the Consumer where in the log to start consuming messages if no previously-committed offset for the consumer group can be found. "``earliest``" will start at the beginning of the topic and "``latest``" will start at the end. Giving "``none``" for this parameter will remove it from the configs, and an error will be thrown if no previously-committed offset for the consumer group can be found.
* ``enable.auto.commit`` to tell the Consumer whether or not to automatically commit offsets. Some portions of the code manually commit offsets, and if this config is left as its default value (True) a Warning will be logged stating that the "at least once" guarantee is not valid unless you set ``enable.auto.commit = False``.
* ``fetch.min.bytes`` to change how many bytes must accumulate before a batch of messages is consumed from the topic (consuming batches of messages is also subject to a timeout, so changing this parameter will only ever adjust the tradeoff between throughput and latency, but will not prevent any messages from being consumed in general)

Additionally, the ``key.deserializer`` and ``value.deserializer`` configs allow users to change methods used to convert message keys and values (respectively) from byte arrays to objects. The OpenMSIStream code provides an additional option called ``DataFileChunkDeserializer`` that you'll want to use if you're consuming messages that are chunks of data files produced by OpenMSIStream.
