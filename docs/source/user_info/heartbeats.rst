"Heartbeat" messages for long-running programs (optional)
---------------------------------------------------------

Many of the programs provided by OpenMSIStream are designed to run for long periods of time on remote systems. OpenMSIStream therefore provides an option to periodically send "heartbeat" messages to a Kafka topic to give users insight into which of their programs are still running, even remotely. 

"Heartbeat" messages have a consistent structure and are produced at a constant (configurable) interval. By default, each message has a string key like "``[program_ID]_heartbeat``" (where ``program_ID`` is set by the user when the program is first started) and a json-formatted string value with a "timestamp" field (timestamps are given in ISO format in the program-local timezone). Additional fields in the heartbeat message values are detailed on the individual program/base class pages linked above, but they generally include the number of messages and bytes produced/read/processed since the last heartbeat was sent.

Command line arguments
^^^^^^^^^^^^^^^^^^^^^^

All long-running programs accept the following three command line arguments to configure how heartbeat messages are sent:

* ``--heartbeat_topic_name``: the name of the topic to which heartbeat messages should be produced. This parameter must be included on the command line to produce heartbeat messages.
* ``--heartbeat_program_id``: the "ID" of the long-running program that should uniquely identify it amongst any programs producing heartbeat messages to the given topic (this is what goes in the key of each message). If this value isn't given on the command line, the message keys will include the hex code of the heartbeat producer's location in memory instead, which WILL change if the program is shut down and restarted (it's also not particularly identifiable or meaningful).
* ``--heartbeat_interval_secs``: how often (in seconds) the "heartbeat" messages should be sent to the topic. NOTE: this parameter (and the discussion of "heartbeats" in this section in general) is completely independent of the "``heartbeat.interval.ms``" configuration parameter for Kafka consumers (all Kafka consumers send periodic "heartbeats" of their own to the broker so that brokers know which consumers are still part of the group and which have stopped listening to topics).

Broker/producer configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The broker to which the heartbeat messages are sent (and the producer used to send them) are configured from the main program configuration file. By default, the broker is configured using the regular "``[broker]``" section of the main program file and the heartbeat producer uses the default Kafka Producer configurations.

Heartbeats can be sent to a different broker by including those broker configurations in the optional "``[heartbeat]``" section of the main configuration file. If the files includes a "``[heartbeat]``" section with a ``bootstrap.servers`` parameter, the "``[heartbeat]``" section must also include any other necessary authentication/configuration parameters for the broker to use.

The "``[heartbeat]``" section in the configuration file can also contain any of the parameters allowed in the "``[producer]``" section. These parameters will be used to configure the producer that sends the heartbeat messages.

Delivery guarantees
^^^^^^^^^^^^^^^^^^^

Heartbeat messages are not sent with any callbacks registered. No guarantees are made with respect to their delivery to the registered broker beyond a best-faith effort from the producer sending them. Users should not assume that heartbeat messages accurately reflect, for example, the amount of data stored in a particular topic, and should only use heartbeat messages as approximate metrics and for general health checks.

Consuming heartbeat messages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Heartbeat messages have string keys and values. The value strings are json-formatted and parseable to dictionaries. The following Python code is an example of a program that can run in an OpenMSIStream-configured computing environment to read heartbeat messages:

::

    import uuid, json
    from confluent_kafka.serialization import StringDeserializer
    from confluent_kafka import DeserializingConsumer
    from openmsistream.kafka_wrapper import OpenMSIStreamConsumer

    configs = {
        "bootstrap.servers":"localhost:9092",
        "key.deserializer": StringDeserializer(),
        "value.deserializer": StringDeserializer(),
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
    }
    consumer = OpenMSIStreamConsumer(DeserializingConsumer, configs)
    consumer.subscribe(["heartbeats"])
    while True:
        msg = consumer.get_next_message(5)
        if msg is not None:
            value_dict = json.loads(msg.value())
            print(f"[{msg.key()}]: {value_dict}")

(The example is reading from a broker with no user authentication running on localhost port 9092, using a random group ID for the consumer.)
