Production of own logs via Kafka (optional)
---------------------------------------------------------

Many of the programs provided by OpenMSIStream run on remote systems. OpenMSIStream provides an option to send messages containing that nodes OpenMSI logs to a Kafka topic.

"Log" messages are produced at a constant (configurable) interval. By default, each message has a string key like "``[program_ID]_log``" (where ``program_ID`` is set by the user when the program is first started) and a json-formatted string containing a "timestamp" (unix epoch seconds) and a "messages" array of log entries.

Command line arguments
^^^^^^^^^^^^^^^^^^^^^^

All long-running programs accept the following three command line arguments to configure how log messages are sent:

* ``--log_topic_name``: the name of the topic to which log messages should be produced. This parameter must be included on the command line to produce log messages.
* ``--log_program_id``: the "ID" of the long-running program that should uniquely identify it amongst any programs producing log messages to the given topic (this is what goes in the key of each message). If this value isn't given on the command line, the message keys will include the hex code of the log producer's location in memory instead, which WILL change if the program is shut down and restarted (it's also not particularly identifiable or meaningful).
* ``--log_interval_secs``: how often (in seconds) the "log" messages should be sent to the topic.

Broker/producer configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The broker to which the log messages are sent (and the producer used to send them) are configured from the main program configuration file. By default, the broker is configured using the regular "``[broker]``" section of the main program file and the log producer uses the default Kafka Producer configurations.

Logs can be sent to a different broker by including those broker configurations in the optional "``[log]``" section of the main configuration file. If the files includes a "``[log]``" section with a ``bootstrap.servers`` parameter, the "``[log]``" section must also include any other necessary authentication/configuration parameters for the broker to use.

The "``[log]``" section in the configuration file can also contain any of the parameters allowed in the "``[producer]``" section. These parameters will be used to configure the producer that sends the log messages.

Delivery guarantees
^^^^^^^^^^^^^^^^^^^

Log messages are not sent with any callbacks registered. No guarantees are made with respect to their delivery to the registered broker beyond a best-faith effort from the producer sending them. Users should not assume that log messages are complete logs (though they strive to be).

Consuming log messages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Log messages have string keys and values. The value strings are json-formatted and parseable to dictionaries. The following Python code is an example of a program that can run in an OpenMSIStream-configured computing environment to read log messages:

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
    consumer.subscribe(["logs-topic"])
    while True:
        msg = consumer.get_next_message(5)
        if msg is not None:
            value_dict = json.loads(msg.value())
            print(f"[{msg.key()}]: {value_dict}")

(The example is reading from a broker with no user authentication running on localhost port 9092, using a random group ID for the consumer, and no message encryption.)
