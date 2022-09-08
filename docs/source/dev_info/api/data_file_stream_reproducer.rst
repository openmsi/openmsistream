========================
DataFileStreamReproducer
========================

A DataFileStreamReproducer is a useful base class that can be extended to run arbitrary Python code that computes some sort of "result message" on whole data files as they're reconstructed from messages on a Kafka topic, and then produce those "result messages" to a different Kafka topic. 

.. autoclass:: openmsistream.DataFileStreamReproducer
   :private-members:
   :exclude-members: _on_check
