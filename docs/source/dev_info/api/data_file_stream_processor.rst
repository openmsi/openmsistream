=======================
DataFileStreamProcessor
=======================

A DataFileStreamProcessor is a useful base class that can be extended to run arbitrary Python code on whole data files as they're reconstructed from messages on a Kafka topic. The :class:`openmsistream.S3TransferStreamProcessor` class is one example of an implemented DataFileStreamProcessor.

.. autoclass:: openmsistream.DataFileStreamProcessor
   :private-members:
   :exclude-members: _on_check
