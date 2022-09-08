======================
MetadataJSONReproducer
======================

A MetadataJSONReproducer is a useful base class that can be extended to run arbitrary metadata extraction Python code on whole data files as they're reconstructed from messages on a Kafka topic, and then produce those metadata keys and values as a JSON-formatted string to a different Kafka topic. Extending a MetadataJSONReproducer for a use case usually only requires writing a custom :func:`openmsistream.metadata_extraction.MetadataJSONReproducer._get_metadata_dict_for_file` function to return a dictionary of metadata keys and values given a Datafile object.

A MetadataJSONReproducer is a special type of :class:`openmsistream.DataFileStreamReproducer`.

.. autoclass:: openmsistream.metadata_extraction.MetadataJSONReproducer
   :private-members:
   :exclude-members: _on_check, _on_shutdown, _get_processing_result_message_for_file