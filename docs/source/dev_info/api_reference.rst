=============
API reference
=============

On the pages linked below we document the details of some of the main Python classes provided by OpenMSIStream. Users may find this reference helpful in extending OpenMSIStream classes for customized applications.

Top-level classes
-----------------

.. toctree::
   :maxdepth: 1

   api/upload_data_file
   api/data_file_upload_directory
   api/data_file_download_directory
   api/s3_transfer_stream_processor
   api/girder_upload_stream_processor
   api/metadata_json_reproducer
   api/data_file_stream_processor
   api/data_file_stream_reproducer

Kafka wrapper
-------------

.. toctree::
   :maxdepth: 1

   api/openmsistream_producer
   api/openmsistream_consumer
   api/consumer_and_producer_group
   api/openmsistream_kafka_crypto
   api/producible

Services/daemons
----------------

.. toctree::
   :maxdepth: 1

   api/windows_service_manager
   api/linux_service_manager

Selected base + utility classes
-------------------------------

.. toctree::
   :maxdepth: 1

   api/data_file_chunk
   api/data_file
   api/download_data_file
   api/download_data_file_to_disk
   api/download_data_file_to_memory
   api/download_data_file_to_memory_and_disk
   api/data_file_directory
   api/dataclass_table
   api/reproducer_message
