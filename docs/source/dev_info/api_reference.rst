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
   api/metadata_json_reproducer
   api/data_file_stream_processor
   api/data_file_stream_reproducer

Kafka wrapper
-------------

.. toctree::
   :maxdepth: 1

   api/openmsistream_producer
   api/openmsistream_consumer
   api/producer_group
   api/consumer_group
   api/consumer_and_producer_group
   api/openmsistream_kafka_crypto
   api/producible

Services/daemons
----------------

.. toctree::
   :maxdepth: 1

   api/windows_service_manager
   api/linux_service_manager

Workflow utility classes
------------------------

.. toctree::
   :maxdepth: 1

   api/runnable
   api/controlled_process
   api/controlled_process_single_thread
   api/controlled_process_multi_threaded
   api/openmsistream_argument_parser
   api/has_arguments
   api/has_argument_parser

Selected base + utility classes
-------------------------------

.. toctree::
   :maxdepth: 1

   api/data_file_chunk
   api/data_file
   api/download_data_file
   api/download_data_file_to_disk
   api/download_data_file_to_memory
   api/data_file_directory
   api/log_owner
   api/logger
   api/dataclass_table
   api/reproducer_message
