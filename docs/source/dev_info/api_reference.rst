=============
API reference
=============

On the pages linked below we document the details of some of the main Python classes provided by OpenMSIStream. Users may find this reference helpful in extending OpenMSIStream classes for customized applications.

Top-level classes
-----------------

.. toctree::
   :maxdepth: 1

   api/upload_data_file.rst
   api/data_file_upload_directory.rst
   api/data_file_download_directory.rst
   api/s3_transfer_stream_processor.rst
   api/data_file_stream_processor

Kafka wrapper
-------------

.. toctree::
   :maxdepth: 1

   api/openmsistream_producer
   api/openmsistream_consumer
   api/producer_group
   api/consumer_group
   api/openmsistream_kafka_crypto

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

   api/runnable
   api/log_owner
   api/logger
   api/openmsistream_argument_parser
   api/dataclass_table