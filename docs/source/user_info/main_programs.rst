=========================================
Using Main Programs
=========================================

.. include:: config_files.rst

Details on programs
-------------------

The main functional programs provided in OpenMSIStream are accessible through special "console entry point" commands. The pages linked below will describe how these programs can be run from the command line and what they do. 

.. toctree::
   :maxdepth: 1

   main_programs/upload_data_file
   main_programs/data_file_upload_directory
   main_programs/data_file_download_directory
   main_programs/s3_transfer_stream_processor

Installing programs as Services or daemons
------------------------------------------

Instances of each of the main programs can also be installed as Windows Services or Linux daemons to keep them running persistently. OpenMSIStream includes some wrapper/utility functions to facilitate working with programs installed as Services or daemons, and is designed to make the process as transparent as possible to the end user by giving them the same command line arguments to use for running programs on the command line and installing them persistently. Please see the :doc:`Services/Daemons <services>` page for more information on how to set up a Service or daemon on your system.

Encrypting data (optional)
--------------------------

The messages sent and received by the main programs above (running either interactively or as Services/daemons) can optionally be encrypted while stored on the broker, so that only the Producer/Consumer endpoints need to be trusted. OpenMSIStream includes a wrapper around `KafkaCrypto <https://github.com/tmcqueen-materials/kafkacrypto>`_ to facilitate this encryption. Please see :doc:`the page on message encryption <encryption>` for more information, including how to provision nodes and set up configuration files to encrypt messages.

