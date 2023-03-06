=========
Tutorials
=========

The hands-on examples described in this section will walk through some of the key functionality of the OpenMSIStream package. After setting up access to a Kafka cluster, creating a topic to use for testing, and downloading some specific example data, you'll use a :doc:`DataFileUploadDirectory <../user_info/main_programs/data_file_upload_directory>` producer to upload the example files from your local machine to the topic. You'll then use a :doc:`DataFileDownloadDirectory <../user_info/main_programs/>` consumer to reconstruct the files to your local machine from their chunks stored on the topic.

More advanced tutorials explore other OpenMSIStream functionality by setting up additional types of consumers on your local machine. An example :doc:`DataFileStreamProcessor <../user_info/base_classes/data_file_stream_processor>` can create simple plots from the data in the files on the topic.  An example :doc:`MetadataJSONReproducer <../user_info/base_classes/metadata_json_reproducer>` can extract the metadata blocks of the example files and produce those blocks to a second topic as JSON-formatted strings. An :doc:`S3TransferStreamProcessor <../user_info/main_programs/s3_transfer_stream_processor>` can reconstruct the files in an S3 bucket instead of locally on disk (after some more setup).

Initial setup
=============

To get started, you'll need to get access to a Kafka broker, create at least two topics to use for testing, set some related environment variables (in order to use the example config files), and download some data to use in your tests.

Access to a Kafka broker
------------------------

To begin, you'll need access to a Kafka broker. That broker can be running locally or on a server that you have access to (possibly in Docker), or on `Confluent Cloud <https://confluent.cloud/>`_, which provides Kafka as a managed service. If ACLs are defined for your broker, you will need to be able to authenticate to it using plaintext SASL authentication (the default for Confluent Cloud). 

You will also need to create a topic on that broker to use for testing, called "``openmsistream_tutorial_data``" (if you use a different name just replace it in the commands shown below). The default settings are fine for a single test, but if you'd like you can adjust the retention times to be an hour or less so that the contents of the testing topics will get flushed out regularly and you can work through the tutorial repeatedly without reading previously-produced data.

The OpenMSIStream GitHub repository includes a `Docker compose .yaml file <https://github.com/openmsi/openmsistream/blob/main/test/local-kafka-broker-docker-compose.yml>`_ that you can use to set up a very simple single-node broker on your local machine (if you have Docker installed). To start up the broker, run `the command shown in the "start broker" shell script <https://github.com/openmsi/openmsistream/blob/main/test/start_local_broker.sh#L5>`_. After starting the local broker, you can add topics using the Kafka CLI, like what's shown in the `"create topics" shell script <https://github.com/openmsi/openmsistream/blob/main/test/create_local_testing_topics.sh#L5>`_. You can stop and delete the broker using `the command in the "stop broker" shell script <https://github.com/openmsi/openmsistream/blob/33-use-local-kafka-broker/test/stop_local_broker.sh#L5>`_.

Setting environment variables
-----------------------------

Next you'll need to set some environment variables needed to connect to the Kafka broker. You'll need the "Bootstrap server" for the cluster, and a username and password if your broker requires authentication. If you're using the example local broker (or another broker with no authentication required), you should set the "``LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS``" environment variable to the broker's server (in the case of the example local broker this is ``localhost:9092``).

If you're using Confluent Cloud, you can find the bootstrap server on the "Cluster settings" page from the left hand menu, and you can create a new API key using the "API keys" page also on the left hand menu. Set the corresponding values for the "``KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS``", "``KAFKA_TEST_CLUSTER_USERNAME``" and "``KAFKA_TEST_CLUSTER_PASSWORD``" environment variables on your system [#]_.

Downloading test data
---------------------

Lastly, you'll need to download some test data to work with. The five files linked below contain X-Ray Diffraction (XRD) data made publicly available under the `CC BY-NC-ND 4.0 <https://creativecommons.org/licenses/by-nc-nd/4.0/>`_ license by the `PARADIM Materials Innovation Platform <https://www.paradim.org/>`_, as part of the work leading to the publication of S. Chae et. al., "Epitaxial stabilization of rutile germanium oxide thin film by molecular beam epitaxy", Applied Physics Letters 117, 072105 (2020) `https://doi.org/10.1063/5.0018031 <https://doi.org/10.1063/5.0018031>`_. You can download these five files to your machine to use them in this tutorial.

* `SC001 XRR.csv <https://data.paradim.org/194/XRD/SC001/SC001%20XRR.csv>`_
* `SC002 XRR.csv <https://data.paradim.org/194/XRD/SC002/SC002%20XRR.csv>`_
* `SC006 XRR.csv <https://data.paradim.org/194/XRD/SC006/SC006%20XRR.csv>`_
* `SC007 XRR.csv <https://data.paradim.org/194/XRD/SC007/SC007%20XRR.csv>`_
* `SC009 XRR.csv <https://data.paradim.org/194/XRD/SC009/SC009%20XRR.csv>`_

Each file contains a header block with some metadata, and then a block of XRD data in two columns ("angle" and "intensity"). We'll produce the full contents of these files to the Kafka topic you just created, and then we'll consume them in several different ways to demonstrate how OpenMSIStream can be used.

"Round trip" tutorial
=====================

This simple tutorial will produce the data files you downloaded to a Kafka topic and then consume the messages in that topic to reconstruct copies of the files locally.

Create a directory called "``openmsistream_upload``" that is initially empty. Then activate your ``openmsi`` Conda environment and type:: 
    
    DataFileUploadDirectory openmsistream_upload --topic_name openmsistream_tutorial_data 

If you're using the local example broker (or another unauthenticated broker), add ``--config local_broker_test.config`` to the command.

The terminal with that command in it will stay running, listening for files to be added to the ``openmsistream_upload`` directory. As soon as you run it, you can see that the "``LOGS``" subdirectory will be created inside the ``openmsistream_upload`` directory, and that subdirectory will have a log file and a .csv file in it.

Leave that terminal running, and open another to run a local consumer. Activate the ``openmsi`` Conda environment in the new terminal, and type::

    DataFileDownloadDirectory openmsistream_download --topic_name openmsistream_tutorial_data

If you're using the local example broker (or another unauthenticated broker), add ``--config local_broker_test.config`` to the command.

That terminal will also keep running, but in the meantime you'll see that a directory called ``openmsistream_download`` will be created, and it will have a log file in it.

With both terminal windows running, you can then move the test files you downloaded into the ``openmsistream_upload`` directory. You can also create any subdirectory tree you'd like inside the ``openmsistream_upload`` directory, and add test files to any subdirectory (other than the ``LOGS`` subdirectory). 

As files are recognized by the producer, you'll see the same files appear with the same subdirectory structure inside the ``openmsistream_download`` directory. At any time you can type "c" or "check" into either of the terminal windows and you'll get a message stating how many files have been uploaded, or how many messages have been received/files have been completely reconstructed. When you've added all of the test files you downloaded, type "q" or "quit" into both of the terminals to shut down the processes, and you'll get output messages listing which files were uploaded and downloaded. 

At that point you should also be able to see all of the test files listed in ``openmsistream_upload/LOGS/uploaded_to_openmsistream_tutorial_data.csv``. Both of the log files should be empty if everything proceeded normally, because they only log warnings or errors. 

If you'd like to do this tutorial again you'll need to delete the ``LOGS`` subdirectory (otherwise the producer will skip files that have been uploaded previously). You can see more about what's going on under the hood by running either or both of the commands above with the flag ``--logger_stream_level debug``, in which case more messages will be printed as files are uploaded and downloaded, or with the ``-h`` flag to see all of the command line options. You can also upload/download any files you'd like, but the test files used here were chosen for the advanced tutorials in the following pages.

More advanced tutorials
=======================

The pages below contain some more hands-on examples of different ways to consume the data you've produced to the topic to demonstrate other functionality available in OpenMSIStream.

.. toctree::
   :maxdepth: 1

   tutorials/s3_transfer
   tutorials/creating_plots
   tutorials/extracting_metadata

You can work through these examples as well, if you'd like, or you can skip to the :doc:`documentation on the main OpenMSIStream programs <../user_info/main_programs>` if you'd like more detailed information about the programs you've run above or the base classes provided by OpenMSIStream. The pages about :doc:`services/daemons <../user_info/services>` and :doc:`encryption <../user_info/encryption>` describe more options for setup and deployment.

.. 
    footnote below

.. [#] The environment variables are referenced in the default configuration files used for this tutorial. You can also write your own configuration files to use if you'd rather not set environment variables; see :doc:`the page on configuration files <../user_info/config_files>` for more information.