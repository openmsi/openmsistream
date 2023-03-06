=======================================================
Transfering to an S3 bucket (S3TransferStreamProcessor)
=======================================================

This tutorial will walk through setting up a Consumer to read the test files uploaded to the ``openmsistream_tutorial_data`` topic and transfer them to an S3 object store bucket. You'll need externally-configured access to an S3 bucket in order to work through this example. Make note of the access key ID, the secret key ID, the endpoint URL, and the global region (i.e. "us-east-2") for your bucket, and set environment variables [#]_ called ``$ACCESS_KEY_ID``, ``$SECRET_KEY_ID``, ``$ENDPOINT_URL``, and ``$REGION``, respectively, to store them. You'll also need the name of your bucket for a command line argument in a moment.

If you haven't done the "round trip" :ref:`tutorial <"Round trip" tutorial>` yet, or if the messages produced during that tutorial no longer exist on the ``openmsistream_tutorial_data`` topic, you should go back and produce the test files to that topic. Then activate the ``openmsi`` Conda environment (or keep it activated).

With the environment variables set on your system and the test data produced, you can run the consumer with the following command::

    S3TransferStreamProcessor [bucket_name] --topic_name openmsistream_tutorial_data

where ``[bucket_name]`` is the name of your S3 bucket. If you're using the local example broker (or another unauthenticated broker), add ``--config local_broker_test_s3_transfer.config`` to the command.

Starting that process running will create a directory called ``S3TransferStreamProcessor_output``; inside that directory you'll find a ``LOGS`` subdirectory containing a log file and some :class:`~.utilities.DataclassTable` files. While the process runs, you can type ``c`` or ``check`` into the terminal to see how many messages have been received and how many files have been transferred to the S3 bucket. You can shut the process down by typing ``q`` or ``quit`` into the terminal. 

After the process quits, you should see all five test files listed in the ``S3TransferStreamProcessor_output/LOGS/processed_from_openmsistream_tutorial_data_by_[consumerID].csv`` file, and you'll see the same subdirectory structure from your upload run reproduced inside the S3 bucket, under a new directory named ``openmsistream_tutorial_data``.

You can see the other options available for this example consumer by adding the ``-h`` flag to the command above.

.. 
    footnote below

.. [#] The environment variables are referenced in the default configuration file used for this tutorial. You can also write your own configuration file to use if you'd rather not set environment variables; see :doc:`the page on configuration files <../user_info/config_files>` and :doc:`the page on the S3TransferStreamProcessor <../user_info/main_programs/s3_transfer_stream_processor>` for more information.
