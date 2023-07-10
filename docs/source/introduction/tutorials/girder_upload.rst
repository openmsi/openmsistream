=================================================
Uploading to Girder (GirderUploadStreamProcessor)
=================================================

This tutorial will walk through setting up a Consumer to read the test files uploaded to the ``openmsistream_tutorial_data`` topic and transfer them to Girder instance. You'll need access to a Girder instance in order to work through this example; the tutorial will use a local Girder instance running in Docker, but all you need is the URL of the REST API endpoint and an API key.

If you haven't done the "round trip" :ref:`tutorial <"Round trip" tutorial>` yet, or if the messages produced during that tutorial no longer exist on the ``openmsistream_tutorial_data`` topic, you should go back and produce the test files to that topic. Then activate the ``openmsi`` Conda environment (or keep it activated).

To set up a local Girder instance using a local MongoDB backend, you can use the `Docker compose .yml file <https://github.com/openmsi/openmsistream/blob/main/test/local-girder-docker-compose.yml>`_ in the OpenMSIStream repository. With the local instance running you can then create a new admin User and API key, using the `web API served at http://localhost:8080/api/v1 <http://localhost:8080/api/v1>`_ or external requests to the REST API at the same URL. 

With the instance running, an admin account created with an API key, and the test data produced, you can run the consumer with the following command::

    GirderUploadStreamProcessor [girder_api_url] [girder_api_key] --topic_name openmsistream_tutorial_data --collection_name "Girder Upload Test"

where ``[girder_api_url]`` is the URL of your Girder instance's REST API endpoint ("``http://localhost:8080/api/v1``" if you're using the local Girder instance) and ``[girder_api_key]`` is the value of the API key you created. If you're using the local example broker (or another unauthenticated broker), add ``--config local_broker_test.config`` to the command.

Starting that process running will create a directory called ``GirderUploadStreamProcessor_output``; inside that directory you'll find a ``LOGS`` subdirectory containing a log file and some :class:`~.utilities.DataclassTable` files. While the process runs, you can type ``c`` or ``check`` into the terminal to see how many messages have been received and how many files have been uploaded. You can shut the process down by typing ``q`` or ``quit`` into the terminal. 

After the process quits, you should see all five test files listed in the ``GirderUploadStreamProcessor_output/LOGS/processed_from_openmsistream_tutorial_data_by_[consumerID].csv`` file, and you'll see the same subdirectory structure from your upload run reproduced inside the Girder instance, under a new collection named ``Girder Upload Test`` (if you're using the local Girder instance, you can browse it at the URL `http://localhost:8080 <http://localhost:8080/api/v1>`_).

You can see the other options available for this example consumer by adding the ``-h`` flag to the command above.

.. 
    footnote below

.. [#] The environment variables are referenced in the default configuration file used for this tutorial. You can also write your own configuration file to use if you'd rather not set environment variables; see :doc:`the page on configuration files <../user_info/config_files>` and :doc:`the page on the GirderUploadStreamProcessor <../user_info/main_programs/s3_transfer_stream_processor>` for more information.
