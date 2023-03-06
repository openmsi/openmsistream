================================================
Creating plots (example DataFileStreamProcessor)
================================================

This tutorial will walk through setting up a Consumer to read the test files uploaded to the ``openmsistream_tutorial_data`` topic and automatically make simple scatter plots of the intensity vs. angle XRD data they contain. To get set up, you'll first need to download the example `Python code <https://github.com/openmsi/openmsistream/tree/main/examples/creating_plots/xrd_csv_plotter.py>`_ for this tutorial from the OpenMSIStream GitHub repository.

If you haven't done the "round trip" :ref:`tutorial <"Round trip" tutorial>` yet or if the messages produced during that tutorial no longer exist on the ``openmsistream_tutorial_data`` topic, you should go back and produce the test files to that topic. Then activate the ``openmsi`` Conda environment (or keep it activated).

With the code downloaded and the test data produced, you can run the consumer with the following command::

    python -m xrd_csv_plotter --topic_name openmsistream_tutorial_data

If you're using the local example broker (or another unauthenticated broker), add ``--config local_broker_test.config`` to the command.

Starting that process running will create a directory called ``XRDCSVPlotter_output``; inside that directory you'll find a ``LOGS`` subdirectory containing a log file and some :class:`~.utilities.DataclassTable` files. While the process runs, you can type ``c`` or ``check`` into the terminal to see how many messages have been received, how many of the test files have been reconstructed, and how many plots have been made. You can shut the process down by typing ``q`` or ``quit`` into the terminal. 

After the process quits, you should see all five test files listed in the ``XRDCSVPlotter_output/LOGS/processed_from_openmsistream_tutorial_data_by_[consumerID].csv`` file, and you'll see the same subdirectory structure from your upload run reproduced inside the ``XRDCSVPlotter_output`` directory with the original data files replaced by images showing simple scatter plots of their data.

You can see the other options available for this example consumer by adding the ``-h`` flag to the command above, and you can edit the script you downloaded and rerun it to change aspects of the plots or how the output gets organized.
