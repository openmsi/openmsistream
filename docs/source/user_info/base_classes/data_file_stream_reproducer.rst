========================
DataFileStreamReproducer
========================

.. image:: ../../images/data_file_stream_reproducer.png
   :alt: DataFileStreamReproducer
   :scale: 80 %

A DataFileStreamReproducer is a base class that can be extended to compute arbitrary messages from whole data files as they're reconstructed from a Kafka topic, and then "re-produce" those computed messages to a different topic. DataFileStreamReproducer-type classes can be used, for example, to automatically run analyses on data files and produce the results of those analyses to a different topic than the one storing the chunks of the raw data files. The :class:`~.metadata_extraction.MetadataJSONReproducer` class is a specific type of DataFileStreamReproducer.

The main function that users will need to write to instantiate DataFileStreamReproducer-type classes is :func:`openmsistream.DataFileStreamReproducer._get_processing_result_message_for_file`, which will compute and return the "processing result message" for each reconstructed data file as they become available. Running the class using the :class:`~.utilities.Runnable` workflow also requires writing a :func:`openmsistream.utilities.Runnable.run_from_command_line` function.

The first argument to the :func:`openmsistream.DataFileStreamReproducer._get_processing_result_message_for_file` function is an :class:`~.data_file_io.DownloadDataFileToMemory` object holding a file that has been fully reconstructed from chunks in a topic. The content of the file can be accessed using its :attr:`bytestring` attribute. The second argument to that function is a :class:`threading.Lock` object that can be used to ensure only one instance of the function is running at a time even though multiple threads may be running, in case computing the result requires modifying a shared resource. The function should return a :class:`~.data_file_io.ReproducerMessage` object so that the Producer callback arguments are already defined. If computing the message fails, an explanatory warning or error should be logged in :func:`openmsistream.DataFileStreamReproducer._get_processing_result_message_for_file` and None should be returned.

The :func:`openmsistream.utilities.Runnable.run_from_command_line` function is a class method that should be written like a script to run the class end-to-end. In that function, a user can call :func:`openmsistream.utilities.Runnable.get_argument_parser` to get the command-line argument parser to use. If a user would like to add additional command line arguments for the extended class they can implement the :func:`openmsistream.utilities.Runnable.get_command_line_arguments` class method. Command line arguments already defined include the config file, consumer and producer topic names/numbers of threads, and output location; they can be picked up by calling :func:`super().get_command_line_arguments` in the extended class.

With a :func:`openmsistream.utilities.Runnable.run_from_command_line` function defined, adding a block like this::

    def main() :
        ClassName.run_from_command_line()

    if __name__=='__main__' :
        main()

to the bottom of the file will allow the class to be run as a module, with::

    >>> python -m path.to.class.file [arguments]

Please see :doc:`the page about the S3TransferStreamProcessor <../main_programs/s3_transfer_stream_processor>` to better understand the structure of the logging files that a DataFileStreamReproducer-type program will create, how offsets are manually committed, and guarantees for restarting in the case of an abrupt shutdown of the program.