"""
A stream handler that triggers a function to compute a message on file completion.
Produces the computed message to a different topic.
"""

# imports
from abc import ABC, abstractmethod
from ..config import DATA_FILE_HANDLING_CONST
from .data_file_chunk_handlers import DataFileChunkReproducer
from .data_file_stream_handler import DataFileStreamHandler
from .file_registry.stream_handler_registries import StreamReproducerRegistry


class DataFileStreamReproducer(DataFileStreamHandler, DataFileChunkReproducer, ABC):
    """
    Consumes :class:`~.data_file_io.entity.data_file_chunk.DataFileChunk` messages
    into memory, compute some processing result when entire files are available,
    and produce that result to a different topic.

    This is a base class that cannot be instantiated on its own.

    :param config_file: Path to the config file to use in defining the Broker connection,
        Consumers, and Producers
    :type config_file: :class:`pathlib.Path`
    :param consumer_topic_name: Name of the topic to which the Consumers should be subscribed
    :type consumer_topic_name: str
    :param producer_topic_name: Name of the topic to which the Producer should produce
        the processing results
    :type producer_topic_name: str
    :param output_dir: Path to the directory where the log and csv registry files should be kept
        (if None a default will be created in the current directory)
    :type output_dir: :class:`pathlib.Path`, optional
    :param mode: a string flag determining whether reconstructed data files should
        have their contents stored only in "memory" (the default, and the fastest),
        only on "disk" (in the output directory, to reduce the memory footprint),
        or "both" (for flexibility in processing)
    :type mode: str, optional
    :param datafile_type: the type of data file that recognized files should be reconstructed as.
        Default options are set automatically depending on the "mode" argument.
        (must be a subclass of :class:`~.data_file_io.DownloadDataFile`)
    :type datafile_type: :class:`~.data_file_io.DownloadDataFile`, optional
    :param n_producer_threads: the number of producers to run. The total number of
        producer/consumer threads started is `max(n_consumer_threads,n_producer_threads)`.
    :type n_producer_threads: int, optional
    :param n_consumer_threads: the number of consumers to run. The total number of
        producer/consumer threads started is `max(n_consumer_threads,n_producer_threads)`.
    :type n_consumer_threads: int, optional
    :param consumer_group_id: the group ID under which each consumer should be created
    :type consumer_group_id: str, optional
    :param filepath_regex: If given, only messages associated with files whose paths match
        this regex will be consumed
    :type filepath_regex: :type filepath_regex: :func:`re.compile` or None, optional

    :raises ValueError: if `datafile_type` is not a subclass of
        :class:`~.data_file_io.DownloadDataFileToMemory`, or more specific as determined
        by the "mode" argument
    """

    def __init__(self, config_file, consumer_topic_name, producer_topic_name, **kwargs):
        """
        Constructor method signature duplicated above to display in Sphinx docs
        """
        super().__init__(config_file, consumer_topic_name, producer_topic_name, **kwargs)

    def produce_processing_results_for_files_as_read(self):
        """
        Consumes messages in several parallel threads and stores their data in memory. Calls
        :func:`~_get_processing_result_message_for_file` for fully-read files to get their
        processing result messages. Produces processing result messages in several parallel
        threads as they're generated. Runs until the user inputs a command to shut it down.

        :return: the total number of messages consumed
        :rtype: int
        :return: the total number of messages processed (registered in memory)
        :rtype: int
        :return: the number of files reconstructed from the topic
        :rtype: int
        :return: the number of files whose processing results were successfully produced
            to the Producer topic
        :rtype: int
        :return: the paths of up to 50 most recent files whose processing results were
            successfully produced to the Producer topic during the run
        :rtype: list
        """
        # startup message
        msg = (
            f"Will process files from messages in the {self.consumer_topic_name} topic "
            f"using {self.n_consumer_threads} thread"
            f'{"s" if self.n_consumer_threads>1 else ""} and produce their '
            f"processing results to the {self.producer_topic_name} topic using "
            f'{self.n_producer_threads} thread{"s" if self.n_producer_threads>1 else ""}'
        )
        self.logger.info(msg)
        # set up the stream reproducer registry
        self.file_registry = StreamReproducerRegistry(
            dirpath=self._logs_subdir,
            consumer_topic_name=self.consumer_topic_name,
            consumer_group_id=self.consumer_group_id,
            producer_topic_name=self.producer_topic_name,
            logger=self.logger,
        )
        # if there are files that need to be re-processed,
        # set the variables to re-read messages from those files
        if self.file_registry.rerun_file_key_regex is not None:
            msg = (
                f'Consumer{"s" if self.n_consumer_threads>1 else ""} will start from '
                "the beginning of the topic to re-read messages for "
                f"{self.file_registry.n_files_to_rerun} previously-failed "
                f'file{"s" if self.file_registry.n_files_to_rerun>1 else ""}'
            )
            self.logger.info(msg)
            self.restart_at_beginning = True
            self.message_key_regex = self.file_registry.rerun_file_key_regex
        # create the arguments for each _run_worker thread
        run_worker_args_per_thread = []
        for ti in range(self.n_threads):
            run_worker_args_per_thread.append(
                [ti < self.n_consumer_threads, ti < self.n_producer_threads]
            )
        run_worker_kwargs_per_thread = {
            "produce_from_queue_kwargs": {
                "callback": self.producer_callback,
                "print_every": 1,
            }
        }
        # call the run loop
        # pylint: disable=unexpected-keyword-arg
        self.run(
            args_per_thread=run_worker_args_per_thread,
            kwargs_per_thread=run_worker_kwargs_per_thread,
        )
        # return the results of the processing
        return (
            self.n_msgs_read,
            self.n_msgs_processed,
            self.n_processed_files,
            self.n_results_produced_files,
            self.recent_results_produced,
        )

    def producer_callback(self, err, msg, prodid, filename, rel_filepath, n_total_chunks):
        """
        A reference to this method is given as the callback for each call to
        :func:`confluent_kafka.Producer.produce`. It is called for every message
        upon acknowledgement by the broker, and it uses the file registries in the LOGS
        subdirectory to keep the information about what has and hasn't been uploaded
        current with what has been received by the broker.

        Messages associated with an error from the broker will be recomputed and added
        back to the queue to be produced again, logging an error and registering the file
        as failed if the message can't be computed from the datafile.

        Messages that are successfully produced will move their associated data files
        to the "results_produced" csv file.

        :param err: The error object for the message
        :type err: :class:`confluent_kafka.KafkaError`
        :param msg: The message object
        :type msg: :class:`confluent_kafka.Message`
        :param prodid: The ID of the producer that produced the message
            (hex(id(producer)) in memory)
        :type prodid: str
        :param filename: The name of the file that was used to create this processing result message
        :type filename: str
        :param rel_filepath: The path to the file that was used to create this
            processing result message, relative to the
            :class:`~.data_file_io.entity.data_file_chunk.DataFileChunk`'s root directory
        :type rel_filepath: :class:`pathlib.Path`
        :param n_total_chunks: The total number of chunks in the file used to create this
            processing result message
        :type n_total_chunks: int
        """
        # If no error occured, increment the counter for the number of messages produced
        if err is None and msg.error() is None:
            with self.lock:
                self.n_msgs_produced += 1
                self.n_msgs_produced_since_last_heartbeat += 1
                self.n_bytes_produced_since_last_heartbeat += len(msg)
        # If any error occured, log a warning and re-enqueue the message to be produced again
        if err is None and msg.error() is not None:
            err = msg.error()
        if err is not None:
            self.file_registry.register_file_result_production_failed(
                filename, rel_filepath, n_total_chunks
            )
            if err.fatal():
                warnmsg = (
                    "WARNING: fatally failed to deliver processing result message for "
                    f"{rel_filepath}. This message will be re-enqueued. "
                    f"Error reason: {err.str()}"
                )
            elif not err.retriable():
                warnmsg = (
                    "WARNING: Failed to deliver processing result message for "
                    f"{rel_filepath} and cannot retry. This message will be re-enqueued. "
                    "Error reason: {err.str()}"
                )
            self.logger.warning(warnmsg)
            datafile = self.files_in_progress_by_path[rel_filepath]
            new_msg = self._get_processing_result_message_for_file(datafile, self.lock)
            if new_msg is not None:
                self.producer_message_queue.put(new_msg)
            else:
                self._failed_computing_processing_result(datafile, self.lock)
        # Otherwise, register the associated file as having its processing result produced
        else:
            with self.lock:
                self.file_registry.register_file_results_produced(
                    filename, rel_filepath, n_total_chunks, prodid
                )
                self.recent_results_produced.append(rel_filepath)
                while len(self.recent_results_produced) > self.N_RECENT_FILES:
                    _ = self.recent_results_produced.pop(0)
                self.n_results_produced_files += 1
                # stop tracking the file
                del self.files_in_progress_by_path[rel_filepath]
                del self.locks_by_fp[rel_filepath]
            debugmsg = (
                f"Processing result message for {rel_filepath} has been received by "
                f"the broker for the {self.producer_topic_name} topic"
            )
            self.logger.debug(debugmsg)

    def _process_message(self, lock, msg, rootdir_to_set=None):
        """
        Process a single message to add it to a file being held in memory
        until all messages are received.

        If the message failed to be decrypted, this method calls
        :func:`~_undecryptable_message_callback` and returns.

        If the message is the first one consumed for a particular file, or any message other
        than the last one needed, it registers the file as 'in_progress' in the .csv file.

        If the message is the last message needed for a file and its contents match the
        original hash of the file on disk, this method calls
        :func:`~_get_processing_result_message_for_file` and enqueues the result to be produced.
        The file is moved to the 'results_produced' .csv file when the produced message
        is acknowledged by the broker through :func:`~producer_callback`.

        If the call to :func:`~_get_processing_result_message_for_file` returns None,
        this method calls :func:`~_failed_computing_processing_result` and returns.

        If the message is the last one needed but the contents are somehow different than the
        original file on disk, this method calls :func:`~_mismatched_hash_callback`,
        registers the file as 'mismatched_hash' in the .csv file, and returns.

        :param lock: Acquiring this :class:`threading.Lock` object ensures that only one instance
            of :func:`~_process_message` is running at once
        :type lock: :class:`threading.Lock`
        :param msg: The received :class:`confluent_kafka.KafkaMessage` object,
            or an undecrypted KafkaCrypto message
        :type msg: :class:`confluent_kafka.KafkaMessage` or :class:`kafkacrypto.Message`
        :param rootdir_to_set: Path to a directory that should be set as the "root"
            for reconstructed data files (default is the output directory)
        :type rootdir_to_set: :class:`pathlib.Path`

        :return: True if processing the message was successful (file in progress or
            message enqueued to be produced), False otherwise
        :rtype: bool
        """
        retval = super()._process_message(
            lock, msg, self._output_dir if rootdir_to_set is None else rootdir_to_set
        )
        # if the file was in progress or had a mismatched hash, return True or False, respectively
        if retval in (True, False):
            return retval
        # get the DataFileChunk from the message value
        try:
            dfc = msg.value()  # from a regular Kafka Consumer
        except TypeError:
            dfc = msg.value  # from KafkaCrypto
        # if the file has had all of its messages read successfully,
        # try to enqueue its processing result to produce
        if retval == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE:
            with lock:
                self.recent_processed_filepaths.append(dfc.relative_filepath)
                while len(self.recent_processed_filepaths) > self.N_RECENT_FILES:
                    _ = self.recent_processed_filepaths.pop(0)
                self.n_processed_files += 1
            self.logger.debug(
                f"Getting message to produce for {dfc.relative_filepath}..."
            )
            new_msg = self._get_processing_result_message_for_file(
                self.files_in_progress_by_path[dfc.relative_filepath], lock
            )
            if new_msg is not None:
                self.producer_message_queue.put(new_msg)
                return True
            self._failed_computing_processing_result(
                self.files_in_progress_by_path[dfc.relative_filepath], self.lock
            )
            return False
        # otherwise the file is just in progress
        return True

    @abstractmethod
    def _get_processing_result_message_for_file(self, datafile, lock):
        """
        Given a relative :class:`~.data_file_io.DownloadDataFileToMemory`, compute and return a
        :class:`~.data_file_io.ReproducerMessage` object that should be produced as the processing
        result for the file.

        This function should log an error and return None if the processing result
        fails to be computed.

        Not implemented in the base class.

        :param datafile: A :class:`~.data_file_io.DownloadDataFileToMemory` object that has received
            all of its messages from the topic
        :type datafile: :class:`~.data_file_io.DownloadDataFileToMemory`
        :param lock: Acquiring this :class:`threading.Lock` object would ensure that only one
            instance of :func:`~_get_processing_result_message_for_file` is running at once
        :type lock: :class:`threading.Lock`

        :return: message object to be produced
            (or None if computing it failed for any reason)
        :rtype: :class:`~.kafka_wrapper.Producible`
        """
        raise NotImplementedError

    def _failed_computing_processing_result(self, datafile, lock):
        """
        This function is called when :func:`_get_processing_result_message_for_file` returns None
        because a processing result message could not be computed. It registers the original data
        file read from the topic as 'failed' in the .csv file so that that file will have its
        messages re-consumed if the program is restarted reading from the same topic with the
        same Consumer group ID. It then logs a warning and stops tracking the file.

        :param datafile: The datafile that should have been used to compute a
            processing result message
        :type datafile: :class:`~.data_file_io.DownloadDataFileToMemory`
        :param lock: Acquiring this :class:`threading.Lock` object would ensure that only
            one instance of :func:`~_get_processing_result_message_for_file` is running at once
        :type lock: :class:`threading.Lock`
        """
        # register the file in the registry as computing result failed
        with lock:
            self.file_registry.register_file_computing_result_failed(
                datafile.filename,
                datafile.filepath.relative_to(self._output_dir),
                datafile.n_total_chunks,
            )
        # log the warning
        warnmsg = (
            f"WARNING: Failed to compute the message to produce from {datafile.filepath}. "
            "Messages for this file will need to be re-consumed to try again. "
            "Check logs above for specific error messages."
        )
        self.logger.warning(warnmsg)
        # stop tracking the file
        with self.lock:
            del self.files_in_progress_by_path[datafile.relative_filepath]
            del self.locks_by_fp[datafile.relative_filepath]

    def _on_check(self):
        msg = (
            f"{self.n_msgs_read} messages read, {self.n_msgs_processed} messages processed, "
            f"{self.n_processed_files} files completely reconstructed, and "
            f"{self.n_results_produced_files} processing result messages produced so far"
        )
        self.logger.info(msg)
        if (
            len(self.files_in_progress_by_path) > 0
            or len(self.recent_processed_filepaths) > 0
            or len(self.recent_results_produced) > 0
        ):
            self.logger.debug(self.progress_msg)

    def _on_shutdown(self):
        super()._on_shutdown()
        self.file_registry.consolidate_succeeded_files()
