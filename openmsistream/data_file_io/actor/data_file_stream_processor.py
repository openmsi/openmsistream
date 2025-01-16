"""A DataFileStreamHandler that triggers some arbitrary local code when full files are available"""

# imports
from abc import ABC, abstractmethod
from ...utilities.config import RUN_CONST
from ..config import DATA_FILE_HANDLING_CONST
from .data_file_chunk_handlers import DataFileChunkProcessor
from .data_file_stream_handler import DataFileStreamHandler
from .file_registry.stream_handler_registries import StreamProcessorRegistry


class DataFileStreamProcessor(DataFileStreamHandler, DataFileChunkProcessor, ABC):
    """
    Consumes :class:`~.data_file_io.entity.data_file_chunk.DataFileChunk` messages
    into memory and perform some operation(s) when entire files are available.
    This is a base class that cannot be instantiated on its own.

    :param config_file: Path to the config file to use in defining the Broker connection
        and Consumers
    :type config_file: :class:`pathlib.Path`
    :param topic_name: Name of the topic to which the Consumers should be subscribed
    :type topic_name: str
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
    :param n_threads: the number of threads/consumers to run
    :type n_threads: int, optional
    :param consumer_group_id: the group ID under which each consumer should be created
    :type consumer_group_id: str, optional
    :param filepath_regex: If given, only messages associated with files whose paths match
        this regex will be consumed
    :type filepath_regex: :type filepath_regex: :func:`re.compile` or None, optional

    :raises ValueError: if `datafile_type` is not a subclass of
        :class:`~.data_file_io.DownloadDataFileToMemory`, or more specific as determined
        by the "mode" argument
    """

    def __init__(self, config_file, topic_name, **kwargs):
        """
        Constructor method (duplicated here for its function signature in the docs)
        """
        super().__init__(config_file, topic_name, **kwargs)

    def process_files_as_read(self):
        """
        Consumes messages and stores their data in memory.
        Uses several parallel threads to consume message and calls
        :func:`~_process_downloaded_data_file` for fully read files.
        Runs until the user inputs a command to shut it down.

        :return: the total number of messages consumed
        :rtype: int
        :return: the total number of messages processed (registered in memory)
        :rtype: int
        :return: the number of files successfully processed during the run
        :rtype: int
        :return: the paths of up to 50 most recent files successfully processed during the run
        :rtype: List
        """
        # startup message
        msg = (
            f"Will process files from messages in the {self.consumer_topic_name} topic using "
            f'{self.n_threads} thread{"s" if self.n_threads>1 else ""}'
        )
        self.logger.info(msg)
        # set up the stream processor registry
        self.file_registry = StreamProcessorRegistry(
            dirpath=self._logs_subdir,
            topic_name=self.consumer_topic_name,
            consumer_group_id=self.consumer_group_id,
            logger=self.logger,
        )
        # if there are files that need to be re-processed,
        # set the variables to re-read messages from those files
        if self.file_registry.rerun_file_key_regex is not None:
            msg = (
                f'Consumer{"s" if self.n_threads>1 else ""} will start from the beginning '
                f"of the topic to re-read messages for {self.file_registry.n_files_to_rerun} "
                f'previously-failed file{"s" if self.file_registry.n_files_to_rerun>1 else ""}'
            )
            self.logger.info(msg)
            self.restart_at_beginning = True
            self.message_key_regex = self.file_registry.rerun_file_key_regex
        # call the run loop
        self.run()
        # return the results of the processing
        return (
            self.n_msgs_read,
            self.n_msgs_processed,
            self.n_processed_files,
            self.recent_processed_filepaths,
        )

    def _process_message(self, lock, msg, rootdir_to_set=None):
        """
        Process a single message to add it to a file being held in memory until all messages
        are received.

        If the message failed to be decrypted, this method calls
        :func:`~_undecryptable_message_callback` and returns.

        If the message is the first one consumed for a particular file, or any message other
        than the last one needed, it registers the file as 'in_progress' in the .csv file.

        If the message is the last message needed for a file and its contents match the
        original hash of the file on disk, this method calls
        :func:`~_process_downloaded_data_file`.

        If the call to :func:`~_process_downloaded_data_file` returns None (success),
        this method moves the file to the 'successfully processed' .csv file, and returns.

        If the call to :func:`~_process_downloaded_data_file` returns an Exception,
        this method calls :func:`~_failed_processing_callback`, registers the file as
        'failed' in the .csv file, and returns.

        If the message is the last one needed but the contents are somehow different than
        the original file on disk, this method calls :func:`~_mismatched_hash_callback`,
        registers the file as 'mismatched_hash' in the .csv file, and returns.

        :param lock: Acquiring this :class:`threading.Lock` object ensures that only one
            instance of :func:`~_process_message` is running at once
        :type lock: :class:`threading.Lock`
        :param msg: The received :class:`confluent_kafka.KafkaMessage` object,
            or an undecrypted KafkaCrypto message
        :type msg: :class:`confluent_kafka.KafkaMessage` or :class:`kafkacrypto.Message`
        :param rootdir_to_set: Path to a directory that should be set as the "root"
            for reconstructed data files (default is the output directory)
        :type rootdir_to_set: :class:`pathlib.Path`

        :return: True if processing the message was successful
            (file in progress or successfully processed), False otherwise
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
        # send it to the processing function
        if retval == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE:
            self.logger.debug(f"Processing {dfc.relative_filepath}...")
            processing_retval = self._process_downloaded_data_file(
                self.files_in_progress_by_path[dfc.relative_filepath], lock
            )
            to_return = None
            # if it was able to be processed
            if processing_retval is None:
                self.logger.debug(
                    f"Fully-read file {dfc.relative_filepath} successfully processed"
                )
                self.file_registry.register_file_successfully_processed(dfc)
                with lock:
                    self.recent_processed_filepaths.append(dfc.relative_filepath)
                    while len(self.recent_processed_filepaths) > self.N_RECENT_FILES:
                        self.recent_processed_filepaths.pop(0)
                    self.n_processed_files += 1
                    _ = self.files_in_progress_by_path.pop(dfc.relative_filepath)
                    _ = self.locks_by_fp.pop(dfc.relative_filepath)
                to_return = True
            # warn if it wasn't processed correctly and invoke the callback
            else:
                if isinstance(processing_retval, Exception):
                    warnmsg = (
                        f"WARNING: Fully-read file {dfc.relative_filepath} was not able "
                        "to be processed. The traceback of the Exception thrown during "
                        "processing will be logged below, but not re-raised. "
                        "The messages for this file will need to be consumed again if "
                        "the file is to be processed! Please rerun with the same "
                        "consumer ID to try again."
                    )
                    self.logger.warning(warnmsg, exc_info=processing_retval)
                else:
                    warnmsg = (
                        "Unrecognized return value from _process_downloaded_data_file: "
                        f"{processing_retval}"
                    )
                    self.logger.warning(warnmsg)
                self.file_registry.register_file_processing_failed(dfc)
                self._failed_processing_callback(
                    self.files_in_progress_by_path[dfc.relative_filepath], lock
                )
                with lock:
                    _ = self.files_in_progress_by_path.pop(dfc.relative_filepath)
                    _ = self.locks_by_fp.pop(dfc.relative_filepath)
                to_return = False
            return to_return
        # otherwise the file is just in progress
        return True

    @abstractmethod
    def _process_downloaded_data_file(self, datafile, lock):
        """
        Perform some arbitrary operation(s) on a given data file that has been fully read
        from the stream. Can optionally lock other threads using the given lock.

        Not implemented in the base class.

        :param datafile: A :class:`~.data_file_io.DownloadDataFileToMemory` object that
            has received all of its messages from the topic
        :type datafile: :class:`~.data_file_io.DownloadDataFileToMemory`
        :param lock: Acquiring this :class:`threading.Lock` object would ensure that
            only one instance of :func:`~_process_downloaded_data_file` is running at once
        :type lock: :class:`threading.Lock`

        :return: None if processing was successful, an Exception otherwise
        """
        raise NotImplementedError

    def _failed_processing_callback(self, datafile, lock):
        """
        Called when :func:`~_process_downloaded_data_file` returns an Exception,
        providing an opportunity for fallback/backup processing in the case of failure.

        Does nothing in the base class.

        :param datafile: A :class:`~.data_file_io.DownloadDataFileToMemory` object
            that has received all of its messages from the topic
        :type datafile: :class:`~.data_file_io.DownloadDataFileToMemory`
        :param lock: Acquiring this :class:`threading.Lock` object would ensure that
            only one instance of :func:`~_failed_processing_callback` is running at once
        :type lock: :class:`threading.Lock`
        """

    def _on_check(self):
        msg = (
            f"{self.n_msgs_read} messages read, {self.n_msgs_processed} messages "
            f"processed, {self.n_processed_files} files completely processed so far"
        )
        self.logger.info(msg)
        if (
            len(self.files_in_progress_by_path) > 0
            or len(self.recent_processed_filepaths) > 0
        ):
            self.logger.debug(self.progress_msg)

    def _on_shutdown(self):
        super()._on_shutdown()
        self.file_registry.consolidate_succeeded_files()

    @classmethod
    def get_command_line_arguments(cls):
        superargs, superkwargs = super().get_command_line_arguments()
        kwargs = {**superkwargs, "n_threads": RUN_CONST.N_DEFAULT_DOWNLOAD_THREADS}
        return superargs, kwargs
