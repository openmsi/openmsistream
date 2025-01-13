"""
A directory holding files that may or may not be marked for uploading.
Updates when new files are added.
"""

# imports
import datetime, time
from threading import Lock
from queue import Queue
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from confluent_kafka.serialization import StringSerializer
from openmsitoolbox import Runnable
from openmsitoolbox.utilities.misc import populated_kwargs
from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread
from ...utilities.controlled_processes_heartbeats_logs import (
    ControlledProcessSingleThreadHeartbeatsLogs,
)
from ...utilities import OpenMSIStreamArgumentParser
from ...kafka_wrapper import ConsumerAndProducerGroup
from ...utilities.config import RUN_CONST
from ...utilities.heartbeat_producibles import UploadDirectoryHeartbeatProducible
from .. import DataFileDirectory
from ..entity.upload_data_file import UploadDataFile
from ..entity.upload_directory_event_handler import UploadDirectoryEventHandler
from .file_registry.producer_file_registry import ProducerFileRegistry


class DataFileUploadDirectory(
    DataFileDirectory,
    ControlledProcessSingleThreadHeartbeatsLogs,
    ConsumerAndProducerGroup,
    Runnable,
):
    """
    Class representing a directory being watched for new files to be added.
    Files added to the directory are broken into chunks and uploaded as messages to a Kafka topic.

    :param dirpath: Path to the directory that should be watched for new files
    :type dirpath: :class:`pathlib.Path`
    :param config_path: Path to the config file to use in defining the Broker connection
        and Producers
    :type config_path: :class:`pathlib.Path`
    :param upload_regex: only files matching this regular expression will be uploaded
    :type upload_regex: :func:`re.compile`, optional
    :param datafile_type: the type of data file that recognized files should be uploaded as
        (must be a subclass of :class:`~UploadDataFile`)
    :type datafile_type: :class:`~UploadDataFile`, optional
    :param watchdog_lag_time: Number of seconds that files must remain static (unmodified)
        for their uploads to begin
    :type watchdog_lag_time: int

    :raises ValueError: if `datafile_type` is not a subclass of :class:`~UploadDataFile`
    """

    #################### CONSTANTS ####################

    ARGUMENT_PARSER_TYPE = OpenMSIStreamArgumentParser
    # starting point for how long to wait between pinging the directory looking for new files
    MIN_WAIT_TIME = 0.005
    # never wait more than a minute to check again for new files
    MAX_WAIT_TIME = 60
    # amount of time to wait for watchdog to pull events from the queue
    WATCHDOG_OBSERVER_TIMEOUT = 0.1
    # the max number of files to report status messages for
    N_STATUS_MESSAGE_FILES = 50
    # name of the logging subdirectory
    LOG_SUBDIR_NAME = "LOGS"

    #################### PUBLIC FUNCTIONS ####################

    def __init__(
        self,
        dirpath,
        config_path,
        upload_regex=RUN_CONST.DEFAULT_UPLOAD_REGEX,
        datafile_type=UploadDataFile,
        watchdog_lag_time=RUN_CONST.DEFAULT_WATCHDOG_LAG_TIME,
        use_polling_observer=False,
        **kwargs,
    ):
        """
        Constructor method
        """
        # create a subdirectory for the logs
        self._logs_subdir = dirpath / self.LOG_SUBDIR_NAME
        if "logger_file" in kwargs and kwargs["logger_file"] is not None:
            logger_file_arg = kwargs["logger_file"].resolve()
            if "." in logger_file_arg.name:
                self._logs_subdir = logger_file_arg.parent
            else:
                self._logs_subdir = logger_file_arg
            kwargs["logger_file"] = self._logs_subdir
        if not self._logs_subdir.is_dir():
            self._logs_subdir.mkdir(parents=True)
        # put the log file in the subdirectory
        kwargs = populated_kwargs(kwargs, {"logger_file": self._logs_subdir})
        super().__init__(dirpath, config_path, **kwargs)
        if not issubclass(datafile_type, UploadDataFile):
            errmsg = (
                "ERROR: DataFileUploadDirectory requires a datafile_type that is a "
                f"subclass of UploadDataFile but {datafile_type} was given!"
            )
            self.logger.error(errmsg, exc_type=ValueError)
        self.__datafile_type = datafile_type
        self.__wait_time = self.MIN_WAIT_TIME
        self.__lock = Lock()
        if use_polling_observer:
            self.logger.debug(
                "Using a PollingObserver instead of the default watchdog Observer"
            )
            observer_class = PollingObserver
        else:
            observer_class = Observer
        self.__observer = observer_class(timeout=self.WATCHDOG_OBSERVER_TIMEOUT)
        self.__event_handler = UploadDirectoryEventHandler(
            upload_regex=upload_regex,
            logs_subdir=self._logs_subdir,
            lag_time=watchdog_lag_time,
            logger=self.logger,
        )
        self.__active_files_by_path = {}
        self.__status_message_files = {}
        self.__topic_name = None
        self.__chunk_size = None
        self.__file_registry = None
        self.__upload_queue = None
        self.__producers = []
        self.__upload_threads = []
        self.__n_messages_produced_since_heartbeat = 0
        self.__n_bytes_produced_since_heartbeat = 0
        # update heartbeat/log producers
        prod = super().get_new_producer(
            key_serializer_override=StringSerializer(),
            value_serializer_override=StringSerializer(),
        )
        # cannot just add to producers list since we need to close it after everything else
        self.__heartbeat_log_producer = prod
        self.set_heartbeat_producer(prod, close_it=True)
        self.set_log_producer(prod, close_it=True)

    def upload_files_as_added(
        self,
        topic_name,
        n_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS,
        chunk_size=RUN_CONST.DEFAULT_CHUNK_SIZE,
        max_queue_size=RUN_CONST.DEFAULT_MAX_UPLOAD_QUEUE_MEGABYTES,
        upload_existing=False,
    ):
        """
        Watch for new files to be added to the directory.
        Chunk and produce newly added files as messages to a Kafka topic.

        :param topic_name: Name of the topic to produce messages to
        :type topic_name: str
        :param n_threads: The number of threads to use to produce from the shared queue
        :type n_threads: int, optional
        :param chunk_size: The size of each file chunk in bytes
        :type chunk_size: int, optional
        :param max_queue_size: The maximum size in MB of the internal upload queue
            (this is distinct from the librdkafka buffering queue)
        :type max_queue_size: int, optional
        :param upload_existing: True if any files that already exist in the directory
            should be uploaded. If False (the default) then only files added to the
            directory after startup will be enqueued to the producer
        :type upload_existing: bool, optional
        """
        # set the topic name and chunk size
        self.__topic_name = topic_name
        self.__chunk_size = chunk_size
        msg = (
            f'Will upload {"files in" if upload_existing else "new files added to"}'
            f"{self.dirpath} to the {self.__topic_name} topic as "
            f"{self.__chunk_size}-byte chunks using {n_threads} threads"
        )
        self.logger.info(msg)
        # start up a file registry in the watched directory
        self.__file_registry = ProducerFileRegistry(
            dirpath=self._logs_subdir, topic_name=topic_name, logger=self.logger
        )
        # start the upload queue
        n_max_queue_items = int((1000000 * max_queue_size) / self.__chunk_size)
        self.__upload_queue = Queue(n_max_queue_items)
        # start the producers and upload threads
        for _ in range(n_threads):
            self.__producers.append(self.get_new_producer())
            thread = ExceptionTrackingThread(
                target=self.__producers[-1].produce_from_queue_looped,
                args=(self.__upload_queue, self.__topic_name),
                kwargs={"callback": self.producer_callback},
            )
            thread.start()
            self.__upload_threads.append(thread)
        # if we're going to upload files that are already in the directory, scrape it now
        if upload_existing:
            self.__scrape_dir_for_files()
        # add or modify datafiles to upload based on the contents of the file registry
        self.__startup_from_file_registry()
        # schedule and start the observer
        self.__observer.schedule(self.__event_handler, self.dirpath, recursive=True)
        self.__observer.start()
        # loop until the user inputs a command to stop
        self.run()
        # return a list of relative filepaths that have been uploaded
        to_return = []
        for rel_filepath in self.__file_registry.get_completed_filepaths():
            to_return.append(rel_filepath)
        return to_return

    def producer_callback(
        self, err, msg, prodid, filename, filepath, n_total_chunks, chunk_i
    ):
        """
        A reference to this method is given as the callback for each call to
        :func:`confluent_kafka.Producer.produce`. It is called for every message
        upon acknowledgement by the broker, and it uses the file registries in the LOGS
        subdirectory to keep the information about what has and hasn't been uploaded
        current with what has been received by the broker.

        If the message is the final one to be acknowledged from its corresponding
        :class:`~UploadDataFile`, the :class:`~UploadDataFile` is registered as "fully produced".

        :param err: The error object for the message
        :type err: :class:`confluent_kafka.KafkaError`
        :param msg: The message object
        :type msg: :class:`confluent_kafka.Message`
        :param prodid: The ID of the producer that produced the message
            (hex(id(producer)) in memory)
        :type prodid: str
        :param filename: The name of the file the message is coming from
        :type filename: str
        :param filepath: The full path to the file the message is coming from
        :type filepath: :class:`pathlib.Path`
        :param n_total_chunks: The total number of chunks in the file this message came from
        :type n_total_chunks: int
        :param chunk_i: The index of the message's file chunk in the full list for the file
        :type chunk_i: int
        """
        # If any error occured, re-enqueue the message's file chunk
        if err is None and msg.error() is not None:
            err = msg.error()
        if err is not None:
            warnmsg = (
                f'WARNING: Producer with ID {prodid} {"fatally" if err.fatal() else ""} '
                f"failed to deliver message for chunk {chunk_i} of {filepath}"
                f'{" and cannot retry" if not err.retriable() else ""}. '
                f"This message will be re-enqeued. Error reason: {err.str()}"
            )
            self.logger.warning(warnmsg)
            with self.__lock:
                self.__add_chunks_for_filepath(filepath, [chunk_i])
        # Otherwise, register the chunk as successfully sent to the broker
        else:
            rel_filepath = filepath.relative_to(self.dirpath)
            fully_produced = self.__file_registry.register_chunk(
                filename, rel_filepath, n_total_chunks, chunk_i, prodid
            )
            with self.__lock:
                self.__n_messages_produced_since_heartbeat += 1
                self.__n_bytes_produced_since_heartbeat += len(msg)
            # If the file has now been fully produced to the topic,
            # set the variable for the file and log a line
            if fully_produced:
                with self.__lock:
                    self.__active_files_by_path[filepath].fully_enqueued = False
                    self.__active_files_by_path[filepath].fully_produced = True
                    self.__forget_inactive_files()
                debugmsg = (
                    f'{rel_filepath} has been fully produced to the "{self.__topic_name}"'
                    f' topic as {n_total_chunks} message{"s" if n_total_chunks!=1 else ""}'
                )
                self.logger.debug(debugmsg)

    def filepath_should_be_uploaded(self, filepath):
        """
        Returns True if a given filepath should be uploaded (if it matches the upload regex)

        :param filepath: A candidate upload filepath
        :type filepath: :class:`pathlib.Path`

        :return: True if the filepath is an existing file outside of the LOGS directory
            whose path matches the upload regex, False otherwise
        :rtype: bool

        :raises TypeError: if `filepath` isn't a :class:`pathlib.Path` object
        """
        return self.__event_handler.filepath_matched(filepath)

    def get_heartbeat_message(self):
        new_msg = UploadDirectoryHeartbeatProducible(
            self._heartbeat_program_id,
            self.__n_messages_produced_since_heartbeat,
            self.__n_bytes_produced_since_heartbeat,
        )
        with self.__lock:
            self.__n_messages_produced_since_heartbeat = 0
            self.__n_bytes_produced_since_heartbeat = 0
        return new_msg

    # no override of get_log_message needed

    #################### PRIVATE HELPER FUNCTIONS ####################

    def _run_iteration(self):
        """
        If there are any files currently uploading, finish uploading them.
        If not, consult the watchdog event handler to get new active files.
        On iterations where nothing is being actively uploaded, poll the producers
        and increase the timeout for getting event handler updates exponentially
        if nothing is happening.
        """
        # pull any new files from the event handler and poll the producers
        if not self.have_file_to_upload:
            # wait here so that this loop isn't constantly consuming CPU if nothing
            # is happening
            time.sleep(self.__wait_time)
            self.__pull_from_handler()
            n_new_callbacks = 0
            for producer in self.__producers:
                new_callbacks = producer.poll(0)
                if new_callbacks is not None:
                    n_new_callbacks += new_callbacks
            # Poll but do not count heartbeat/logs
            self.__heartbeat_log_producer.poll(0)
            if (not self.have_file_to_upload) and (n_new_callbacks == 0):
                if self.__wait_time < self.MAX_WAIT_TIME:
                    self.__wait_time *= 1.05
            else:
                self.__wait_time = self.MIN_WAIT_TIME
            return
        # find the first file that's running and add some of its chunks to the upload queue
        for datafile in self.__active_files_by_path.values():
            if datafile.upload_in_progress or datafile.waiting_to_upload:
                datafile.enqueue_chunks_for_upload(
                    self.__upload_queue,
                    n_threads=len(self.__upload_threads),
                    chunk_size=self.__chunk_size,
                )
                break
        # restart any crashed threads
        self.__restart_crashed_threads()

    def _on_check(self):
        # poll the producers
        for producer in self.__producers:
            producer.poll(0)
        self.__heartbeat_log_producer.poll(0)
        # forget inactive files
        with self.__lock:
            self.__forget_inactive_files()
        # log progress so far
        self.logger.info(self.status_msg)
        self.logger.debug(self.progress_msg)
        # reset the wait time
        self.__wait_time = self.MIN_WAIT_TIME

    def _on_shutdown(self):
        self.logger.info(
            "Will quit after all currently enqueued files are done being transferred."
        )
        self.logger.debug(self.progress_msg)
        # shut down the watchdog observer
        self.__observer.stop()
        self.__observer.join()
        # add the remainder of any files currently in progress
        if self.n_partially_done_files > 0:
            msg = "Will finish queueing the following files:\n\t" "\n\t".join(
                [str(_) for _ in self.partially_done_file_paths]
            )
            self.logger.info(msg)
        while self.n_partially_done_files > 0:
            for datafile in self.__active_files_by_path.values():
                if datafile.upload_in_progress:
                    datafile.enqueue_chunks_for_upload(
                        self.__upload_queue,
                        n_threads=len(self.__upload_threads),
                        chunk_size=self.__chunk_size,
                    )
                    break
        # stop the uploading threads by adding "None"s to their queues and joining them
        for thread in self.__upload_threads:
            self.__upload_queue.put(None)
        for thread in self.__upload_threads:
            thread.join()
        self.logger.info(
            "Waiting for all enqueued messages to be delivered (this may take a moment)"
        )
        for producer in self.__producers:
            # don't move on until all enqueued messages have been sent/received
            producer.flush(timeout=-1)
            producer.close()
        self.close()
        super()._on_shutdown()
        self.__file_registry.consolidate_completed_files()

    def __add_active_datafile_for_path(self, filepath):
        """
        Create a new datafile with the given path and add it to the dictionaries of
        active datafiles and datafiles to report for status messages

        filepath = the path to the file that should be added
        """
        new_datafile = self.__datafile_type(
            filepath,
            to_upload=True,
            rootdir=self.dirpath,
            logger=self.logger,
            **self.other_datafile_kwargs,
        )
        self.__active_files_by_path[filepath] = new_datafile
        n_keys_to_remove = (
            len(self.__status_message_files) + 1 - self.N_STATUS_MESSAGE_FILES
        )
        if n_keys_to_remove > 0:
            keys_to_remove = list(self.__status_message_files.keys())[:n_keys_to_remove]
            for key in keys_to_remove:
                _ = self.__status_message_files.pop(key)
        self.__status_message_files[filepath] = new_datafile

    def __scrape_dir_for_files(self, retries_left=10):
        """
        Search the directory for any unrecognized files and add them as active datafiles

        retries_left = how many attempts should be made to retry if an error is encountered
        """
        # This is in a try/except in case a file is moved or a subdirectory is renamed
        # while this method is running: it'll retry up to ten times
        try:
            for filepath in self.dirpath.rglob("*"):
                filepath = filepath.resolve()
                if (
                    filepath not in self.__active_files_by_path
                ) and self.filepath_should_be_uploaded(filepath):
                    # wait until the file is actually available
                    file_ready = False
                    total_time_waited = 0.0
                    while (not file_ready) and total_time_waited < 10.0:
                        try:
                            with open(filepath) as _:
                                file_ready = True
                        except PermissionError:
                            time.sleep(0.05)
                            total_time_waited += 0.05
                    if not file_ready:
                        warnmsg = (
                            f"WARNING: failed to open {filepath} for more than 10 secs "
                            "due to permission errors, skipping it for now"
                        )
                        self.logger.warning(warnmsg)
                        continue
                    self.__add_active_datafile_for_path(filepath)
        except FileNotFoundError as exc:
            if retries_left > 0:
                self.__scrape_dir_for_files(retries_left=retries_left - 1)
            warnmsg = (
                f"WARNING: exhausted retries finding new files in {self.dirpath}. "
                "Check log messages below for the error encountered."
            )
            self.logger.warning(warnmsg, exc_info=exc)

    def __pull_from_handler(self):
        """
        Call the watchdog EventHandler to get paths to files that should be uploaded
        """
        to_kick_back = set()
        # get all the new files for which events have occurred
        for handler_file in self.__event_handler.get_new_files():
            new_filepath = handler_file.filepath
            # if the file isn't active yet, add it and move on
            if new_filepath not in self.__active_files_by_path:
                with self.__lock:
                    self.__add_active_datafile_for_path(new_filepath)
                continue
            # otherwise, if the file is already active in the directory
            updated_at = handler_file.last_updated
            extant_datafile = self.__active_files_by_path[new_filepath]
            # if it wasn't going to be uploaded, set it to upload
            if not extant_datafile.to_upload:
                extant_datafile.to_upload = True
            # if it's been fully produced, remove and replace it with the new version
            elif extant_datafile.fully_produced:
                with self.__lock:
                    self.__forget_inactive_files()
                    self.__add_active_datafile_for_path(new_filepath)
            # if it's been fully enqueued, or is being uploaded, send it back
            elif extant_datafile.fully_enqueued or extant_datafile.upload_in_progress:
                to_kick_back.add(handler_file)
            # if it's been waiting to upload
            elif extant_datafile.waiting_to_upload:
                # if it has been chunked before its updated time, send it back
                # otherwise nothing needs to happen
                if (
                    extant_datafile.chunked_at_timestamp
                    and extant_datafile.chunked_at_timestamp < updated_at
                ):
                    to_kick_back.add(handler_file)
            else:
                warnmsg = (
                    "WARNING: unknown status for active upload file! Will reset file to "
                    f"produce again (status message = {extant_datafile.upload_status_message})"
                )
                self.logger.warning(warnmsg)
                with self.__lock:
                    self.__forget_inactive_files()
                    self.__add_active_datafile_for_path(new_filepath)
        # send back any files that will need to be handled later
        for handler_file in to_kick_back:
            self.__event_handler.kick_back(handler_file)

    def __forget_inactive_files(self):
        """
        Read through the dictionary of active files and remove any
        that won't be uploaded or that have been fully produced
        """
        keys_to_remove = []
        for key, datafile in self.__active_files_by_path.items():
            if (not datafile.to_upload) or datafile.fully_produced:
                keys_to_remove.append(key)
        for key in keys_to_remove:
            _ = self.__active_files_by_path.pop(key)

    def __startup_from_file_registry(self):
        """
        Look through the file registry to find and prepare to enqueue any files
        that were in progress the last time the code was shut down and ignore
        any files that have previously been fully uploaded
        """
        # make sure any files listed as "in progress" have their remaining chunks enqueued
        n_files_to_resume = 0
        n_chunks_resumed = 0
        for (
            rel_filepath,
            chunks,
        ) in self.__file_registry.get_incomplete_filepaths_and_chunks():
            debugmsg = (
                f"Found {rel_filepath} in progress from a previous run. "
                f"Re-enqueuing {len(chunks)} chunks."
            )
            self.logger.debug(debugmsg)
            self.__add_chunks_for_filepath(self.dirpath / rel_filepath, chunks)
            n_files_to_resume += 1
            n_chunks_resumed += len(chunks)
        if n_files_to_resume > 0:
            infomsg = (
                f'Found {n_files_to_resume} file{"s" if n_files_to_resume!=1 else ""}'
                f" in progress from a previous run. Will re-enqueue {n_chunks_resumed} "
                f'total chunk{"s" if n_chunks_resumed!=1 else ""}'
            )
            self.logger.info(infomsg)
        # make sure any files listed as "completed" will not be uploaded again
        n_prev_uploaded = 0
        for rel_filepath in self.__file_registry.get_completed_filepaths():
            filepath = self.dirpath / rel_filepath
            debugmsg = (
                f"Found {filepath} listed as fully uploaded during a previous run. "
                "Will not produce it again."
            )
            if filepath in self.__active_files_by_path:
                if self.__active_files_by_path[filepath].to_upload:
                    self.logger.debug(debugmsg)
                self.__active_files_by_path[filepath].to_upload = False
            elif filepath.is_file():
                self.logger.debug(debugmsg)
            n_prev_uploaded += 1
        if n_prev_uploaded > 0:
            infomsg = (
                f'Found {n_prev_uploaded} file{"s" if n_prev_uploaded!=1 else ""} '
                "fully uploaded during a previous run that will not be produced again."
            )
            self.logger.info(infomsg)
        # forget any files that are now set to inactive
        self.__forget_inactive_files()

    def __add_chunks_for_filepath(self, filepath, chunks):
        """
        Add the given chunks to the list of chunks to upload for a given file
        Creates the file if it doesn't already exist in the dictionary of active files
        """
        if filepath in self.__active_files_by_path:
            self.__active_files_by_path[filepath].to_upload = True
        else:
            self.__add_active_datafile_for_path(filepath)
        self.__active_files_by_path[filepath].add_chunks_to_upload(
            chunks,
            chunk_size=self.__chunk_size,
        )

    def __restart_crashed_threads(self):
        """
        Iterate over the upload threads and restart any that crashed,
        logging the exception they threw
        """
        for ti, upload_thread in enumerate(self.__upload_threads):
            if upload_thread.caught_exception is not None:
                # log the error
                warnmsg = (
                    "WARNING: an upload thread raised an Exception, which will be logged "
                    "as an error below but not re-raised. The Producer and upload thread "
                    "that raised the error will be restarted."
                )
                self.logger.warning(warnmsg, exc_info=upload_thread.caught_exception)
                # try to join the thread
                try:
                    upload_thread.join()
                except Exception:
                    pass
                finally:
                    self.__upload_threads[ti] = None
                    self.__producers[ti] = None
                # recreate the producer and restart the thread
                self.__producers[ti] = self.get_new_producer()
                thread = ExceptionTrackingThread(
                    target=self.__producers[ti].produce_from_queue_looped,
                    args=(self.__upload_queue, self.__topic_name),
                    kwargs={"callback": self.producer_callback},
                )
                thread.start()
                self.__upload_threads[ti] = thread

    #################### CLASS METHODS ####################

    @classmethod
    def get_command_line_arguments(cls):
        superargs, superkwargs = super().get_command_line_arguments()
        args = [
            *superargs,
            "upload_dir",
            "topic_name",
            "upload_regex",
            "chunk_size",
            "queue_max_size",
            "upload_existing",
            "watchdog_lag_time",
            "use_polling_observer",
        ]
        kwargs = {**superkwargs, "n_threads": RUN_CONST.N_DEFAULT_UPLOAD_THREADS}
        return args, kwargs

    @classmethod
    def get_init_args_kwargs(cls, parsed_args):
        superargs, superkwargs = super().get_init_args_kwargs(parsed_args)
        args = [
            parsed_args.upload_dir,
            *superargs,
        ]
        kwargs = {
            **superkwargs,
            "upload_regex": parsed_args.upload_regex,
            "watchdog_lag_time": parsed_args.watchdog_lag_time,
            "use_polling_observer": parsed_args.use_polling_observer,
        }
        return args, kwargs

    @classmethod
    def run_from_command_line(cls, args=None):
        """
        Run a :class:`~DataFileUploadDirectory` directly from the command line

        Calls :func:`~upload_files_as_added` on a :class:`~DataFileUploadDirectory`
        defined by command line (or given) arguments

        :param args: the list of arguments to send to the parser
        :type args: List
        """
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        # make the DataFileDirectory for the specified directory
        init_args, init_kwargs = cls.get_init_args_kwargs(args)
        upload_file_directory = cls(*init_args, **init_kwargs)
        # listen for new files in the directory and run uploads until shut down
        run_start = datetime.datetime.now()
        if not args.upload_existing:
            upload_file_directory.logger.info(
                f"Listening for files to be added to {args.upload_dir}..."
            )
        else:
            upload_file_directory.logger.info(
                f"Uploading files in/added to {args.upload_dir}..."
            )
        uploaded_filepaths = upload_file_directory.upload_files_as_added(
            args.producer_topic_name,
            n_threads=args.n_threads,
            chunk_size=args.chunk_size,
            max_queue_size=args.queue_max_size,
            upload_existing=args.upload_existing,
        )
        run_stop = datetime.datetime.now()
        upload_file_directory.logger.info(
            f"Done listening to {args.upload_dir} for files to upload"
        )
        if len(uploaded_filepaths) > 0:
            final_msg = (
                f"The following {len(uploaded_filepaths)} file"
                f'{" was" if len(uploaded_filepaths)==1 else "s were"}'
                f" uploaded between {run_start} and {run_stop}:\n\t"
            )
            final_msg += "\n\t".join([str(ufp) for ufp in uploaded_filepaths])
        else:
            final_msg = f"No files were uploaded between {run_start} and {run_stop}"
        upload_file_directory.logger.info(final_msg)

    #################### PROPERTIES ####################

    @property
    def other_datafile_kwargs(self):
        """
        Overload this property to send extra keyword arguments to the
        :class:`~UploadDataFile` constructor for each recognized file
        (useful if using a custom `datafile_type`)
        """
        return {}

    @property
    def status_msg(self):
        """
        A message stating the number of files currently at each stage of progress,
        up to the maximum number of files
        """
        self.__forget_inactive_files()
        n_wont_be_uploaded = 0
        n_waiting_to_upload = 0
        n_upload_in_progress = 0
        n_fully_enqueued = 0
        n_fully_produced = 0
        for datafile in self.__status_message_files.values():
            if not datafile.to_upload:
                n_wont_be_uploaded += 1
            elif datafile.waiting_to_upload:
                n_waiting_to_upload += 1
            elif datafile.upload_in_progress:
                n_upload_in_progress += 1
            elif datafile.fully_enqueued:
                n_fully_enqueued += 1
            elif datafile.fully_produced:
                n_fully_produced += 1
        if len(self.__status_message_files) > 0:
            status_message = (
                f"Of the {len(self.__status_message_files)} most recent files "
                f"found in {self.dirpath}, "
            )
            ns_msgs = [
                (n_wont_be_uploaded, "will not be uploaded"),
                (n_waiting_to_upload, "are waiting to upload"),
                (n_upload_in_progress, "have uploads in progress"),
                (n_fully_enqueued, "are enqueued to be produced"),
                (n_fully_produced, "are fully produced"),
            ]
            for nvar, msg in ns_msgs:
                if nvar > 0:
                    status_message += f"{nvar} {msg}, "
            status_message = f"{status_message[:-2]}"
        else:
            status_message = f"No files to be uploaded found yet in {self.dirpath}"
        return status_message

    @property
    def progress_msg(self):
        """
        A message describing the files that are currently recognized as part of the directory
        """
        self.__forget_inactive_files()
        if len(self.__status_message_files) > 0:
            msg = (
                "The following files have been recognized so far "
                f"(up to {self.N_STATUS_MESSAGE_FILES} most recent):\n\t"
            )
            msg += "\n\t".join(
                [df.upload_status_msg for df in self.__status_message_files.values()]
            )
        else:
            msg = f"No files to be uploaded found yet in {self.dirpath}"
        return msg

    @property
    def have_file_to_upload(self):
        """
        True if there are any files waiting to be uploaded or in progress
        """
        for datafile in self.__active_files_by_path.values():
            if datafile.upload_in_progress or datafile.waiting_to_upload:
                return True
        return False

    @property
    def partially_done_file_paths(self):
        """
        A list of filepaths whose uploads are currently in progress
        """
        return [
            fp for fp, df in self.__active_files_by_path.items() if df.upload_in_progress
        ]

    @property
    def n_partially_done_files(self):
        """
        The number of files currently in the process of being uploaded
        """
        return len(self.partially_done_file_paths)


def main(args=None):
    """
    Main method to run from command line
    """
    DataFileUploadDirectory.run_from_command_line(args)


if __name__ == "__main__":
    main()
