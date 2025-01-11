# imports
import pathlib, shutil, unittest, os, time, datetime, subprocess, re
from kafkacrypto import KafkaCryptoMessage
from openmsitoolbox.testing import TestWithLogger, TestWithOutputLocation
from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsitoolbox.utilities.misc import populated_kwargs
from openmsistream.utilities.config import RUN_CONST
from openmsistream.kafka_wrapper import OpenMSIStreamConsumer
from openmsistream import (
    DataFileUploadDirectory,
    DataFileDownloadDirectory,
    DataFileStreamProcessor,
    UploadDataFile,
)

try:
    from .config import TEST_CONST  # pylint: disable=import-error,wrong-import-order
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order


class TestWithKafkaTopics(unittest.TestCase):
    """
    Base class for tests that depend on one or more Kafka topics

    Topics are created and deleted in setUp/tearDownClass
    """

    TOPICS = {}

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if os.environ.get("TESTS_NO_KAFKA"):
            return
        if not (
            os.environ.get("LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS")
            and os.environ.get("USE_LOCAL_KAFKA_BROKER_IN_TESTS")
        ):
            return
        for topic_name, args_dict in cls.TOPICS.items():
            n_partitions = 2
            replication_factor = 1
            extra_args = []
            for argflag, argval in args_dict.items():
                if argflag == "--partitions":
                    n_partitions = argval
                    continue
                if argflag == "--replication-factor":
                    replication_factor = argval
                    continue
                extra_args += [argflag, str(argval)]
            cmd = [
                "docker",
                "exec",
                "local_kafka_broker",
                "kafka-topics",
                "--bootstrap-server",
                os.environ.get("LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS"),
                "--create",
                "--partitions",
                str(n_partitions),
                "--replication-factor",
                str(replication_factor),
                *extra_args,
                "--topic",
                topic_name,
            ]
            subprocess.check_output(cmd)

    @classmethod
    def tearDownClass(cls):
        if not (
            os.environ.get("LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS")
            and os.environ.get("USE_LOCAL_KAFKA_BROKER_IN_TESTS")
        ):
            return
        for topic_name in cls.TOPICS:
            cmd = [
                "docker",
                "exec",
                "local_kafka_broker",
                "kafka-topics",
                "--bootstrap-server",
                os.environ.get("LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS"),
                "--delete",
                "--topic",
                topic_name,
            ]
            subprocess.check_output(cmd)
        super().tearDownClass()


class TestWithUploadDataFile(TestWithLogger):
    """
    Base class for tests that need to upload single files
    """

    def upload_single_file(
        self,
        filepath,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name="test",
        rootdir=None,
        n_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS,
        chunk_size=TEST_CONST.TEST_CHUNK_SIZE,
    ):
        """
        Upload a file to a topic using UploadDataFile
        """
        upload_datafile = UploadDataFile(filepath, rootdir=rootdir, logger=self.logger)
        upload_datafile.upload_whole_file(
            cfg_file,
            topic_name,
            n_threads=n_threads,
            chunk_size=chunk_size,
        )


class TestWithOpenMSIStreamOutputLocation(TestWithOutputLocation):
    """
    A test with an output location.
    Overrides the default to be this package's "test" directory.
    """

    def setUp(self):
        """
        Set the default output_dir to one inside the OpenMSIStream package's
        "test" directory
        """
        # If output directory isn't set, set it to a directory named for the test function
        if self.output_dir is None:
            self.output_dir = (
                TEST_CONST.TEST_DIR_PATH
                / f"{self._testMethodName}_output_{TEST_CONST.PY_VERSION}"
            )
        super().setUp()


class TestWithDataFileUploadDirectory(TestWithOpenMSIStreamOutputLocation):
    """
    Base class for tests that need to run a DataFileUploadDirectory
    """

    # constants
    WATCHED_DIR_NAME = "test_watched_dir"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.watched_dir = None
        self.upload_directory = None
        self.upload_thread = None

    def setUp(self):
        """
        Make sure the parameters haven't been set for this test
        """
        super().setUp()
        if self.watched_dir is not None:
            raise RuntimeError(
                f"ERROR: watched_dir = {self.watched_dir} should not be set yet!"
            )
        if self.upload_directory is not None:
            raise RuntimeError(
                f"ERROR: upload_directory = {self.upload_directory} should not be set yet!"
            )
        if self.upload_thread is not None:
            raise RuntimeError(
                f"ERROR: upload_thread = {self.upload_thread} should not be set yet!"
            )

    def tearDown(self):
        """
        Make sure the upload thread is no longer running,
        and reset the variables for a new test
        """
        if self.upload_thread and self.upload_directory:
            if self.upload_thread.is_alive():
                try:
                    self.upload_directory.shutdown()
                    self.upload_thread.join(timeout=30)
                    if self.upload_thread.is_alive():
                        raise TimeoutError("Upload thread timed out after 30 seconds")
                except Exception as exc:
                    raise exc
        self.watched_dir = None
        self.upload_directory = None
        self.upload_thread = None
        super().tearDown()

    def create_upload_directory(
        self,
        watched_dir=None,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH,
        **other_kwargs,
    ):
        """
        Create the upload directory object to use
        """
        other_kwargs = populated_kwargs(other_kwargs, {"logger": self.logger})
        # make the directory to watch
        self.watched_dir = watched_dir
        if self.watched_dir is None:
            self.watched_dir = self.output_dir / self.WATCHED_DIR_NAME
        if not self.watched_dir.is_dir():
            self.watched_dir.mkdir(parents=True)
        # create the DataFileUploadDirectory
        self.upload_directory = DataFileUploadDirectory(
            self.watched_dir,
            cfg_file,
            **other_kwargs,
        )

    def start_upload_thread(
        self,
        topic_name="test",
        n_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS,
        chunk_size=TEST_CONST.TEST_CHUNK_SIZE,
        max_queue_size=RUN_CONST.DEFAULT_MAX_UPLOAD_QUEUE_MEGABYTES,
        upload_existing=True,
    ):
        """
        Start the upload directory running in a new thread with Exceptions tracked
        """
        self.upload_thread = ExceptionTrackingThread(
            target=self.upload_directory.upload_files_as_added,
            args=(topic_name,),
            kwargs={
                "n_threads": n_threads,
                "chunk_size": chunk_size,
                "max_queue_size": max_queue_size,
                "upload_existing": upload_existing,
            },
        )
        self.upload_thread.start()

    def copy_file_to_watched_dir(self, src_path, dest_rel_path=None):
        """
        Copy a file from src_path to watched_directory/dest_rel_path
        Wait slightly before and after
        """
        time.sleep(1)
        if dest_rel_path is not None:
            dest_path = self.watched_dir / dest_rel_path
        else:
            dest_path = self.watched_dir / src_path.name
        if not dest_path.parent.is_dir():
            dest_path.parent.mkdir(parents=True)
        dest_path.write_bytes(src_path.read_bytes())
        time.sleep(5)

    def stop_upload_thread(self, timeout_secs=90):
        """
        Shut down the upload thread
        """
        self.log_at_info(
            f"\nQuitting upload thread; will timeout after {timeout_secs} seconds"
        )
        # put the quit command in the command queue to stop the process running
        self.upload_directory.control_command_queue.put("q")
        # wait for the uploading thread to complete
        self.upload_thread.join(timeout=timeout_secs)
        if self.upload_thread.is_alive():
            raise TimeoutError(
                f"ERROR: upload thread timed out after {timeout_secs} seconds!"
            )


class TestWithDataFileDownloadDirectory(TestWithOpenMSIStreamOutputLocation):
    """
    Base class for tests that need to do things with download directories
    """

    # constants
    RECO_DIR_NAME = "test_reconstruction"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reco_dir = None
        self.download_directory = None
        self.download_thread = None

    def setUp(self):
        """
        Make sure the parameters haven't been set for this test
        """
        super().setUp()
        if self.reco_dir is not None:
            raise RuntimeError(
                f"ERROR: reco_dir = {self.reco_dir} should not be set yet!"
            )
        if self.download_directory is not None:
            raise RuntimeError(
                f"ERROR: download_directory = {self.download_directory} should not be set yet!"
            )
        if self.download_thread is not None:
            raise RuntimeError(
                f"ERROR: download_thread = {self.download_thread} should not be set yet!"
            )

    def tearDown(self):
        """
        Make sure the download thread is no longer running,
        and reset the variables for a new test
        """
        if self.download_thread and self.download_directory:
            if self.download_thread.is_alive():
                try:
                    self.download_directory.shutdown()
                    self.download_thread.join(timeout=30)
                    if self.download_thread.is_alive():
                        raise TimeoutError("Download thread timed out after 30 seconds")
                except Exception as exc:
                    raise exc
        self.reco_dir = None
        self.download_directory = None
        self.download_thread = None
        super().tearDown()

    def create_download_directory(
        self,
        reco_dir=None,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name="test",
        **other_kwargs,
    ):
        """
        Create the download directory object to use
        """
        other_kwargs = populated_kwargs(other_kwargs, {"logger": self.logger})
        self.reco_dir = reco_dir
        if not self.reco_dir:
            self.reco_dir = self.output_dir / self.RECO_DIR_NAME
        # make the directory to reconstruct files into
        if not self.reco_dir.is_dir():
            self.reco_dir.mkdir(parents=True)
        # start up the DataFileDownloadDirectory
        self.download_directory = DataFileDownloadDirectory(
            self.reco_dir,
            cfg_file,
            topic_name,
            **other_kwargs,
        )

    def start_download_thread(self):
        """
        Start running the "reconstruct" function in a new thread
        """
        self.download_thread = ExceptionTrackingThread(
            target=self.download_directory.reconstruct
        )
        self.download_thread.start()

    def wait_for_files_to_reconstruct(
        self, rel_filepaths, timeout_secs=90, before_close_callback=lambda *args: None
    ):
        """
        Keep running the download thread until one or more files are fully recognized,
        then shut down the download thread
        """
        # keep track of which of the requested files has been reconstructed
        if isinstance(rel_filepaths, pathlib.PurePath):
            rel_filepaths = [rel_filepaths]
        files_found_by_path = {}
        for rel_fp in rel_filepaths:
            files_found_by_path[rel_fp] = False
        # read messages until all files are reconstructed
        current_messages_read = -1
        time_waited = 0
        self.log_at_info(
            f"Waiting to reconstruct files; will timeout after {timeout_secs} seconds..."
        )
        all_files_found = False
        start_time = datetime.datetime.now()
        while (not all_files_found) and (
            datetime.datetime.now() - start_time
        ).total_seconds() < timeout_secs:
            current_messages_read = self.download_directory.n_msgs_read
            time_waited = (datetime.datetime.now() - start_time).total_seconds()
            self.log_at_info(
                f"\t{current_messages_read} messages read after {time_waited:.2f} seconds...."
            )
            time.sleep(5)
            for rel_fp, found_file in files_found_by_path.items():
                if found_file:
                    continue
                if rel_fp in self.download_directory.recent_processed_filepaths:
                    files_found_by_path[rel_fp] = True
            all_files_found = sum(files_found_by_path.values()) == len(rel_filepaths)
        # Do any extra work before killing thread
        before_close_callback()
        # after timing out, stalling, or completely reconstructing the test file,
        # put the "quit" command into the input queue to stop the function running
        msg = (
            f"Quitting download thread after reading {self.download_directory.n_msgs_read} "
            "messages; will timeout after 30 seconds...."
        )
        self.log_at_info(msg)
        self.download_directory.control_command_queue.put("q")
        # wait for the download thread to finish
        self.download_thread.join(timeout=30)
        if self.download_thread.is_alive():
            raise TimeoutError("ERROR: download thread timed out after 30 seconds!")


class DataFileStreamProcessorForTesting(DataFileStreamProcessor):
    """
    Class to use for testing DataFileStreamProcessor functions since it's an abstract base class
    """

    def __init__(self, *args, **kwargs):
        self.checked = False
        self.completed_filenames_bytestrings = []
        self.filenames_to_fail = []
        super().__init__(*args, **kwargs)

    def _process_downloaded_data_file(self, datafile, lock):
        # fail any files that have been set to fail
        if datafile.filename in self.filenames_to_fail:
            with lock:
                self.completed_filenames_bytestrings.append((datafile.filename, None))
            return ValueError(
                f"ERROR: testing processing for {datafile.filename} is set to fail!"
            )
        # add the filename and bytestring to the list
        with lock:
            self.completed_filenames_bytestrings.append(
                (datafile.filename, datafile.bytestring)
            )
        return None

    def _failed_processing_callback(self, datafile, lock):
        if datafile.filename not in self.filenames_to_fail:
            raise RuntimeError("ERROR: _failed_processing_callback invoked in test!")

    def _mismatched_hash_callback(self, datafile, lock):
        raise RuntimeError("ERROR: _mismatched_hash_callback invoked in test!")

    def _on_check(self):
        self.checked = True
        super()._on_check()

    @classmethod
    def run_from_command_line(cls, args=None):
        pass


class TestWithStreamProcessor(TestWithOpenMSIStreamOutputLocation):
    """
    Base class for tests that need to run stream processors
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream_processor = None
        self.stream_processor_thread = None

    def setUp(self):
        """
        Make sure the parameters haven't been set for this test
        """
        super().setUp()
        if self.stream_processor is not None:
            raise RuntimeError(
                f"ERROR: stream_processor = {self.stream_processor} should not be set yet!"
            )
        if self.stream_processor_thread is not None:
            errmsg = (
                f"ERROR: stream_processor_thread = {self.stream_processor_thread} "
                "but it should not be set yet!"
            )
            raise RuntimeError(errmsg)

    def tearDown(self):
        """
        Make sure the download thread is no longer running,
        and reset the variables for a new test
        """
        self.reset_stream_processor()
        super().tearDown()

    def create_stream_processor(
        self,
        stream_processor_type=DataFileStreamProcessorForTesting,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name="test",
        output_dir=None,
        n_threads=RUN_CONST.N_DEFAULT_DOWNLOAD_THREADS,
        consumer_group_id="create_new",
        other_init_args=(),
        other_init_kwargs=None,
    ):
        """
        Create the stream processor object to use
        """
        if self.stream_processor is not None:
            raise RuntimeError(
                f"ERROR: stream processor is {self.stream_processor} but should be None!"
            )
        if output_dir is None:
            output_dir = self.output_dir
        if not other_init_kwargs:
            other_init_kwargs = {}
        self.stream_processor = stream_processor_type(
            *other_init_args,
            cfg_file,
            topic_name,
            output_dir=output_dir,
            n_threads=n_threads,
            consumer_group_id=consumer_group_id,
            logger=self.logger,
            **other_init_kwargs,
        )

    def start_stream_processor_thread(self, func=None, args=(), kwargs=None):
        """
        Start running the stream processor in a new thread
        """
        if self.stream_processor_thread is not None:
            errmsg = (
                f"ERROR: stream processor thread is {self.stream_processor_thread} "
                "but it should be None!"
            )
            raise RuntimeError(errmsg)
        if func is None:
            func = self.stream_processor.process_files_as_read
        if not kwargs:
            kwargs = {}
        self.stream_processor_thread = ExceptionTrackingThread(
            target=func, args=args, kwargs=kwargs
        )
        self.stream_processor_thread.start()

    def wait_for_files_to_be_processed(self, rel_filepaths, timeout_secs=90):
        """
        Keep running the stream processor thread until one or more files are processed,
        then shut down the stream processor thread
        """
        # keep track of which of the requested files has been reconstructed
        if isinstance(rel_filepaths, pathlib.PurePath):
            rel_filepaths = [rel_filepaths]
        files_found_by_path = {}
        for rel_fp in rel_filepaths:
            files_found_by_path[rel_fp] = False
        # read messages until all files are processed
        current_messages_read = -1
        time_waited = 0
        self.log_at_info(
            f"Waiting to process files; will timeout after {timeout_secs} seconds..."
        )
        all_files_found = False
        start_time = datetime.datetime.now()
        while (not all_files_found) and (
            datetime.datetime.now() - start_time
        ).total_seconds() < timeout_secs:
            current_messages_read = self.stream_processor.n_msgs_read
            time_waited = (datetime.datetime.now() - start_time).total_seconds()
            self.log_at_info(
                f"\t{current_messages_read} messages read after {time_waited:.2f} seconds...."
            )
            time.sleep(5)
            for rel_fp, found_file in files_found_by_path.items():
                if found_file:
                    continue
                if rel_fp in self.stream_processor.recent_processed_filepaths:
                    files_found_by_path[rel_fp] = True
            all_files_found = sum(files_found_by_path.values()) == len(rel_filepaths)
        # after timing out, stalling, or completely processing the test file,
        # put the "quit" command into the input queue to stop the function running
        msg = (
            f"Quitting stream processor thread after reading {self.stream_processor.n_msgs_read} "
            "messages; will timeout after 30 seconds...."
        )
        self.log_at_info(msg)
        self.stream_processor.control_command_queue.put("q")
        # wait for the download thread to finish
        self.stream_processor_thread.join(timeout=30)
        if self.stream_processor_thread.is_alive():
            raise TimeoutError(
                "ERROR: stream processor thread timed out after 30 seconds!"
            )

    def reset_stream_processor(self, remove_output=False):
        """
        Shut down any running stream processor thread and set the stream processor
        and its thread back to None. Also optionally remove and re-create the output dir
        """
        if self.stream_processor_thread and self.stream_processor:
            if self.stream_processor_thread.is_alive():
                try:
                    self.stream_processor.shutdown()
                    self.stream_processor_thread.join(timeout=30)
                    if self.stream_processor_thread.is_alive():
                        raise TimeoutError("Download thread timed out after 30 seconds")
                except Exception as exc:
                    raise exc
        self.stream_processor = None
        self.stream_processor_thread = None
        if remove_output:
            shutil.rmtree(self.output_dir)
            self.output_dir.mkdir(parents=True)


class TestWithStreamReproducer(TestWithOpenMSIStreamOutputLocation):
    """
    Base class for tests that need to run stream reproducers
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream_reproducer = None
        self.stream_reproducer_thread = None

    def setUp(self):
        """
        Make sure the parameters haven't been set for this test
        """
        super().setUp()
        if self.stream_reproducer is not None:
            errmsg = (
                f"ERROR: stream_reproducer = {self.stream_reproducer} "
                "but it should not be set yet!"
            )
            raise RuntimeError(errmsg)
        if self.stream_reproducer_thread is not None:
            errmsg = (
                f"ERROR: stream_reproducer_thread = {self.stream_reproducer_thread} "
                "but it should not be set yet!"
            )
            raise RuntimeError(errmsg)

    def tearDown(self):
        """
        Make sure the download thread is no longer running,
        and reset the variables for a new test
        """
        self.reset_stream_reproducer()
        super().tearDown()

    def create_stream_reproducer(
        self,
        stream_reproducer_type,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH,
        source_topic_name="test",
        dest_topic_name="test",
        output_dir=None,
        n_producer_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS,
        n_consumer_threads=RUN_CONST.N_DEFAULT_DOWNLOAD_THREADS,
        consumer_group_id="create_new",
        other_init_args=(),
        other_init_kwargs=None,
    ):
        """
        Create the stream processor object to use
        """
        if self.stream_reproducer is not None:
            raise RuntimeError(
                f"ERROR: stream processor is {self.stream_reproducer} but should be None!"
            )
        if output_dir is None:
            output_dir = self.output_dir
        if not other_init_kwargs:
            other_init_kwargs = {}
        self.stream_reproducer = stream_reproducer_type(
            *other_init_args,
            cfg_file,
            source_topic_name,
            dest_topic_name,
            output_dir=output_dir,
            n_producer_threads=n_producer_threads,
            n_consumer_threads=n_consumer_threads,
            consumer_group_id=consumer_group_id,
            logger=self.logger,
            **other_init_kwargs,
        )

    def start_stream_reproducer_thread(self, func=None, args=(), kwargs=None):
        """
        Start running the stream processor in a new thread
        """
        if self.stream_reproducer_thread is not None:
            errmsg = (
                f"ERROR: stream reproducer thread is {self.stream_reproducer_thread} "
                "but it should be None!"
            )
            raise RuntimeError(errmsg)
        if func is None:
            func = self.stream_reproducer.produce_processing_results_for_files_as_read
        if not kwargs:
            kwargs = {}
        self.stream_reproducer_thread = ExceptionTrackingThread(
            target=func, args=args, kwargs=kwargs
        )
        self.stream_reproducer_thread.start()

    def wait_for_files_to_be_processed(self, rel_filepaths, timeout_secs=90):
        """
        Keep running the stream processor thread until one or more files are processed,
        then shut down the stream processor thread
        """
        # keep track of which of the requested files has been reconstructed
        if isinstance(rel_filepaths, pathlib.PurePath):
            rel_filepaths = [rel_filepaths]
        files_found_by_path = {}
        for rel_fp in rel_filepaths:
            files_found_by_path[rel_fp] = False
        # read messages until all files are processed
        current_messages_read = -1
        time_waited = 0
        msg = (
            "Waiting to process files and produce their computed messages; "
            f"will timeout after {timeout_secs} seconds..."
        )
        self.log_at_info(msg)
        all_files_found = False
        start_time = datetime.datetime.now()
        while (not all_files_found) and (
            datetime.datetime.now() - start_time
        ).total_seconds() < timeout_secs:
            current_messages_read = self.stream_reproducer.n_msgs_read
            time_waited = (datetime.datetime.now() - start_time).total_seconds()
            self.log_at_info(
                f"\t{current_messages_read} messages read after {time_waited:.2f} seconds...."
            )
            time.sleep(5)
            for rel_fp, found_file in files_found_by_path.items():
                if found_file:
                    continue
                if rel_fp in self.stream_reproducer.recent_processed_filepaths:
                    files_found_by_path[rel_fp] = True
            all_files_found = sum(files_found_by_path.values()) == len(rel_filepaths)
        # after timing out, stalling, or completely processing the test file,
        # put the "quit" command into the input queue to stop the function running
        msg = (
            f"Quitting stream reproducer thread after reading {self.stream_reproducer.n_msgs_read} "
            "messages; will timeout after 30 seconds...."
        )
        self.log_at_info(msg)
        self.stream_reproducer.control_command_queue.put("q")
        # wait for the download thread to finish
        self.stream_reproducer_thread.join(timeout=30)
        if self.stream_reproducer_thread.is_alive():
            raise TimeoutError(
                "ERROR: stream reproducer thread timed out after 30 seconds!"
            )

    def reset_stream_reproducer(self, remove_output=False):
        """
        Shut down any running stream reproducer thread and set the stream reproducer
        and its thread back to None. Also optionally remove and re-create the output dir
        """
        if self.stream_reproducer_thread and self.stream_reproducer:
            if self.stream_reproducer_thread.is_alive():
                try:
                    self.stream_reproducer.shutdown()
                    self.stream_reproducer_thread.join(timeout=30)
                    if self.stream_reproducer_thread.is_alive():
                        raise TimeoutError("Download thread timed out after 30 seconds")
                except Exception as exc:
                    raise exc
        self.stream_reproducer = None
        self.stream_reproducer_thread = None
        if remove_output:
            shutil.rmtree(self.output_dir)
            self.output_dir.mkdir(parents=True)


class TestWithEnvVars(unittest.TestCase):
    """
    Class for performing tests that don't interact with the Kafka cluster
    but that do need some environment variable values set to run properly
    """

    @classmethod
    def setUpClass(cls):
        """
        Set some placeholder environment variable values if they're not already on the system
        """
        super().setUpClass()
        cls.ENV_VARS_TO_RESET = []
        for env_var_name in TEST_CONST.ENV_VAR_NAMES:
            if os.path.expandvars(f"${env_var_name}") == f"${env_var_name}":
                os.environ[env_var_name] = f"PLACEHOLDER_{env_var_name}"
                cls.ENV_VARS_TO_RESET.append(env_var_name)

    @classmethod
    def tearDownClass(cls):
        """
        Unset any of the placeholder environment variables
        """
        for env_var_name in cls.ENV_VARS_TO_RESET:
            os.environ.pop(env_var_name)
        super().tearDownClass()


class TestWithHeartbeats(TestWithKafkaTopics, TestWithLogger):
    """
    Class for running tests that might produce heartbeat messages to a topic
    """

    TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S.%f"

    def get_heartbeat_messages(
        self,
        config_path,
        heartbeat_topic_name,
        program_id,
        per_wait_secs=5,
    ):
        """Return a list of all the heartbeat messages sent to a particular topic with
        a particular program ID
        """
        c_args, c_kwargs = OpenMSIStreamConsumer.get_consumer_args_kwargs(
            config_path,
            logger=self.logger,
            max_wait_per_decrypt=1,
        )
        heartbeat_consumer = OpenMSIStreamConsumer(
            *c_args,
            **c_kwargs,
            message_key_regex=re.compile(f"{program_id}_heartbeat"),
            filter_new_message_keys=True,
        )
        heartbeat_consumer.subscribe([heartbeat_topic_name])
        heartbeat_msgs = []
        start_time = datetime.datetime.now()
        cutoff_time = (time.time() + per_wait_secs) * 1000  # Kafka timestamps in ms
        last_msg_time = 0
        while (
            datetime.datetime.now() - start_time
        ).total_seconds() < per_wait_secs and last_msg_time < cutoff_time:
            msg = heartbeat_consumer.get_next_message(1)
            if msg is not None:
                # extract message timestamp
                try:
                    _, last_msg_time = msg.timestamp()
                except TypeError:
                    _, last_msg_time = msg.timestamp
                if not isinstance(msg.value, (KafkaCryptoMessage,)):
                    heartbeat_msgs.append(msg)
                    start_time = (
                        datetime.datetime.now()
                    )  # reset per wait timer on each success
        heartbeat_consumer.close()
        return heartbeat_msgs


class TestWithLogs(TestWithKafkaTopics, TestWithLogger):
    """
    Class for running tests that might produce log messages to a topic
    """

    TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S.%f"

    def get_log_messages(self, config_path, log_topic_name, program_id, per_wait_secs=30):
        """Return a list of all the log messages sent to a particular topic with
        a particular program ID
        """
        c_args, c_kwargs = OpenMSIStreamConsumer.get_consumer_args_kwargs(
            config_path,
            logger=self.logger,
            max_wait_per_decrypt=0.1,
        )
        log_consumer = OpenMSIStreamConsumer(
            *c_args,
            **c_kwargs,
            message_key_regex=re.compile(f"{program_id}_log"),
            filter_new_message_keys=True,
        )
        log_consumer.subscribe([log_topic_name])
        log_msgs = []
        start_time = datetime.datetime.now()
        cutoff_time = (time.time() + per_wait_secs) * 1000  # Kafka timestamps in ms
        last_msg_time = 0
        while (
            datetime.datetime.now() - start_time
        ).total_seconds() < per_wait_secs and last_msg_time < cutoff_time:
            msg = log_consumer.get_next_message(1)
            if msg is not None:
                # extract message timestamp
                try:
                    _, last_msg_time = msg.timestamp()
                except TypeError:
                    _, last_msg_time = msg.timestamp
                if not isinstance(msg.value, (KafkaCryptoMessage,)):
                    log_msgs.append(msg)
                    start_time = (
                        datetime.datetime.now()
                    )  # reset per wait timer on each success
        log_consumer.close()
        return log_msgs
