# imports
import pathlib, shutil, unittest, os, logging, time, datetime
from openmsistream.utilities import LogOwner
from openmsistream.utilities.misc import populated_kwargs
from openmsistream.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream.data_file_io.config import RUN_OPT_CONST
from openmsistream import DataFileUploadDirectory, DataFileDownloadDirectory
from config import TEST_CONST


class TestCaseWithLogger(LogOwner, unittest.TestCase):
    """
    Base class for unittest.TestCase classes that should own a logger
    By default the logger won't write to a file, and will set the stream level to ERROR
    Contains a function to overwrite the current stream level temporarily to log a
    particular message
    """

    def __init__(self, *args, **kwargs):
        kwargs = populated_kwargs(kwargs, {"streamlevel": logging.ERROR})
        super().__init__(*args, **kwargs)

    def log_at_level(self, msg, level, **kwargs):
        """
        Temporarily change the logger stream level for a single message to go through
        """
        old_level = self.logger.get_stream_level()
        self.logger.set_stream_level(level)
        levels_funcs = {
            logging.DEBUG: self.logger.debug,
            logging.INFO: self.logger.info,
            logging.WARNING: self.logger.warning,
            logging.ERROR: self.logger.error,
        }
        levels_funcs[level](msg, **kwargs)
        self.logger.set_stream_level(old_level)

    def log_at_debug(self, msg, **kwargs):
        """
        Log a message at debug level, temporarily changing the stream level
        """
        self.log_at_level(msg, logging.DEBUG, **kwargs)

    def log_at_info(self, msg, **kwargs):
        """
        Log a message at info level, temporarily changing the stream level
        """
        self.log_at_level(msg, logging.INFO, **kwargs)

    def log_at_warning(self, msg, **kwargs):
        """
        Log a message at warning level, temporarily changing the stream level
        """
        self.log_at_level(msg, logging.WARNING, **kwargs)

    def log_at_error(self, msg, **kwargs):
        """
        Log a message at error level, temporarily changing the stream level
        """
        self.log_at_level(msg, logging.ERROR, **kwargs)


class TestCaseWithOutputLocation(TestCaseWithLogger):
    """
    Base class for unittest.TestCase classes that will put output in a directory.
    Also owns an OpenMSIStream Logger.
    """

    def __init__(self, *args, output_dir=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_dir = output_dir
        self.test_dependent_output_dirs = self.output_dir is None
        self.success = False

    def setUp(self):
        """
        Create the output location, removing it if it already exists.
        Set success to false before every test.
        """
        # If output directory isn't set, set it to a directory named for the test function
        if self.output_dir is None:
            self.output_dir = TEST_CONST.TEST_DIR_PATH / f"{self._testMethodName}_output"
        # if output from a previous test already exists, remove it
        if self.output_dir.is_dir():
            self.logger.info(f"Will delete existing output location at {self.output_dir}")
            try:
                shutil.rmtree(self.output_dir)
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.error(
                    f"ERROR: failed to remove existing output at {self.output_dir}!",
                    exc_info=exc,
                )
        # create the directory to hold the output DB
        self.logger.debug(f"Creating output location at {self.output_dir}")
        self.output_dir.mkdir()
        # set the success variable to false
        self.success = False

    def tearDown(self):
        # if the test was successful, remove the output directory
        if self.success:
            try:
                self.logger.debug(
                    f"Test success={self.success}; removing output in {self.output_dir}"
                )
                shutil.rmtree(self.output_dir)
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.error(
                    f"ERROR: failed to remove test output at {self.output_dir}!",
                    exc_info=exc,
                )
        else:
            self.logger.info(
                f"Test success={self.success}; output at {self.output_dir} will be retained."
            )
        # reset the output directory variable if we're using output directories per test
        if self.test_dependent_output_dirs:
            self.output_dir = None


class TestWithDataFileUploadDirectory(TestCaseWithOutputLocation):
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
                except Exception as e:
                    raise e
        self.watched_dir = None
        self.upload_directory = None
        self.upload_thread = None
        super().tearDown()

    def create_upload_directory(
        self, watched_dir=None, cfg_file=TEST_CONST.TEST_CFG_FILE_PATH, update_secs=5
    ):
        """
        Create the upload directory object to use
        """
        # make the directory to watch
        self.watched_dir = watched_dir
        if self.watched_dir is None:
            self.watched_dir = self.output_dir / self.WATCHED_DIR_NAME
        self.watched_dir.mkdir(parents=True)
        # create the DataFileUploadDirectory
        self.upload_directory = DataFileUploadDirectory(
            self.watched_dir,
            cfg_file,
            update_secs=update_secs,
            logger=self.logger,
        )

    def start_upload_thread(
        self,
        topic_name="test",
        n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
        chunk_size=TEST_CONST.TEST_CHUNK_SIZE,
        max_queue_size=RUN_OPT_CONST.DEFAULT_MAX_UPLOAD_QUEUE_MEGABYTES,
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

    def copy_file_to_watched_dir(self, src_path, dest_rel_path):
        """
        Copy a file from src_path to watched_directory/dest_rel_path
        Wait slightly before and after
        """
        time.sleep(1)
        (self.watched_dir / dest_rel_path).write_bytes(src_path.read_bytes())
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


class TestWithDataFileDownloadDirectory(TestCaseWithOutputLocation):
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
                except Exception as e:
                    raise e
        self.watched_dir = None
        self.download_directory = None
        self.download_thread = None
        super().tearDown()

    def create_download_directory(
        self,
        reco_dir=None,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name="test",
        n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
        update_secs=5,
        consumer_group_id='create_new',
    ):
        """
        Create the download directory object to use
        """
        self.reco_dir = reco_dir
        if not self.reco_dir :
            self.reco_dir = self.output_dir/self.RECO_DIR_NAME
        #make the directory to reconstruct files into
        self.reco_dir.mkdir()
        #start up the DataFileDownloadDirectory
        self.download_directory = DataFileDownloadDirectory(
            self.reco_dir,
            cfg_file,
            topic_name,
            n_threads=n_threads,
            update_secs=update_secs,
            consumer_group_id=consumer_group_id,
            logger=self.logger,
        )

    def start_download_thread(self):
        """
        Start running the "reconstruct" function in a new thread
        """
        self.download_thread = ExceptionTrackingThread(
            target=self.download_directory.reconstruct
        )
        self.download_thread.start()

    def wait_for_files_to_reconstruct(self,rel_filepaths,timeout_secs=90):
        """
        Keep running the download thread until one or more files are fully recognized,
        then shut down the download thread
        """
        # keep track of which of the requested files has been reconstructed
        if isinstance(rel_filepaths,pathlib.PurePath):
            rel_filepaths = [rel_filepaths]
        files_found_by_path = {}
        for rel_fp in rel_filepaths :
            files_found_by_path[rel_fp] = False
        # read messages until all files are reconstructed
        current_messages_read = -1
        time_waited = 0
        self.log_at_info(
            f'Waiting to reconstruct files; will timeout after {timeout_secs} seconds)...'
        )
        all_files_found = False
        start_time = datetime.datetime.now()
        while (
            (not all_files_found)
            and (datetime.datetime.now()-start_time).total_seconds()<timeout_secs
        ):
            current_messages_read = self.download_directory.n_msgs_read
            time_waited = (datetime.datetime.now()-start_time).total_seconds()
            self.log_at_info(
                f'\t{current_messages_read} messages read after {time_waited} seconds....'
            )
            time.sleep(5)
            for rel_fp in files_found_by_path:
                if files_found_by_path[rel_fp]:
                    continue
                if rel_fp in self.download_directory.recent_processed_filepaths:
                    files_found_by_path[rel_fp]=True
            all_files_found = sum([files_found_by_path[rel_fp] for rel_fp in files_found_by_path])==len(rel_filepaths)
        # after timing out, stalling, or completely reconstructing the test file, 
        # put the "quit" command into the input queue to stop the function running
        msg = (
            f'Quitting download thread after reading {self.download_directory.n_msgs_read} '
            'messages; will timeout after 30 seconds....'
        )
        self.log_at_info(msg)
        self.download_directory.control_command_queue.put('q')
        # wait for the download thread to finish
        self.download_thread.join(timeout=30)
        if self.download_thread.is_alive() :
            raise TimeoutError('ERROR: download thread timed out after 30 seconds!')

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
