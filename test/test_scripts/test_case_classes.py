# imports
import shutil, unittest, os, logging
from openmsistream.utilities import LogOwner
from openmsistream.utilities.misc import populated_kwargs
from config import TEST_CONST


class TestCaseWithLogger(LogOwner, unittest.TestCase):
    """
    Base class for unittest.TestCase classes that should own a logger
    By default the logger won't write to a file, and will set the stream level to ERROR
    Contains a function to overwrite the current stream level temporarily to log a
    particular message
    """

    def __init__(self, *args, **kwargs):
        kwargs = populated_kwargs(
            kwargs,
            {"streamlevel":logging.ERROR}
        )
        super().__init__(*args, **kwargs)

    def log_at_level(self, msg, level, **kwargs):
        """
        Temporarily change the logger stream level for a single message to go through
        """
        old_level = self.logger.get_stream_level()
        self.logger.set_stream_level(level)
        levels_funcs = {
            logging.DEBUG:self.logger.debug,
            logging.INFO:self.logger.info,
            logging.WARNING:self.logger.warning,
            logging.ERROR:self.logger.error,
        }
        levels_funcs[level](msg,**kwargs)
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
            self.output_dir = (
                TEST_CONST.TEST_DIR_PATH / f"{self._testMethodName}_output"
            )
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
