# imports
import shutil, unittest, os
from openmsistream.utilities import LogOwner
from test_scripts.config import TEST_CONST


class TestCaseWithOutputLocation(LogOwner, unittest.TestCase):
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

    # constants
    ENV_VAR_NAMES = [
        "KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS",
        "KAFKA_TEST_CLUSTER_USERNAME",
        "KAFKA_TEST_CLUSTER_PASSWORD",
    ]

    @classmethod
    def setUpClass(cls):
        """
        Set some placeholder environment variable values if they're not already on the system
        """
        cls.ENV_VARS_TO_RESET = []
        for env_var_name in cls.ENV_VAR_NAMES:
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
