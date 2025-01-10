# imports
import pathlib, urllib.request
import importlib.machinery, importlib.util

try:
    from .config import TEST_CONST  # pylint: disable=import-error

    # pylint: disable=import-error
    from .base_classes import (
        TestWithKafkaTopics,
        TestWithUploadDataFile,
        TestWithStreamProcessor,
    )
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error

    # pylint: disable=import-error
    from base_classes import (
        TestWithKafkaTopics,
        TestWithUploadDataFile,
        TestWithStreamProcessor,
    )


# import the XRDCSVPlotter from the examples directory
class_path = TEST_CONST.EXAMPLES_DIR_PATH / "creating_plots" / "xrd_csv_plotter.py"
module_name = class_path.name[: -len(".py")]
loader = importlib.machinery.SourceFileLoader(module_name, str(class_path))
module = loader.load_module()  # pylint: disable=deprecated-method,no-value-for-parameter

# constants
TIMEOUT_SECS = 90
UPLOAD_FILE = TEST_CONST.EXAMPLES_DIR_PATH / "creating_plots" / "SC001_XRR.csv"
CONSUMER_GROUP_ID = f"test_plots_for_tutorial_{TEST_CONST.PY_VERSION}"


class TestPlotsForTutorial(
    TestWithKafkaTopics, TestWithUploadDataFile, TestWithStreamProcessor
):
    """
    Class for testing that an uploaded file can be read back from the topic and have its metadata
    successfully extracted and produced to another topic as a string of JSON
    """

    TOPIC_NAME = "test_plots_for_tutorial"

    TOPICS = {TOPIC_NAME: {}}

    def setUp(self):  # pylint: disable=invalid-name
        """
        Download the test data file from its URL on the PARADIM website
        """
        urllib.request.urlretrieve(TEST_CONST.TUTORIAL_TEST_FILE_URL, UPLOAD_FILE)
        super().setUp()

    def tearDown(self):  # pylint: disable=invalid-name
        """
        Remove the test data file that was downloaded
        """
        super().tearDown()
        if UPLOAD_FILE.is_file():
            UPLOAD_FILE.unlink()

    def test_plots_for_tutorial_kafka(self):
        """
        Test processing a file from a topic and creating plots from it
        """
        # start up the plot maker
        self.create_stream_processor(
            module.XRDCSVPlotter,
            topic_name=self.TOPIC_NAME,
            consumer_group_id=CONSUMER_GROUP_ID,
        )
        self.start_stream_processor_thread()
        try:
            # upload the test data file
            self.upload_single_file(UPLOAD_FILE, topic_name=self.TOPIC_NAME)
            recofp = pathlib.Path(UPLOAD_FILE.name)
            # wait for the file to be processed
            self.wait_for_files_to_be_processed(recofp)
            # make sure the file is listed in the 'results_produced' file
            self.stream_processor.file_registry.in_progress_table.dump_to_file()
            self.stream_processor.file_registry.succeeded_table.dump_to_file()
            self.assertEqual(
                len(self.stream_processor.file_registry.filepaths_to_rerun), 0
            )
            in_prog_table = self.stream_processor.file_registry.in_progress_table
            in_prog_entries = in_prog_table.obj_addresses_by_key_attr("status")
            succeeded_table = self.stream_processor.file_registry.succeeded_table
            succeeded_entries = succeeded_table.obj_addresses
            self.assertTrue(len(succeeded_entries) >= 1)
            self.assertTrue(
                self.stream_processor.file_registry.FAILED not in in_prog_entries.keys()
            )
            # get the attributes of the succeeded file to make sure its the one that was produced
            succeeded_entry_attrs = succeeded_table.get_entry_attrs(succeeded_entries[0])
            self.assertTrue(succeeded_entry_attrs["filename"] == UPLOAD_FILE.name)
            # make sure the plot file exists
            self.assertTrue(
                (self.output_dir / (UPLOAD_FILE.stem + "_xrd_plot.png")).is_file()
            )
        except Exception as exc:
            raise exc
        self.success = True  # pylint: disable=attribute-defined-outside-init
