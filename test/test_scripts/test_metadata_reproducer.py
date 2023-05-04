# imports
import pathlib, datetime, json, pickle, urllib.request, os
import importlib.machinery, importlib.util
from openmsistream.kafka_wrapper import ConsumerGroup
from config import TEST_CONST # pylint: disable=import-error,wrong-import-order
# pylint: disable=import-error,wrong-import-order
from test_base_classes import TestWithUploadDataFile, TestWithStreamReproducer

# import the XRDCSVMetadataReproducer from the examples directory
class_path = (
    TEST_CONST.EXAMPLES_DIR_PATH
    / "extracting_metadata"
    / "xrd_csv_metadata_reproducer.py"
)
module_name = class_path.name[: -len(".py")]
loader = importlib.machinery.SourceFileLoader(module_name, str(class_path))
module = loader.load_module() # pylint: disable=deprecated-method,no-value-for-parameter

# constants
TIMEOUT_SECS = 90
REP_CONFIG_PATH = (
    TEST_CONST.EXAMPLES_DIR_PATH
    / "extracting_metadata"
    / "test_xrd_csv_metadata_reproducer.config"
)
if os.environ.get("LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS") and os.environ.get(
    "USE_LOCAL_KAFKA_BROKER_IN_TESTS"
):
    REP_CONFIG_PATH = REP_CONFIG_PATH.with_name(f"local_broker_{REP_CONFIG_PATH.name}")
UPLOAD_FILE = TEST_CONST.EXAMPLES_DIR_PATH / "extracting_metadata" / "SC001_XRR.csv"
SOURCE_TOPIC_NAME = (
    TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[: -len(".py")]] + "_source"
)
DEST_TOPIC_NAME = (
    TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[: -len(".py")]] + "_dest"
)
CONSUMER_GROUP_ID = "test_metadata_reproducer"


class TestMetadataReproducer(TestWithUploadDataFile, TestWithStreamReproducer):
    """
    Class for testing that an uploaded file can be read back from the topic
    and have its metadata successfully extracted and produced to another topic
    as a string of JSON
    """

    def setUp(self): # pylint: disable=invalid-name
        """
        Download the test data file from its URL on the PARADIM website
        """
        urllib.request.urlretrieve(TEST_CONST.TUTORIAL_TEST_FILE_URL, UPLOAD_FILE)
        super().setUp()

    def tearDown(self): # pylint: disable=invalid-name
        """
        Remove the test data file that was downloaded
        """
        super().tearDown()
        if UPLOAD_FILE.is_file():
            UPLOAD_FILE.unlink()

    def run_metadata_reproducer(self):
        """
        Convenience function to run the metadata reproducer
        """
        # upload the test data file
        self.upload_single_file(UPLOAD_FILE, topic_name=SOURCE_TOPIC_NAME)
        recofp = pathlib.Path(UPLOAD_FILE.name)
        # wait for the file to be processed
        self.wait_for_files_to_be_processed(recofp)
        # make sure the file is listed in the registry
        self.stream_reproducer.file_registry.in_progress_table.dump_to_file()
        self.stream_reproducer.file_registry.succeeded_table.dump_to_file()
        self.assertEqual(
            len(self.stream_reproducer.file_registry.filepaths_to_rerun), 0
        )
        in_prog_table = self.stream_reproducer.file_registry.in_progress_table
        in_prog_entries = in_prog_table.obj_addresses_by_key_attr("status")
        succeeded_table = self.stream_reproducer.file_registry.succeeded_table
        succeeded_entries = succeeded_table.obj_addresses
        self.assertTrue(len(succeeded_entries) >= 1)
        self.assertTrue(
            self.stream_reproducer.file_registry.PRODUCING_MESSAGE_FAILED
            not in in_prog_entries.keys()
        )
        self.assertTrue(
            self.stream_reproducer.file_registry.COMPUTING_RESULT_FAILED
            not in in_prog_entries.keys()
        )
        # get the attributes of the succeeded file to make sure it matches
        succeeded_entry_attrs = succeeded_table.get_entry_attrs(succeeded_entries[0])
        self.assertTrue(succeeded_entry_attrs["filename"] == UPLOAD_FILE.name)

    def test_metadata_reproducer_kafka(self):
        """
        Test a metadata reproducer
        """
        # make note of the start time
        start_time = datetime.datetime.now()
        # start up the reproducer
        self.create_stream_reproducer(
            module.XRDCSVMetadataReproducer,
            cfg_file=REP_CONFIG_PATH,
            source_topic_name=SOURCE_TOPIC_NAME,
            dest_topic_name=DEST_TOPIC_NAME,
            consumer_group_id=CONSUMER_GROUP_ID,
        )
        self.start_stream_reproducer_thread()
        try:
            self.run_metadata_reproducer()
            # consume messages from the destination topic and make sure the metadata
            # from the test file is there
            consumer_group = ConsumerGroup(
                TEST_CONST.TEST_CFG_FILE_PATH_MDC,
                DEST_TOPIC_NAME,
                consumer_group_id=CONSUMER_GROUP_ID,
            )
            consumer = consumer_group.get_new_subscribed_consumer()
            self.log_at_info(
                f"Consuming metadata message; will timeout after {TIMEOUT_SECS} seconds"
            )
            success = False
            consume_start_time = datetime.datetime.now()
            while (not success) and (
                datetime.datetime.now() - consume_start_time
            ).total_seconds() < TIMEOUT_SECS:
                msg = None
                while (
                    msg is None
                    and (datetime.datetime.now() - consume_start_time).total_seconds()
                    < TIMEOUT_SECS
                ):
                    msg = consumer.get_next_message()
                if msg is None:
                    continue
                msg_value = msg.value()
                msg_dict = json.loads(msg_value)
                created_at_time = datetime.datetime.strptime(
                    msg_dict["metadata_message_generated_at"], "%m/%d/%Y, %H:%M:%S"
                )
                if (created_at_time - start_time).total_seconds() > 0:
                    with open(TEST_CONST.TEST_METADATA_DICT_PICKLE_FILE, "rb") as fp:
                        ref_dict = pickle.load(fp)
                    n_matches = 0
                    for k, v in ref_dict.items():
                        if k in msg_dict.keys() and msg_dict[k] == v:
                            n_matches += 1
                    if n_matches == len(ref_dict):
                        success = True
            if msg is None:
                errmsg = (
                    f"ERROR: could not consume metadata message from {DEST_TOPIC_NAME} "
                    f"within {TIMEOUT_SECS} seconds."
                )
                raise RuntimeError(errmsg)
            if not success:
                errmsg = (
                    "ERROR: message read from destination topic does not match "
                    "the reference metadata dictionary!"
                )
                raise RuntimeError(errmsg)
        except Exception as exc:
            raise exc
        self.success = True # pylint: disable=attribute-defined-outside-init
