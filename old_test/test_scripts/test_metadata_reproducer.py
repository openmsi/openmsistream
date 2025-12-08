# imports
import pathlib, datetime, json, pickle, urllib.request, os, time
import importlib.machinery, importlib.util
from openmsistream.kafka_wrapper import ConsumerAndProducerGroup

try:
    from .config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from .base_classes import (
        TestWithHeartbeats,
        TestWithLogs,
        TestWithUploadDataFile,
        TestWithStreamReproducer,
    )
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from base_classes import (
        TestWithHeartbeats,
        TestWithLogs,
        TestWithUploadDataFile,
        TestWithStreamReproducer,
    )

# import the XRDCSVMetadataReproducer from the examples directory
class_path = (
    TEST_CONST.EXAMPLES_DIR_PATH
    / "extracting_metadata"
    / "xrd_csv_metadata_reproducer.py"
)
module_name = class_path.name[: -len(".py")]
loader = importlib.machinery.SourceFileLoader(module_name, str(class_path))
module = loader.load_module()  # pylint: disable=deprecated-method,no-value-for-parameter

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
CONSUMER_GROUP_ID = f"test_metadata_reproducer_{TEST_CONST.PY_VERSION}"


class TestMetadataReproducer(
    TestWithHeartbeats, TestWithLogs, TestWithUploadDataFile, TestWithStreamReproducer
):
    """
    Class for testing that an uploaded file can be read back from the topic
    and have its metadata successfully extracted and produced to another topic
    as a string of JSON
    """

    SOURCE_TOPIC_NAME = "test_metadata_extractor_source"
    DEST_TOPIC_NAME = "test_metadata_extractor_dest"
    HEARTBEAT_TOPIC_NAME = "heartbeats"
    LOG_TOPIC_NAME = "logs"

    TOPICS = {
        SOURCE_TOPIC_NAME: {},
        DEST_TOPIC_NAME: {},
        HEARTBEAT_TOPIC_NAME: {"--partitions": 1},
        LOG_TOPIC_NAME: {"--partitions": 1},
    }

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

    def run_metadata_reproducer(self):
        """
        Convenience function to run the metadata reproducer
        """
        # upload the test data file
        self.upload_single_file(UPLOAD_FILE, topic_name=self.SOURCE_TOPIC_NAME)
        recofp = pathlib.Path(UPLOAD_FILE.name)
        # wait for the file to be processed
        self.wait_for_files_to_be_processed(recofp)
        # make sure the file is listed in the registry
        self.stream_reproducer.file_registry.in_progress_table.dump_to_file()
        self.stream_reproducer.file_registry.succeeded_table.dump_to_file()
        self.assertEqual(len(self.stream_reproducer.file_registry.filepaths_to_rerun), 0)
        time.sleep(5)
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
        start_time_uts = time.time()
        # start up the reproducer
        program_id = "reproducer"
        self.create_stream_reproducer(
            module.XRDCSVMetadataReproducer,
            cfg_file=REP_CONFIG_PATH,
            source_topic_name=self.SOURCE_TOPIC_NAME,
            dest_topic_name=self.DEST_TOPIC_NAME,
            consumer_group_id=CONSUMER_GROUP_ID,
            other_init_kwargs={
                "heartbeat_topic_name": self.HEARTBEAT_TOPIC_NAME,
                "heartbeat_program_id": program_id,
                "heartbeat_interval_secs": 1,
                "log_topic_name": self.LOG_TOPIC_NAME,
                "log_program_id": program_id,
                "log_interval_secs": 1,
            },
        )
        self.start_stream_reproducer_thread()
        consumer = None
        try:
            self.run_metadata_reproducer()
            # consume messages from the destination topic and make sure the metadata
            # from the test file is there
            consumer_group = ConsumerAndProducerGroup(
                TEST_CONST.TEST_CFG_FILE_PATH_MDC,
                consumer_topic_name=self.DEST_TOPIC_NAME,
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
                    f"ERROR: could not consume metadata message from {self.DEST_TOPIC_NAME} "
                    f"within {TIMEOUT_SECS} seconds."
                )
                raise RuntimeError(errmsg)
            if not success:
                errmsg = (
                    "ERROR: message read from destination topic does not match "
                    "the reference metadata dictionary!"
                )
                raise RuntimeError(errmsg)
            # validate the heartbeat messages
            self.validate_heartbeats(program_id, start_time)
            # validate the log messages
            self.validate_logs(program_id, start_time_uts)
        except Exception as exc:
            raise exc
        finally:
            if consumer is not None:
                consumer.close()
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def validate_heartbeats(self, program_id, start_time):
        """Validate that the metadata reproducer sent heartbeat messages with the
        correct structure and content
        """
        heartbeat_msgs = self.get_heartbeat_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
            self.HEARTBEAT_TOPIC_NAME,
            program_id,
        )
        self.assertTrue(len(heartbeat_msgs) > 0)
        total_msgs_read = 0
        total_bytes_read = 0
        total_msgs_processed = 0
        total_bytes_processed = 0
        total_msgs_produced = 0
        total_bytes_produced = 0
        for msg in heartbeat_msgs:
            msg_dict = json.loads(msg.value())
            msg_timestamp = datetime.datetime.strptime(
                msg_dict["timestamp"], self.TIMESTAMP_FMT
            )
            self.assertTrue(msg_timestamp > start_time)
            total_msgs_read += msg_dict["n_messages_read"]
            total_bytes_read += msg_dict["n_bytes_read"]
            total_msgs_processed += msg_dict["n_messages_processed"]
            total_bytes_processed += msg_dict["n_bytes_processed"]
            total_msgs_produced += msg_dict["n_messages_produced"]
            total_bytes_produced += msg_dict["n_bytes_produced"]
        test_file_size = UPLOAD_FILE.stat().st_size
        test_file_n_chunks = int(test_file_size / TEST_CONST.TEST_CHUNK_SIZE)
        self.assertTrue(total_msgs_read >= test_file_n_chunks)
        self.assertTrue(total_bytes_read >= test_file_size)
        self.assertTrue(total_msgs_processed >= test_file_n_chunks)
        self.assertTrue(total_bytes_processed >= test_file_size)
        self.assertTrue(total_msgs_produced == 1)
        self.assertTrue(total_bytes_produced > 700)  # hardcoded from one example run

    def validate_logs(self, program_id, start_time):
        """Validate that the metadata reproducer sent log messages with
        content
        """
        log_msgs = self.get_log_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
            self.LOG_TOPIC_NAME,
            program_id,
        )
        self.assertTrue(len(log_msgs) > 0)
        for msg in log_msgs:
            msg_dict = json.loads(msg.value())
            self.assertTrue(float(msg_dict["timestamp"]) >= start_time)
