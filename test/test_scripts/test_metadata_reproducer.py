import datetime
import importlib.machinery
import json
import pathlib
import pickle
import time
import urllib.request

import pytest
from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream.kafka_wrapper import ConsumerAndProducerGroup

from .config import TEST_CONST


# ----------------------------------------------------------------------
# Dynamically load XRDCSVMetadataReproducer
# ----------------------------------------------------------------------

_class_path = (
    TEST_CONST.EXAMPLES_DIR_PATH
    / "extracting_metadata"
    / "xrd_csv_metadata_reproducer.py"
)
module = importlib.machinery.SourceFileLoader(  # pylint: disable=deprecated-method,no-value-for-parameter
    _class_path.stem, str(_class_path)
).load_module()


# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

TIMEOUT_SECS = 90
REP_CONFIG_PATH = (
    TEST_CONST.EXAMPLES_DIR_PATH
    / "extracting_metadata"
    / "local_broker_test_xrd_csv_metadata_reproducer.config"
)
UPLOAD_FILE = TEST_CONST.EXAMPLES_DIR_PATH / "extracting_metadata" / "SC001_XRR.csv"
CONSUMER_GROUP_ID = f"test_metadata_reproducer_{TEST_CONST.PY_VERSION}"
TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S.%f"
SOURCE_TOPIC_NAME = "test_metadata_extractor_source"
DEST_TOPIC_NAME = "test_metadata_extractor_dest"
HEARTBEAT_TOPIC_NAME = "heartbeats"
LOG_TOPIC_NAME = "logs"
PROGRAM_ID = "reproducer"

TOPICS = {
    SOURCE_TOPIC_NAME: {},
    DEST_TOPIC_NAME: {},
    HEARTBEAT_TOPIC_NAME: {"--partitions": 1},
    LOG_TOPIC_NAME: {"--partitions": 1},
}


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def create_reproducer(state, logger):
    output_dir = state["tmp_path"] / "reproducer"
    output_dir.mkdir()
    state["reproducer"] = module.XRDCSVMetadataReproducer(
        REP_CONFIG_PATH,
        SOURCE_TOPIC_NAME,
        DEST_TOPIC_NAME,
        output_dir=output_dir,
        consumer_group_id=CONSUMER_GROUP_ID,
        logger=logger,
        heartbeat_topic_name=HEARTBEAT_TOPIC_NAME,
        heartbeat_program_id=PROGRAM_ID,
        heartbeat_interval_secs=1,
        log_topic_name=LOG_TOPIC_NAME,
        log_program_id=PROGRAM_ID,
        log_interval_secs=1,
    )
    return state["reproducer"]


def start_reproducer_thread(state):
    thread = ExceptionTrackingThread(
        target=state["reproducer"].produce_processing_results_for_files_as_read
    )
    thread.start()
    state["reproducer_thread"] = thread


def stop_reproducer(state, timeout_secs=30):
    state["reproducer"].control_command_queue.put("q")
    state["reproducer_thread"].join(timeout=timeout_secs)
    if state["reproducer_thread"].is_alive():
        raise TimeoutError(f"Reproducer thread timed out after {timeout_secs} seconds")


def wait_for_reproducer_results(state, rel_filepaths, timeout_secs=TIMEOUT_SECS):
    if isinstance(rel_filepaths, pathlib.PurePath):
        rel_filepaths = [rel_filepaths]
    found = {p: False for p in rel_filepaths}
    start = time.time()
    rep = state["reproducer"]
    while not all(found.values()) and (time.time() - start) < timeout_secs:
        for p in list(found):
            if not found[p] and p in rep.recent_results_produced:
                found[p] = True
        time.sleep(0.25)
    if not all(found.values()):
        raise TimeoutError(
            f"Files not processed within {timeout_secs} seconds: "
            + str([p for p, v in found.items() if not v])
        )


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------


@pytest.fixture(scope="module")
def downloaded_upload_file():
    """Download the test CSV file once for the whole module."""
    urllib.request.urlretrieve(TEST_CONST.TUTORIAL_TEST_FILE_URL, UPLOAD_FILE)
    yield UPLOAD_FILE
    if UPLOAD_FILE.exists():
        UPLOAD_FILE.unlink()


# ----------------------------------------------------------------------
# Test
# ----------------------------------------------------------------------


@pytest.mark.kafka
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("kafka_topics", "apply_kafka_env")
def test_metadata_reproducer_kafka(
    state,
    logger,
    downloaded_upload_file,
    upload_file_helper,
    get_heartbeat_messages,
    get_log_messages,
):
    start_time = datetime.datetime.now()
    start_time_uts = time.time()
    try:
        # --- Upload the test file and wait for it to be processed ---
        upload_file_helper(UPLOAD_FILE, topic_name=SOURCE_TOPIC_NAME)
        time.sleep(0.5)

        create_reproducer(state, logger)
        start_reproducer_thread(state)
        wait_for_reproducer_results(state, pathlib.Path(UPLOAD_FILE.name))

        # Wait additional time for producer to flush and callbacks to complete
        # The producer callback updates recent_results_produced, but the message
        # might still be buffered and not yet visible to consumers
        time.sleep(0.5)

        # --- Validate file registry ---
        registry = state["reproducer"].file_registry
        in_progress_table = registry.in_progress_table
        succeeded_table = registry.succeeded_table
        in_progress_table.dump_to_file()
        succeeded_table.dump_to_file()
        assert len(registry.filepaths_to_rerun) == 0
        in_prog_entries = in_progress_table.obj_addresses_by_key_attr("status")
        succeeded_entries = succeeded_table.obj_addresses
        assert len(succeeded_entries) >= 1
        assert registry.PRODUCING_MESSAGE_FAILED not in in_prog_entries
        assert registry.COMPUTING_RESULT_FAILED not in in_prog_entries
        assert (
            succeeded_table.get_entry_attrs(succeeded_entries[0])["filename"]
            == UPLOAD_FILE.name
        )

        # --- Validate metadata message on destination topic ---
        consumer_group = ConsumerAndProducerGroup(
            TEST_CONST.TEST_CFG_FILE_PATH_MDC,
            consumer_topic_name=DEST_TOPIC_NAME,
            consumer_group_id=CONSUMER_GROUP_ID + "_validator",
        )
        validator_consumer = consumer_group.get_new_subscribed_consumer(
            restart_at_beginning=True
        )

        try:
            success = False
            msg = None
            consume_start = datetime.datetime.now()
            while (
                not success
                and (datetime.datetime.now() - consume_start).total_seconds()
                < TIMEOUT_SECS
            ):
                msg = validator_consumer.get_next_message(1)  # 1-second poll timeout
                if msg is None:
                    continue
                msg_dict = json.loads(msg.value())
                created_at = datetime.datetime.strptime(
                    msg_dict["metadata_message_generated_at"], "%m/%d/%Y, %H:%M:%S"
                )
                if (created_at - start_time).total_seconds() > 0:
                    with open(TEST_CONST.TEST_METADATA_DICT_PICKLE_FILE, "rb") as fp:
                        ref_dict = pickle.load(fp)
                    success = sum(
                        1 for k, v in ref_dict.items() if msg_dict.get(k) == v
                    ) == len(ref_dict)
            assert msg is not None, (
                f"Could not consume metadata message from {DEST_TOPIC_NAME} "
                f"within {TIMEOUT_SECS} seconds"
            )
            assert success, "Consumed metadata does not match reference"
        finally:
            validator_consumer.close()

        # --- Validate heartbeats ---
        heartbeat_msgs = get_heartbeat_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
            HEARTBEAT_TOPIC_NAME,
            PROGRAM_ID,
            per_wait_secs=5,
        )
        assert len(heartbeat_msgs) > 0
        totals = {
            k: 0
            for k in (
                "read_msgs",
                "read_bytes",
                "proc_msgs",
                "proc_bytes",
                "prod_msgs",
                "prod_bytes",
            )
        }
        for msg in heartbeat_msgs:
            msg_dict = json.loads(msg.value())
            assert (
                datetime.datetime.strptime(msg_dict["timestamp"], TIMESTAMP_FMT)
                > start_time
            )
            totals["read_msgs"] += msg_dict["n_messages_read"]
            totals["read_bytes"] += msg_dict["n_bytes_read"]
            totals["proc_msgs"] += msg_dict["n_messages_processed"]
            totals["proc_bytes"] += msg_dict["n_bytes_processed"]
            totals["prod_msgs"] += msg_dict["n_messages_produced"]
            totals["prod_bytes"] += msg_dict["n_bytes_produced"]
        test_file_size = UPLOAD_FILE.stat().st_size
        test_chunks = int(test_file_size / TEST_CONST.TEST_CHUNK_SIZE)
        assert totals["read_msgs"] >= test_chunks
        assert totals["read_bytes"] >= test_file_size
        assert totals["proc_msgs"] >= test_chunks
        assert totals["proc_bytes"] >= test_file_size
        assert totals["prod_msgs"] == 1
        assert totals["prod_bytes"] > 700

        # --- Validate logs ---
        log_msgs = get_log_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
            LOG_TOPIC_NAME,
            PROGRAM_ID,
            per_wait_secs=5,
        )
        assert len(log_msgs) > 0
        for msg in log_msgs:
            assert float(json.loads(msg.value())["timestamp"]) >= start_time_uts

    finally:
        stop_reproducer(state)
