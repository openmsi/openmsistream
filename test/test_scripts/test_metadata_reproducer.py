# imports
import pathlib
import datetime
import json
import pickle
import urllib.request
import os
import time
import importlib.machinery

import pytest

from openmsistream.kafka_wrapper import ConsumerAndProducerGroup

try:
    from .config import TEST_CONST
except ImportError:
    from config import TEST_CONST


# ----------------------------------------------------------------------
# Dynamically load XRDCSVMetadataReproducer
# ----------------------------------------------------------------------

class_path = (
    TEST_CONST.EXAMPLES_DIR_PATH
    / "extracting_metadata"
    / "xrd_csv_metadata_reproducer.py"
)
module_name = class_path.stem
loader = importlib.machinery.SourceFileLoader(module_name, str(class_path))
module = loader.load_module()  # pylint: disable=deprecated-method,no-value-for-parameter


# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

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

SOURCE_TOPIC_NAME = "test_metadata_extractor_source"
DEST_TOPIC_NAME = "test_metadata_extractor_dest"
HEARTBEAT_TOPIC_NAME = "heartbeats"
LOG_TOPIC_NAME = "logs"


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------


@pytest.fixture(scope="module")
def downloaded_upload_file():
    """Download test CSV once per module."""
    urllib.request.urlretrieve(
        TEST_CONST.TUTORIAL_TEST_FILE_URL,
        UPLOAD_FILE,
    )
    yield UPLOAD_FILE
    if UPLOAD_FILE.exists():
        UPLOAD_FILE.unlink()


@pytest.fixture(scope="module")
def start_metadata_reproducer(stream_reproducer_factory):
    """Start the metadata reproducer once for this module."""
    program_id = "reproducer"

    reproducer = stream_reproducer_factory(
        module.XRDCSVMetadataReproducer,
        cfg_file=REP_CONFIG_PATH,
        source_topic_name=SOURCE_TOPIC_NAME,
        dest_topic_name=DEST_TOPIC_NAME,
        consumer_group_id=CONSUMER_GROUP_ID,
        other_init_kwargs={
            "heartbeat_topic_name": HEARTBEAT_TOPIC_NAME,
            "heartbeat_program_id": program_id,
            "heartbeat_interval_secs": 1,
            "log_topic_name": LOG_TOPIC_NAME,
            "log_program_id": program_id,
            "log_interval_secs": 1,
        },
    )

    reproducer.start()
    yield program_id
    reproducer.stop()


# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def run_metadata_reproducer_flow(
    upload_single_file,
    wait_for_files_to_be_processed,
    stream_reproducer,
):
    """Shared logic to upload file and wait for processing."""
    upload_single_file(UPLOAD_FILE, topic_name=SOURCE_TOPIC_NAME)

    recofp = pathlib.Path(UPLOAD_FILE.name)
    wait_for_files_to_be_processed(recofp)

    stream_reproducer.file_registry.in_progress_table.dump_to_file()
    stream_reproducer.file_registry.succeeded_table.dump_to_file()

    assert len(stream_reproducer.file_registry.filepaths_to_rerun) == 0

    time.sleep(5)

    in_prog_entries = (
        stream_reproducer.file_registry.in_progress_table.obj_addresses_by_key_attr(
            "status"
        )
    )
    succeeded_entries = stream_reproducer.file_registry.succeeded_table.obj_addresses

    assert len(succeeded_entries) >= 1
    assert stream_reproducer.file_registry.PRODUCING_MESSAGE_FAILED not in in_prog_entries
    assert stream_reproducer.file_registry.COMPUTING_RESULT_FAILED not in in_prog_entries

    succeeded_attrs = stream_reproducer.file_registry.succeeded_table.get_entry_attrs(
        succeeded_entries[0]
    )
    assert succeeded_attrs["filename"] == UPLOAD_FILE.name


# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "kafka_topics",
    [
        {
            SOURCE_TOPIC_NAME: {},
            DEST_TOPIC_NAME: {},
            HEARTBEAT_TOPIC_NAME: {"--partitions": 1},
            LOG_TOPIC_NAME: {"--partitions": 1},
        }
    ],
    indirect=True,
)
def test_metadata_reproducer_kafka(
    kafka_topics,
    downloaded_upload_file,
    start_metadata_reproducer,
    upload_single_file,
    wait_for_files_to_be_processed,
    stream_reproducer,
    get_heartbeat_messages,
    get_log_messages,
):
    program_id = start_metadata_reproducer

    start_time = datetime.datetime.now()
    start_time_uts = time.time()

    run_metadata_reproducer_flow(
        upload_single_file,
        wait_for_files_to_be_processed,
        stream_reproducer,
    )

    consumer_group = ConsumerAndProducerGroup(
        TEST_CONST.TEST_CFG_FILE_PATH_MDC,
        consumer_topic_name=DEST_TOPIC_NAME,
        consumer_group_id=CONSUMER_GROUP_ID,
    )

    consumer = consumer_group.get_new_subscribed_consumer()

    try:
        success = False
        consume_start = datetime.datetime.now()
        msg = None

        while (
            not success
            and (datetime.datetime.now() - consume_start).total_seconds() < TIMEOUT_SECS
        ):
            msg = consumer.get_next_message()
            if msg is None:
                continue

            msg_dict = json.loads(msg.value())
            created_at = datetime.datetime.strptime(
                msg_dict["metadata_message_generated_at"],
                "%m/%d/%Y, %H:%M:%S",
            )

            if (created_at - start_time).total_seconds() > 0:
                with open(TEST_CONST.TEST_METADATA_DICT_PICKLE_FILE, "rb") as fp:
                    ref_dict = pickle.load(fp)

                matches = sum(1 for k, v in ref_dict.items() if msg_dict.get(k) == v)
                success = matches == len(ref_dict)

        assert msg is not None, (
            f"Could not consume metadata message from {DEST_TOPIC_NAME} "
            f"within {TIMEOUT_SECS} seconds"
        )
        assert success, "Consumed metadata does not match reference"

    finally:
        consumer.close()

    # ------------------------------------------------------------------
    # Validate heartbeats
    # ------------------------------------------------------------------

    heartbeat_msgs = get_heartbeat_messages(
        TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
        HEARTBEAT_TOPIC_NAME,
        program_id,
    )

    assert len(heartbeat_msgs) > 0

    totals = {
        "read_msgs": 0,
        "read_bytes": 0,
        "proc_msgs": 0,
        "proc_bytes": 0,
        "prod_msgs": 0,
        "prod_bytes": 0,
    }

    for msg in heartbeat_msgs:
        msg_dict = json.loads(msg.value())
        ts = datetime.datetime.strptime(
            msg_dict["timestamp"],
            stream_reproducer.TIMESTAMP_FMT,
        )
        assert ts > start_time

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

    # ------------------------------------------------------------------
    # Validate logs
    # ------------------------------------------------------------------

    log_msgs = get_log_messages(
        TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
        LOG_TOPIC_NAME,
        program_id,
    )

    assert len(log_msgs) > 0
    for msg in log_msgs:
        msg_dict = json.loads(msg.value())
        assert float(msg_dict["timestamp"]) >= start_time_uts
