import pytest

from .config import TEST_CONST
from .controlled_process_helpers import (
    ControlledProcessMultiThreadedHeartbeatsForTesting,
    ControlledProcessSingleThreadHeartbeatsForTesting,
)

TIMEOUT_SECS = 10
N_THREADS = 3


# ----------------------------------------------------------------------
# Fixtures replacing TestWithHeartbeats
# ----------------------------------------------------------------------


@pytest.fixture
def timestamp_fmt():
    """Timestamp format used by the heartbeat messages."""
    return "%Y-%m-%d %H:%M:%S.%f"


# ----------------------------------------------------------------------
# Test parameters
# ----------------------------------------------------------------------
TOPIC_NAME = "heartbeats"
TOPICS = {TOPIC_NAME: {"--partitions": 1}}


@pytest.mark.kafka
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics")
class TestControlledProcessHeartbeats:
    """Pytest version of ControlledProcess heartbeat tests."""

    # ---------------------------------------------------------
    def test_single_thread_heartbeats(
        self, logger, get_heartbeat_messages, timestamp_fmt, run_controlled_process_test
    ):
        program_id = "test_controlled_process_single_thread"

        cp = ControlledProcessSingleThreadHeartbeatsForTesting(
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
            heartbeat_topic_name=TOPIC_NAME,
            heartbeat_program_id=program_id,
            heartbeat_interval_secs=1,
            logger=logger,
        )
        run_controlled_process_test(
            cp,
            get_heartbeat_messages,
            program_id,
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
            TOPIC_NAME,
        )

    # ---------------------------------------------------------
    def test_multi_thread_heartbeats(
        self, logger, get_heartbeat_messages, timestamp_fmt, run_controlled_process_test
    ):
        program_id = "test_controlled_process_multi_threaded"

        cp = ControlledProcessMultiThreadedHeartbeatsForTesting(
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
            heartbeat_topic_name=TOPIC_NAME,
            heartbeat_program_id=program_id,
            heartbeat_interval_secs=1,
            logger=logger,
            n_threads=N_THREADS,
        )
        run_controlled_process_test(
            cp,
            get_heartbeat_messages,
            program_id,
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
            TOPIC_NAME,
        )
