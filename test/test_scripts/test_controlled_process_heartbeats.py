import time
import json
import datetime
import pytest

from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread
from .config import TEST_CONST
from .controlled_process_helpers import (
    ControlledProcessSingleThreadHeartbeatsForTesting,
    ControlledProcessMultiThreadedHeartbeatsForTesting,
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
        self, logger, get_heartbeat_messages, timestamp_fmt
    ):
        program_id = "test_controlled_process_single_thread"

        cp = ControlledProcessSingleThreadForTesting(
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
            heartbeat_topic_name=TOPIC_NAME,
            heartbeat_program_id=program_id,
            heartbeat_interval_secs=1,
            logger=logger,
        )

        assert cp.counter == 0

        start_time = datetime.datetime.now()

        run_thread = ExceptionTrackingThread(target=cp.run)
        run_thread.start()

        try:
            time.sleep(3)
            assert not cp.checked

            cp.control_command_queue.put("c")
            cp.control_command_queue.put("check")
            time.sleep(1)

            assert cp.checked
            assert not cp.on_shutdown_called

            cp.control_command_queue.put("q")
            time.sleep(2)

            assert cp.on_shutdown_called

            run_thread.join(timeout=TIMEOUT_SECS)
            time.sleep(1)
            assert not run_thread.is_alive()

            assert cp.counter == 5

            msgs = get_heartbeat_messages(
                TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
                TOPIC_NAME,
                program_id,
            )
            assert len(msgs) > 0

            for msg in msgs:
                payload = json.loads(msg.value())
                ts = datetime.datetime.strptime(payload["timestamp"], timestamp_fmt)
                assert ts > start_time

        finally:
            if run_thread.is_alive():
                cp.shutdown()
                run_thread.join(timeout=5)
                assert not run_thread.is_alive()

    # ---------------------------------------------------------
    def test_multi_thread_heartbeats(self, logger, get_heartbeat_messages, timestamp_fmt):
        program_id = "test_controlled_process_multi_threaded"

        cp = ControlledProcessMultiThreadedForTesting(
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
            heartbeat_topic_name=TOPIC_NAME,
            heartbeat_program_id=program_id,
            heartbeat_interval_secs=1,
            logger=logger,
            n_threads=N_THREADS,
        )

        assert cp.counter == 0
        start_time = datetime.datetime.now()

        run_thread = ExceptionTrackingThread(target=cp.run)
        run_thread.start()

        try:
            time.sleep(3)
            assert not cp.checked

            cp.control_command_queue.put("c")
            cp.control_command_queue.put("check")
            time.sleep(1)

            assert cp.checked
            assert not cp.on_shutdown_called

            cp.control_command_queue.put("q")
            time.sleep(1)

            assert cp.on_shutdown_called

            run_thread.join(timeout=TIMEOUT_SECS)
            assert not run_thread.is_alive()

            assert cp.counter == 5

            msgs = get_heartbeat_messages(
                TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
                TOPIC_NAME,
                program_id,
            )
            assert len(msgs) > 0

            for msg in msgs:
                payload = json.loads(msg.value())
                ts = datetime.datetime.strptime(payload["timestamp"], timestamp_fmt)
                assert ts > start_time

        finally:
            if run_thread.is_alive():
                cp.shutdown()
                run_thread.join(timeout=5)
                assert not run_thread.is_alive()
