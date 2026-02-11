# test_scripts/test_controlled_process_logs.py

import time
import json
import pytest
import re
import datetime
from openmsistream.kafka_wrapper import OpenMSIStreamConsumer

from kafkacrypto import KafkaCryptoMessage

from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream.utilities.controlled_processes_heartbeats_logs import (
    ControlledProcessSingleThreadHeartbeatsLogs,
    ControlledProcessMultiThreadedHeartbeatsLogs,
)

from .config import TEST_CONST

TIMEOUT_SECS = 10
N_THREADS = 3


@pytest.fixture
def get_log_messages(logger):
    """Provide a callable that retrieves all log messages for a program."""

    def _get(config_path, log_topic_name, program_id, per_wait_secs=30):

        c_args, c_kwargs = OpenMSIStreamConsumer.get_consumer_args_kwargs(
            config_path,
            logger=logger,
            max_wait_per_decrypt=0.1,
        )
        log_consumer = OpenMSIStreamConsumer(
            *c_args,
            **c_kwargs,
            message_key_regex=re.compile(f"{program_id}_log"),
            filter_new_message_keys=True,
        )
        log_consumer.subscribe([log_topic_name])

        log_msgs = []
        start_time = datetime.datetime.now()
        cutoff_time = (time.time() + per_wait_secs) * 1000

        last_msg_time = 0

        while (
            datetime.datetime.now() - start_time
        ).total_seconds() < per_wait_secs and last_msg_time < cutoff_time:

            msg = log_consumer.get_next_message(1)

            if msg is not None:
                try:
                    _, last_msg_time = msg.timestamp()
                except TypeError:
                    _, last_msg_time = msg.timestamp

                if not isinstance(msg.value, KafkaCryptoMessage):
                    log_msgs.append(msg)
                    start_time = datetime.datetime.now()

        log_consumer.close()
        return log_msgs

    return _get


class ControlledProcessSingleThreadForTesting(
    ControlledProcessSingleThreadHeartbeatsLogs
):
    """Single-thread CP with instrumentation for tests."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_log_producer("Separate")
        self.counter = 0
        self.checked = False
        self.on_shutdown_called = False

    def _on_check(self):
        self.checked = True

    def _on_shutdown(self):
        super()._on_shutdown()
        self.on_shutdown_called = True

    def _run_iteration(self):
        if self.counter < 5:
            self.counter += 1


class ControlledProcessMultiThreadedForTesting(
    ControlledProcessMultiThreadedHeartbeatsLogs
):
    """Multi-thread CP with instrumentation for tests."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_log_producer("Separate")
        self.counter = 0
        self.checked = False
        self.on_shutdown_called = False

    def _on_check(self):
        self.checked = True

    def _on_shutdown(self):
        super()._on_shutdown()
        self.on_shutdown_called = True

    def _run_worker(self):
        while self.alive:
            if self.counter < 5:
                with self.lock:
                    self.counter += 1


# -------------------------------------------------------------------
#                               TESTS
# -------------------------------------------------------------------


@pytest.mark.parametrize("kafka_topics", [{"logs": {}}], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics", "apply_kafka_env")
def test_controlled_process_single_thread_kafka(logger, get_log_messages):

    program_id = "test_controlled_process_single_thread"

    cp = ControlledProcessSingleThreadForTesting(
        TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
        log_topic_name="logs",
        log_program_id=program_id,
        log_interval_secs=1,
        logger=logger,
    )

    assert cp.counter == 0
    start_time = time.time()

    run_thread = ExceptionTrackingThread(target=cp.run)
    run_thread.start()

    try:
        # let it run and increment its counter a few times
        time.sleep(3.0)
        assert cp.checked is False

        # send check
        time.sleep(1.0)
        cp.control_command_queue.put("c")
        cp.control_command_queue.put("check")
        time.sleep(1.0)
        assert cp.checked is True

        assert cp.on_shutdown_called is False

        # shutdown
        cp.control_command_queue.put("q")
        time.sleep(2.0)
        assert cp.on_shutdown_called is True

        run_thread.join(timeout=TIMEOUT_SECS)
        time.sleep(1.0)  # same delay your unittest had

        if run_thread.is_alive():
            raise TimeoutError(
                f"ERROR: running thread timed out after {TIMEOUT_SECS} seconds!"
            )

        assert cp.counter == 5

        # ---- Retrieve logs ----
        log_msgs = get_log_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
            "logs",
            program_id,
        )

        assert len(log_msgs) > 0

        for msg in log_msgs:
            payload = json.loads(msg.value())
            assert float(payload["timestamp"]) >= start_time

    finally:
        if run_thread.is_alive():
            cp.shutdown()
            run_thread.join(timeout=5)
            if run_thread.is_alive():
                raise TimeoutError(
                    "ERROR: running thread timed out after forced shutdown!"
                )


@pytest.mark.parametrize("kafka_topics", [{"logs": {}}], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics", "apply_kafka_env")
def test_controlled_process_multi_threaded_kafka(logger, get_log_messages):
    program_id = "test_controlled_process_multi_threaded"

    cp = ControlledProcessMultiThreadedForTesting(
        TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
        log_topic_name="logs",
        log_program_id=program_id,
        log_interval_secs=1,
        logger=logger,
        n_threads=N_THREADS,
    )

    assert cp.counter == 0
    start_time = time.time()

    run_thread = ExceptionTrackingThread(target=cp.run)
    run_thread.start()

    try:
        time.sleep(3.0)
        assert cp.checked is False

        cp.control_command_queue.put("c")
        cp.control_command_queue.put("check")
        time.sleep(0.5)
        assert cp.checked is True
        assert cp.on_shutdown_called is False

        cp.control_command_queue.put("q")
        time.sleep(1.0)
        assert cp.on_shutdown_called is True

        run_thread.join(timeout=TIMEOUT_SECS)

        if run_thread.is_alive():
            raise TimeoutError(
                f"ERROR: running thread timed out after {TIMEOUT_SECS} seconds!"
            )

        assert cp.counter == 5

        # ---- Retrieve logs ----
        log_msgs = get_log_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
            "logs",
            program_id,
        )

        assert len(log_msgs) > 0

        for msg in log_msgs:
            payload = json.loads(msg.value())
            assert float(payload["timestamp"]) >= start_time

    finally:
        if run_thread.is_alive():
            cp.shutdown()
            run_thread.join(timeout=5)
            if run_thread.is_alive():
                raise TimeoutError(
                    "ERROR: running thread timed out after forced shutdown!"
                )
