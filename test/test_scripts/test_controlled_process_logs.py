""" Tests for the 'ControlledProcess' classes adapted to include producing log
messages
"""

# imports
import time, json
from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream.utilities.controlled_processes_heartbeats_logs import (
    ControlledProcessSingleThreadHeartbeatsLogs,
    ControlledProcessMultiThreadedHeartbeatsLogs,
)

try:
    from .config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from .base_classes import TestWithLogs
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from base_classes import TestWithLogs

# some constants
TIMEOUT_SECS = 10
N_THREADS = 3


class ControlledProcessSingleThreadForTesting(
    ControlledProcessSingleThreadHeartbeatsLogs
):
    "Utility class for testing the single-threaded controlled process with logs"

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
    "Utility class for testing the multi-threaded controlled process with logs"

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


class TestControlledProcessLogs(TestWithLogs):
    "Tests for the ControlledProcesses that use logs"

    TOPIC_NAME = "logs"
    TOPICS = {TOPIC_NAME: {"--partitions": 1}}

    def test_controlled_process_single_thread_kafka(self):
        "Test the single-thread ControlledProcess with logs"
        program_id = "test_controlled_process_single_thread"
        cpst = ControlledProcessSingleThreadForTesting(
            TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
            log_topic_name=self.TOPIC_NAME,
            log_program_id=program_id,
            log_interval_secs=1,
            logger=self.logger,
        )
        self.assertEqual(cpst.counter, 0)
        start_time = time.time()
        run_thread = ExceptionTrackingThread(target=cpst.run)
        run_thread.start()
        try:
            time.sleep(3.0)
            self.assertFalse(cpst.checked)
            time.sleep(1.0)
            cpst.control_command_queue.put("c")
            cpst.control_command_queue.put("check")
            time.sleep(1.0)
            self.assertTrue(cpst.checked)
            self.assertFalse(cpst.on_shutdown_called)
            cpst.control_command_queue.put("q")
            time.sleep(2.0)
            self.assertTrue(cpst.on_shutdown_called)
            run_thread.join(timeout=TIMEOUT_SECS)
            time.sleep(2.0)
            if run_thread.is_alive():
                errmsg = (
                    "ERROR: running thread in test_controlled_process_single_thread "
                    f"timed out after {TIMEOUT_SECS} seconds!"
                )
                raise TimeoutError(errmsg)
            self.assertEqual(cpst.counter, 5)
            log_msgs = self.get_log_messages(
                TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
                self.TOPIC_NAME,
                program_id,
            )
            self.assertTrue(len(log_msgs) > 0)
            for msg in log_msgs:
                msg_dict = json.loads(msg.value())
                self.assertTrue(float(msg_dict["timestamp"]) >= start_time)
        except Exception as exc:
            raise exc
        finally:
            if run_thread.is_alive():
                try:
                    cpst.shutdown()
                    run_thread.join(timeout=5)
                    if run_thread.is_alive():
                        errmsg = (
                            "ERROR: running thread in test_controlled_process_single_thread "
                            "timed out after 5 seconds!"
                        )
                        raise TimeoutError(errmsg)
                except Exception as exc:
                    raise exc

    def test_controlled_process_multi_threaded_kafka(self):
        "Test the multi-threaded ControlledProcess with logs"
        program_id = "test_controlled_process_multi_threaded"
        cpmt = ControlledProcessMultiThreadedForTesting(
            TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
            log_topic_name=self.TOPIC_NAME,
            log_program_id=program_id,
            log_interval_secs=1,
            logger=self.logger,
            n_threads=N_THREADS,
        )
        self.assertEqual(cpmt.counter, 0)
        start_time = time.time()
        run_thread = ExceptionTrackingThread(target=cpmt.run)
        run_thread.start()
        try:
            time.sleep(3.0)
            self.assertFalse(cpmt.checked)
            time.sleep(0.5)
            cpmt.control_command_queue.put("c")
            cpmt.control_command_queue.put("check")
            time.sleep(0.5)
            self.assertTrue(cpmt.checked)
            self.assertFalse(cpmt.on_shutdown_called)
            cpmt.control_command_queue.put("q")
            time.sleep(1.0)
            self.assertTrue(cpmt.on_shutdown_called)
            run_thread.join(timeout=TIMEOUT_SECS)
            if run_thread.is_alive():
                errmsg = (
                    "ERROR: running thread in test_controlled_process_multi_threaded "
                    f"timed out after {TIMEOUT_SECS} seconds!"
                )
                raise TimeoutError(errmsg)
            self.assertEqual(cpmt.counter, 5)
            log_msgs = self.get_log_messages(
                TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
                self.TOPIC_NAME,
                program_id,
            )
            self.assertTrue(len(log_msgs) > 0)
            for msg in log_msgs:
                msg_dict = json.loads(msg.value())
                self.assertTrue(float(msg_dict["timestamp"]) >= start_time)
        except Exception as exc:
            raise exc
        finally:
            if run_thread.is_alive():
                try:
                    cpmt.shutdown()
                    run_thread.join(timeout=5)
                    if run_thread.is_alive():
                        raise TimeoutError(
                            "ERROR: running thread timed out after 5 seconds!"
                        )
                except Exception as exc:
                    raise exc
