# test_scripts/test_controlled_process_logs.py

import pytest

from .config import TEST_CONST
from .controlled_process_helpers import (
    ControlledProcessMultiThreadedLogsForTesting,
    ControlledProcessSingleThreadLogsForTesting,
)

TIMEOUT_SECS = 10
N_THREADS = 3


# -------------------------------------------------------------------
#                               TESTS
# -------------------------------------------------------------------


@pytest.mark.kafka
@pytest.mark.parametrize("kafka_topics", [{"logs": {}}], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics", "apply_kafka_env")
def test_controlled_process_single_thread_kafka(
    logger, get_log_messages, run_controlled_process_test
):
    program_id = "test_controlled_process_single_thread"

    cp = ControlledProcessSingleThreadLogsForTesting(
        TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
        log_topic_name="logs",
        log_program_id=program_id,
        log_interval_secs=1,
        logger=logger,
    )
    run_controlled_process_test(
        cp, get_log_messages, program_id, TEST_CONST.TEST_CFG_FILE_PATH_LOGS, "logs"
    )


@pytest.mark.kafka
@pytest.mark.parametrize("kafka_topics", [{"logs": {}}], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics", "apply_kafka_env")
def test_controlled_process_multi_threaded_kafka(
    logger, get_log_messages, run_controlled_process_test
):
    program_id = "test_controlled_process_multi_threaded"

    cp = ControlledProcessMultiThreadedLogsForTesting(
        TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
        log_topic_name="logs",
        log_program_id=program_id,
        log_interval_secs=1,
        logger=logger,
        n_threads=N_THREADS,
    )
    run_controlled_process_test(
        cp, get_log_messages, program_id, TEST_CONST.TEST_CFG_FILE_PATH_LOGS, "logs"
    )
