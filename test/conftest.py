# conftest.py
import pytest
import logging
import os
import pathlib
import datetime
from testcontainers.kafka import KafkaContainer
from openmsitoolbox.logging.openmsi_logger import OpenMSILogger
from confluent_kafka.admin import AdminClient, NewTopic
from openmsistream.utilities.config import RUN_CONST
from test_scripts.config import TEST_CONST
import time
from openmsistream.data_file_io.actor.data_file_upload_directory import (
    DataFileUploadDirectory,
)
from openmsistream.data_file_io.actor.data_file_download_directory import (
    DataFileDownloadDirectory,
)
from test_scripts.test_data_file_stream_processor import DataFileStreamProcessorForTesting
from openmsistream import UploadDataFile


@pytest.fixture
def output_dir(tmp_path):
    d = tmp_path / "output"
    d.mkdir()
    return d


@pytest.fixture
def logger():
    """Provide a real OpenMSILogger for tests."""
    test_logger = OpenMSILogger(
        logger_name="pytest_test",
        streamlevel=logging.DEBUG,
        logger_filepath=None,  # no file logging
        filelevel=logging.DEBUG,
        conf_global_logger=False,
    )
    return test_logger


# pytest hook to attach reports (needed for rep_call)
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture(scope="session")
def kafka_container():
    username = os.getenv("KAFKA_TEST_CLUSTER_USERNAME", "testuser")
    password = os.getenv("KAFKA_TEST_CLUSTER_PASSWORD", "testpass")

    container = KafkaContainer("confluentinc/cp-kafka:7.6.0")
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="session", autouse=True)
def apply_kafka_env(kafka_container):
    """
    Uses the *session* container,
    but applies env vars *per test*, avoiding ScopeMismatch.
    """
    bootstrap = kafka_container.get_bootstrap_server()

    os.environ["KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS"] = bootstrap
    os.environ["KAFKA_TEST_CLUSTER_USERNAME"] = "testuser"
    os.environ["KAFKA_TEST_CLUSTER_PASSWORD"] = "testpass"

    yield


@pytest.fixture
def kafka_topics(kafka_container, request):
    # Expecting tests to define request.param = TOPICS
    topics_dict = request.param

    admin = AdminClient({"bootstrap.servers": kafka_container.get_bootstrap_server()})

    topics = [
        NewTopic(name, num_partitions=1, replication_factor=1)
        for name in topics_dict.keys()
    ]

    admin.create_topics(topics)
    yield list(topics_dict.keys())
    admin.delete_topics(list(topics_dict.keys()))


@pytest.fixture
def state(tmp_path):
    """A simple mutable dict holding runtime state per test."""
    return {}


# -------------------
# Stream processor fixture
# -------------------
@pytest.fixture
def stream_processor_helper(tmp_path, logger):
    """
    Fixture that replicates TestWithStreamProcessor exactly,
    returning helper functions for pytest.
    """
    state = {
        "output_dir": tmp_path,
        "stream_processor": None,
        "stream_processor_thread": None,
        "logger": logger,
    }

    def create_stream_processor(
        stream_processor_type=DataFileStreamProcessorForTesting,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name="test",
        output_dir=None,
        n_threads=RUN_CONST.N_DEFAULT_DOWNLOAD_THREADS,
        consumer_group_id="create_new",
        other_init_args=(),
        other_init_kwargs=None,
    ):
        if state["stream_processor"] is not None:
            raise RuntimeError(
                f"ERROR: stream processor is {state['stream_processor']} but should be None!"
            )
        if output_dir is None:
            output_dir = state["output_dir"]
        if not other_init_kwargs:
            other_init_kwargs = {}
        state["stream_processor"] = stream_processor_type(
            *other_init_args,
            cfg_file,
            topic_name,
            output_dir=output_dir,
            n_threads=n_threads,
            consumer_group_id=consumer_group_id,
            logger=state["logger"],
            **other_init_kwargs,
        )

    def start_stream_processor_thread(func=None, args=(), kwargs=None):
        if state["stream_processor_thread"] is not None:
            errmsg = (
                f"ERROR: stream processor thread is {state['stream_processor_thread']} "
                "but it should be None!"
            )
            raise RuntimeError(errmsg)
        if func is None:
            func = state["stream_processor"].process_files_as_read
        if not kwargs:
            kwargs = {}
        state["stream_processor_thread"] = ExceptionTrackingThread(
            target=func, args=args, kwargs=kwargs
        )
        state["stream_processor_thread"].start()

    def wait_for_files_to_be_processed(rel_filepaths, timeout_secs=90):
        if isinstance(rel_filepaths, pathlib.PurePath):
            rel_filepaths = [rel_filepaths]

        files_found_by_path = {fp: False for fp in rel_filepaths}

        state["logger"].info(
            f"Waiting to process files; will timeout after {timeout_secs} seconds..."
        )
        all_files_found = False
        start_time = datetime.datetime.now()
        while (
            not all_files_found
            and (datetime.datetime.now() - start_time).total_seconds() < timeout_secs
        ):
            current_messages_read = state["stream_processor"].n_msgs_read
            time_waited = (datetime.datetime.now() - start_time).total_seconds()
            state["logger"].info(
                f"\t{current_messages_read} messages read after {time_waited:.2f} seconds...."
            )
            time.sleep(5)
            for rel_fp, found_file in files_found_by_path.items():
                if found_file:
                    continue
                if rel_fp in state["stream_processor"].recent_processed_filepaths:
                    files_found_by_path[rel_fp] = True
            all_files_found = sum(files_found_by_path.values()) == len(rel_filepaths)

        msg = (
            f"Quitting stream processor thread after reading {state['stream_processor'].n_msgs_read} "
            "messages; will timeout after 30 seconds...."
        )
        state["logger"].info(msg)
        state["stream_processor"].control_command_queue.put("q")
        state["stream_processor_thread"].join(timeout=30)
        if state["stream_processor_thread"].is_alive():
            raise TimeoutError(
                "ERROR: stream processor thread timed out after 30 seconds!"
            )

    def reset_stream_processor(remove_output=False):
        sp_thread = state["stream_processor_thread"]
        sp = state["stream_processor"]
        if sp_thread and sp:
            if sp_thread.is_alive():
                try:
                    sp.shutdown()
                    sp_thread.join(timeout=30)
                    if sp_thread.is_alive():
                        raise TimeoutError("Download thread timed out after 30 seconds")
                except Exception as exc:
                    raise exc
        state["stream_processor"] = None
        state["stream_processor_thread"] = None
        if remove_output:
            shutil.rmtree(state["output_dir"])
            state["output_dir"].mkdir(parents=True)

    return {
        "create_stream_processor": create_stream_processor,
        "start_stream_processor_thread": start_stream_processor_thread,
        "wait_for_files_to_be_processed": wait_for_files_to_be_processed,
        "reset_stream_processor": reset_stream_processor,
        "state": state,
    }


# -------------------
# Upload single file fixture
# -------------------
@pytest.fixture
def upload_file_helper(logger):
    """
    Fixture that replicates TestWithUploadDataFile exactly
    """

    def upload_single_file(
        filepath,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name="test",
        rootdir=None,
        n_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS,
        chunk_size=TEST_CONST.TEST_CHUNK_SIZE,
    ):
        upload_datafile = UploadDataFile(filepath, rootdir=rootdir, logger=logger)
        upload_datafile.upload_whole_file(
            cfg_file,
            topic_name,
            n_threads=n_threads,
            chunk_size=chunk_size,
        )

    return upload_single_file
