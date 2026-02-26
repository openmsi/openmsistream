# conftest.py
import pytest
import logging
import os
import pathlib
import datetime
import time
import shutil
import re
import docker
import requests
import subprocess
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

from kafkacrypto import KafkaCryptoMessage
from testcontainers.kafka import KafkaContainer
from confluent_kafka.admin import AdminClient, NewTopic
from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream import UploadDataFile
from openmsitoolbox.logging.openmsi_logger import OpenMSILogger

from openmsistream.kafka_wrapper import OpenMSIStreamConsumer
from openmsistream.utilities.config import RUN_CONST
from .test_scripts.config import TEST_CONST
from .test_scripts.test_data_file_stream_processor import (
    DataFileStreamProcessorForTesting,
)

################## GENERAL PURPOSE FIXTURES ##################


# pytest hook to attach reports (needed for rep_call)
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


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


@pytest.fixture(scope="session")
def kafka_container():

    flag = os.environ.get("USE_LOCAL_KAFKA_BROKER_IN_TESTS")

    if flag is None:
        raise pytest.UsageError(
            "Environment variable USE_LOCAL_KAFKA_BROKER_IN_TESTS is not set. "
            "Set it to 'yes' or 'no'."
        )

    if flag == "yes":
        container = KafkaContainer("confluentinc/cp-kafka:7.6.0")
        container.start()

        yield container

        # Brief wait for rdkafka's internal C-level background threads to finish
        # reconnection retries before the container is stopped, avoiding spurious
        # "Connection refused" log noise at shutdown.
        time.sleep(3)
        container.stop()
    else:
        yield None


@pytest.fixture(scope="session")
def kafka_bootstrap(kafka_container):

    if os.environ["USE_LOCAL_KAFKA_BROKER_IN_TESTS"] == "yes":
        address = kafka_container.get_bootstrap_server()
        ### For faster/advanced testing, feel free to build PLAINTEXT or SASL broker
        ### via docker compose yaml that launches local plain/ssl kafka broker
        # address = "localhost:9092"
    else:
        address = os.environ["KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS"]
        if address is None:
            raise ValueError(
                "USE_LOCAL_KAFKA_BROKER_IN_TESTS == no, but "
                "KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS has not set."
            )
    print(f"Using broker at {address}...")
    return address


@pytest.fixture(scope="session")
def apply_kafka_env(kafka_bootstrap):
    """
    Uses the *session* container,
    but applies env vars *per test*, avoiding ScopeMismatch.

    """
    if os.environ["USE_LOCAL_KAFKA_BROKER_IN_TESTS"] == "yes":
        os.environ["LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS"] = kafka_bootstrap
    else:
        required_vars = [
            "KAFKA_TEST_CLUSTER_USERNAME",
            "KAFKA_TEST_CLUSTER_PASSWORD",
        ]
        missing = [v for v in required_vars if not os.getenv(v)]

        if missing:
            pytest.fail(
                "Missing required Kafka env vars when using TEST cluster "
                "(inferred from USE_LOCAL_KAFKA_BROKER_IN_TESTS != 'yes'): "
                f"{', '.join(missing)}"
            )

    yield


@pytest.fixture
def kafka_topics(kafka_bootstrap, apply_kafka_env, request):
    topics_dict = request.param
    topic_names = list(topics_dict.keys())

    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})

    # --- CLEANUP BEFORE CREATING ---
    fs = admin.delete_topics(topic_names)
    for _, f in fs.items():
        try:
            f.result()
        except Exception:
            pass  # topic may not exist yet, that's fine

    # --- RECREATE CLEAN TOPICS ---
    new_topics = [
        NewTopic(
            name,
            num_partitions=1,
            replication_factor=1,
            config={"retention.ms": "1"},  # prevent accumulation of old messages
        )
        for name in topic_names
    ]

    # Retry creation: Kafka marks topics for deletion asynchronously, so even after
    # the delete future resolves a topic may still be "marked for deletion" or
    # "already exists" briefly. Only retry the specific topics that failed.
    pending = list(new_topics)
    deadline = time.time() + 30
    while pending:
        fs = admin.create_topics(pending)
        failed = []
        for topic_name, f in fs.items():
            try:
                f.result()
            except Exception as e:
                msg = str(e)
                if "marked for deletion" in msg or "already exists" in msg:
                    failed.append(topic_name)
                else:
                    raise RuntimeError(f"Failed to create topic {topic_name}: {e}") from e
        if not failed:
            break
        if time.time() > deadline:
            raise RuntimeError(
                f"Timed out waiting for topics to become available: {failed}"
            )
        fs = admin.delete_topics(failed)
        for _, f in fs.items():
            try:
                f.result()
            except Exception:
                pass
        pending = [t for t in new_topics if t.topic in failed]
        time.sleep(0.5)

    # brief wait for leader election to settle
    time.sleep(0.5)

    yield topic_names

    # --- CLEANUP AFTER TEST ---
    fs = admin.delete_topics(topic_names)
    for _, f in fs.items():
        try:
            f.result()
        except Exception:
            pass


################## TESTING CODE FIXTURES ##################


@pytest.fixture
def state(tmp_path):
    """A simple mutable dict holding runtime state per test."""
    return {"tmp_path": tmp_path}


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
            f"Quitting stream processor thread after reading "
            f"{state['stream_processor'].n_msgs_read} "
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
def upload_single_file(upload_file_helper):
    """Standalone fixture returning the upload-single-file callable."""
    return upload_file_helper


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


@pytest.fixture
def get_heartbeat_messages(logger):
    """
    Provide a function that retrieves heartbeat messages from Kafka.
    Replaces TestWithHeartbeats.get_heartbeat_messages.
    """

    def _getter(config_path, topic_name, program_id, per_wait_secs=5):
        c_args, c_kwargs = OpenMSIStreamConsumer.get_consumer_args_kwargs(
            config_path,
            logger=logger,
            max_wait_per_decrypt=1,
        )
        consumer = OpenMSIStreamConsumer(
            *c_args,
            **c_kwargs,
            message_key_regex=re.compile(f"{program_id}_heartbeat"),
            filter_new_message_keys=True,
        )

        consumer.subscribe([topic_name])

        msgs = []
        start = datetime.datetime.now()
        cutoff_ms = (time.time() + per_wait_secs) * 1000
        last_timestamp = 0

        while (
            datetime.datetime.now() - start
        ).total_seconds() < per_wait_secs and last_timestamp < cutoff_ms:
            msg = consumer.get_next_message(1)
            if msg:
                try:
                    _, last_timestamp = msg.timestamp()
                except TypeError:
                    _, last_timestamp = msg.timestamp

                if not isinstance(msg.value, KafkaCryptoMessage):
                    msgs.append(msg)
                    start = datetime.datetime.now()

        consumer.close()
        return msgs

    return _getter


@pytest.fixture
def get_log_messages(logger):
    """Provide a callable that retrieves log messages from Kafka."""

    def _getter(config_path, topic_name, program_id, per_wait_secs=30):
        c_args, c_kwargs = OpenMSIStreamConsumer.get_consumer_args_kwargs(
            config_path,
            logger=logger,
            max_wait_per_decrypt=0.1,
        )
        consumer = OpenMSIStreamConsumer(
            *c_args,
            **c_kwargs,
            message_key_regex=re.compile(f"{program_id}_log"),
            filter_new_message_keys=True,
        )
        consumer.subscribe([topic_name])
        msgs = []
        start = datetime.datetime.now()
        cutoff_ms = (time.time() + per_wait_secs) * 1000
        last_timestamp = 0
        while (
            datetime.datetime.now() - start
        ).total_seconds() < per_wait_secs and last_timestamp < cutoff_ms:
            msg = consumer.get_next_message(1)
            if msg:
                try:
                    _, last_timestamp = msg.timestamp()
                except TypeError:
                    _, last_timestamp = msg.timestamp
                if not isinstance(msg.value, KafkaCryptoMessage):
                    msgs.append(msg)
                    start = datetime.datetime.now()
        consumer.close()
        return msgs

    return _getter


################## GIRDER CODE FIXTURES ##################


GIRDER_API_URL = "http://localhost:8080/api/v1"
HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
GIRDER_TIMEOUT = 10


@pytest.fixture(scope="module")
def girder_instance():
    try:
        docker.from_env()
    except docker.errors.DockerException:
        pytest.skip("Docker not running")

    compose_file = TEST_CONST.TEST_DIR_PATH / "local-girder-docker-compose.yml"

    # Tear down any leftover containers from a previous run to ensure a clean DB
    subprocess.call(
        ["docker", "compose", "-f", str(compose_file), "down", "-t", "0"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.check_output(["docker", "compose", "-f", str(compose_file), "up", "-d"])

    # wait for API
    for _ in range(30):
        try:
            r = requests.get(GIRDER_API_URL, timeout=GIRDER_TIMEOUT)
            if r.status_code == 200:
                break
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    else:
        raise RuntimeError("Girder never became available")

    # create admin
    r = requests.post(
        f"{GIRDER_API_URL}/user",
        headers=HEADERS,
        params=dict(
            login="admin",
            email="root@dev.null",
            firstName="John",
            lastName="Doe",
            password="arglebargle123",
            admin=True,
        ),
        timeout=GIRDER_TIMEOUT,
    )

    if r.status_code == 400:
        raise RuntimeError("Girder DB not clean")

    token = r.json()["authToken"]["token"]
    HEADERS["Girder-Token"] = token

    # create assetstore
    r = requests.post(
        f"{GIRDER_API_URL}/assetstore",
        headers=HEADERS,
        params=dict(
            type=0,
            name="Base",
            root="/home/girder/data/base",
        ),
        timeout=GIRDER_TIMEOUT,
    )
    assetstore_id = r.json()["_id"]

    # create API key
    r = requests.post(
        f"{GIRDER_API_URL}/api_key",
        headers=HEADERS,
        timeout=GIRDER_TIMEOUT,
    )
    api_key = r.json()["key"]
    api_key_id = r.json()["_id"]

    yield {
        "api_url": GIRDER_API_URL,
        "api_key": api_key,
        "api_key_id": api_key_id,
        "assetstore_id": assetstore_id,
    }

    subprocess.check_output(
        ["docker", "compose", "-f", str(compose_file), "down", "-t", "0"]
    )


################## MINIO (S3) FIXTURES ##################

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_REGION = "us-east-1"
MINIO_BUCKET = "openmsistream-test"


@pytest.fixture(scope="module")
def minio_instance():
    try:
        docker.from_env()
    except docker.errors.DockerException:
        pytest.skip("Docker not running")

    compose_file = TEST_CONST.TEST_DIR_PATH / "local-minio-docker-compose.yml"

    # Tear down any leftover containers from a previous run
    subprocess.call(
        ["docker", "compose", "-f", str(compose_file), "down", "-t", "0"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.check_output(["docker", "compose", "-f", str(compose_file), "up", "-d"])

    # Wait for MinIO to be reachable
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )

    for _ in range(30):
        try:
            s3.list_buckets()
            break
        except (EndpointConnectionError, ClientError, Exception):
            pass
        time.sleep(1)
    else:
        raise RuntimeError("MinIO never became available")

    # Create test bucket (ignore if it already exists)
    try:
        s3.create_bucket(Bucket=MINIO_BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise

    yield {
        "endpoint_url": MINIO_ENDPOINT,
        "access_key_id": MINIO_ACCESS_KEY,
        "secret_key_id": MINIO_SECRET_KEY,
        "region": MINIO_REGION,
        "bucket_name": MINIO_BUCKET,
    }

    subprocess.check_output(
        ["docker", "compose", "-f", str(compose_file), "down", "-t", "0"]
    )


# mock openmsitoolbox/controlled_process/controlled_process:add_user_input
# to avoid hanging on user input in tests
@pytest.fixture(autouse=True)
def mock_add_user_input(monkeypatch):
    def _mock_add_user_input(self, *args, **kwargs):
        while True:
            time.sleep(0.1)

    monkeypatch.setattr(
        "openmsitoolbox.controlled_process.controlled_process.add_user_input",
        _mock_add_user_input,
    )
