# conftest.py  # pylint: disable=too-many-lines
import configparser
import datetime
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import time
from datetime import timezone as dtz

import boto3
import dateutil.parser
import msgpack
import pytest
import requests
from botocore.exceptions import ClientError, EndpointConnectionError
from confluent_kafka.admin import AdminClient, NewTopic
from kafkacrypto import KafkaCryptoMessage, KafkaCryptoStore
from kafkacrypto.chain import process_chain
from kafkacrypto.cryptokey import CryptoKey
from kafkacrypto.provisioners import PasswordProvisioner, PasswordROT
from kafkacrypto.ratchet import Ratchet
from kafkacrypto.utils import msgpack_default_pack
from openmsitoolbox.logging.openmsi_logger import OpenMSILogger
from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread
from pysodium import randombytes
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.kafka import KafkaContainer

import docker
from openmsistream import UploadDataFile
from openmsistream.kafka_wrapper import OpenMSIStreamConsumer
from openmsistream.utilities.config import RUN_CONST

from .test_scripts.config import TEST_CONST
from .test_scripts.test_data_file_stream_processor import (
    DataFileStreamProcessorForTesting,
)

################## GENERAL PURPOSE FIXTURES ##################
_GIRDER_IMAGE = "girder/girder:v5.0.0a14-py3"
_GIRDER_INTERNAL_PORT = 8080
HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
GIRDER_TIMEOUT = 10


@pytest.fixture
def mock_env_vars():
    _old_var = os.environ.get("LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS")
    os.environ["LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS"] = "localhost:9092"
    yield
    if _old_var is not None:
        os.environ["LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS"] = _old_var
    else:
        del os.environ["LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS"]


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


@pytest.fixture(scope="session")
def kafka_container():
    container = KafkaContainer("confluentinc/cp-kafka:7.6.0")
    container.start()

    yield container

    # Brief wait for rdkafka's internal C-level background threads to finish
    # reconnection retries before the container is stopped, avoiding spurious
    # "Connection refused" log noise at shutdown.
    time.sleep(3)
    container.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap(kafka_container):
    address = kafka_container.get_bootstrap_server()
    print(f"Using broker at {address}...")
    return address


@pytest.fixture(scope="session")
def apply_kafka_env(kafka_bootstrap):
    """
    Uses the *session* container,
    but applies env vars *per test*, avoiding ScopeMismatch.

    """
    os.environ["LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS"] = kafka_bootstrap
    yield


@pytest.fixture
def kafka_topics(kafka_bootstrap, apply_kafka_env, request):
    topics_dict = request.param
    topic_names = list(topics_dict.keys())

    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})

    fs = admin.delete_topics(topic_names)
    for _, f in fs.items():
        try:
            f.result()
        except Exception:
            pass  # topic may not exist yet, that's fine

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


@pytest.fixture
def state(tmp_path):
    """A simple mutable dict holding runtime state per test."""
    return {"tmp_path": tmp_path}


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
        start_time = datetime.datetime.now(dtz.utc)
        while (
            not all_files_found
            and (datetime.datetime.now(dtz.utc) - start_time).total_seconds()
            < timeout_secs
        ):
            current_messages_read = state["stream_processor"].n_msgs_read
            time_waited = (datetime.datetime.now(dtz.utc) - start_time).total_seconds()
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
            for item in state["output_dir"].iterdir():
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()

    return {
        "create_stream_processor": create_stream_processor,
        "start_stream_processor_thread": start_stream_processor_thread,
        "wait_for_files_to_be_processed": wait_for_files_to_be_processed,
        "reset_stream_processor": reset_stream_processor,
        "state": state,
    }


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


def _get_keyed_messages(
    logger,
    config_path,
    topic_name,
    program_id,
    key_suffix,
    max_wait_per_decrypt,
    per_wait_secs,
):
    """Shared helper: consume messages whose key matches ``{program_id}_{key_suffix}``."""
    c_args, c_kwargs = OpenMSIStreamConsumer.get_consumer_args_kwargs(
        config_path,
        logger=logger,
        max_wait_per_decrypt=max_wait_per_decrypt,
    )
    consumer = OpenMSIStreamConsumer(
        *c_args,
        **c_kwargs,
        message_key_regex=re.compile(f"{program_id}_{key_suffix}"),
        filter_new_message_keys=True,
    )
    consumer.subscribe([topic_name])
    msgs = []
    start = datetime.datetime.now(dtz.utc)
    cutoff_ms = (time.time() + per_wait_secs) * 1000
    last_timestamp = 0
    while (
        datetime.datetime.now(dtz.utc) - start
    ).total_seconds() < per_wait_secs and last_timestamp < cutoff_ms:
        msg = consumer.get_next_message(1)
        if msg is not None:
            try:
                _, last_timestamp = msg.timestamp()
            except TypeError:
                _, last_timestamp = msg.timestamp
            if not isinstance(msg.value, KafkaCryptoMessage):
                msgs.append(msg)
                start = datetime.datetime.now(dtz.utc)
    consumer.close()
    return msgs


@pytest.fixture
def get_heartbeat_messages(logger):
    """Provide a callable that retrieves heartbeat messages from Kafka."""

    def _getter(config_path, topic_name, program_id, per_wait_secs=5):
        return _get_keyed_messages(
            logger, config_path, topic_name, program_id, "heartbeat", 1, per_wait_secs
        )

    return _getter


@pytest.fixture
def get_log_messages(logger):
    """Provide a callable that retrieves log messages from Kafka."""

    def _getter(config_path, topic_name, program_id, per_wait_secs=30):
        return _get_keyed_messages(
            logger, config_path, topic_name, program_id, "log", 0.1, per_wait_secs
        )

    return _getter


@pytest.fixture
def run_controlled_process_test():
    def _run_controlled_process_test(cp, getter, program_id, config, topic_name):
        assert cp.counter == 0
        start_time = datetime.datetime.now(dtz.utc)

        run_thread = ExceptionTrackingThread(target=cp.run)
        run_thread.start()

        try:
            # let it run and increment its counter a few times
            counter = 0
            while cp.counter < 5 and counter < 10:
                time.sleep(0.1)
                counter += 1
            assert not cp.checked

            # send check
            cp.control_command_queue.put("c")
            cp.control_command_queue.put("check")
            counter = 0
            while not cp.checked and counter < 10:
                time.sleep(0.1)
                counter += 1

            assert cp.checked
            assert not cp.on_shutdown_called

            # shutdown
            cp.control_command_queue.put("q")
            counter = 0
            while not cp.on_shutdown_called and counter < 20:
                time.sleep(0.1)
                counter += 1
            assert cp.on_shutdown_called

            run_thread.join(timeout=5)

            if run_thread.is_alive():
                raise TimeoutError("ERROR: running thread timed out after 5 seconds!")

            assert cp.counter == 5

            # ---- Retrieve logs ----
            log_msgs = getter(
                config,
                topic_name,
                program_id,
                per_wait_secs=4,
            )

            assert len(log_msgs) > 0

            for msg in log_msgs:
                payload = json.loads(msg.value())
                try:
                    ts = float(payload["timestamp"])
                    assert ts > start_time.timestamp()
                except ValueError:
                    ts = dateutil.parser.parse(payload["timestamp"])
                    assert ts > start_time

        finally:
            if run_thread.is_alive():
                cp.shutdown()
                run_thread.join(timeout=5)
                if run_thread.is_alive():
                    raise TimeoutError(
                        "ERROR: running thread timed out after forced shutdown!"
                    )

    return _run_controlled_process_test


@pytest.fixture(scope="module")
def girder_instance(request):
    """
    Spin up a Girder stack (MongoDB + Redis + Girder) using testcontainers.

    Pass a list of extra pip packages to install into the Girder image before
    it starts by parametrizing indirectly::

        @pytest.mark.parametrize("girder_instance", [["my_plugin"]], indirect=True)
        def test_with_plugin(girder_instance): ...
    """
    extra_plugins = list(getattr(request, "param", None) or [])

    try:
        docker.from_env()
    except docker.errors.DockerException:
        pytest.skip("Docker not running")

    network = Network()

    mongo = (
        DockerContainer("mongo:4.4").with_network(network).with_network_aliases("mongodb")
    )
    redis_ct = (
        DockerContainer("redis:latest")
        .with_network(network)
        .with_network_aliases("redis")
    )
    girder_ct = (
        DockerContainer(_GIRDER_IMAGE)
        .with_network(network)
        .with_network_aliases("girder")
        .with_exposed_ports(_GIRDER_INTERNAL_PORT)
        .with_env("GIRDER_MONGO_URI", "mongodb://mongodb:27017/girder")
        .with_env("GIRDER_HOST", "0.0.0.0")
        .with_env("GIRDER_WORKER_BROKER", "redis://redis/")
        .with_env("GIRDER_WORKER_BACKEND", "redis://redis/")
        .with_env("GIRDER_NOTIFICATION_REDIS_URL", "redis://redis:6379")
    )
    if extra_plugins:
        plugins_str = " ".join(extra_plugins)
        girder_ct = girder_ct.with_kwargs(entrypoint="bash").with_command(
            ["-c", f"pip install {plugins_str} && girder serve -H 0.0.0.0"]
        )
    else:
        girder_ct = girder_ct.with_command(["-H", "0.0.0.0"])

    with network:
        mongo.start()
        redis_ct.start()
        girder_ct.start()

        try:
            host_port = girder_ct.get_exposed_port(_GIRDER_INTERNAL_PORT)
            api_url = f"http://localhost:{host_port}/api/v1"

            # Wait for the API to become available
            for _ in range(60):
                try:
                    r = requests.get(api_url, timeout=GIRDER_TIMEOUT)
                    if r.status_code == 200:
                        break
                except requests.exceptions.ConnectionError:
                    pass
                time.sleep(1)
            else:
                raise RuntimeError("Girder never became available")

            # Create admin user
            r = requests.post(
                f"{api_url}/user",
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
            headers = {**HEADERS, "Girder-Token": token}

            # Create assetstore
            r = requests.post(
                f"{api_url}/assetstore",
                headers=headers,
                params=dict(
                    type=0,
                    name="Base",
                    root="/home/girder/data/base",
                ),
                timeout=GIRDER_TIMEOUT,
            )
            assetstore_id = r.json()["_id"]

            # Create API key
            r = requests.post(
                f"{api_url}/api_key",
                headers=headers,
                timeout=GIRDER_TIMEOUT,
            )
            api_key = r.json()["key"]
            api_key_id = r.json()["_id"]

            yield {
                "api_url": api_url,
                "api_key": api_key,
                "api_key_id": api_key_id,
                "assetstore_id": assetstore_id,
            }
        finally:
            girder_ct.stop()
            redis_ct.stop()
            mongo.stop()


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


### Encrypted Kafka setup ###


@pytest.fixture(scope="session")
def secure_kafka_container(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("kafka_security")
    # Fix directory permissions for Docker traversal
    tmp_dir.chmod(0o755)

    jaas_file = tmp_dir / "kafka_server_jaas.conf"
    keystore_file = tmp_dir / "server.keystore.jks"
    truststore_file = tmp_dir / "server.truststore.jks"
    password = "test-password"

    # Confluent script requirement: dummy file named the same as the password
    dummy_password_file = tmp_dir / password
    dummy_password_file.write_text(password)

    # 1. Create JAAS Config
    # Added 'user_admin' to ensure the 'admin' principal exists in the PLAIN mechanism
    jaas_file.write_text(
        f"""
    KafkaServer {{
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="admin"
        password="{password}"
        user_admin="{password}"
        user_alice="alice-password";
    }};
    """
    )

    # 2. Generate SSL Assets
    # Generate Keystore
    cmd = (
        f"keytool -genkey -keystore {str(keystore_file)} -alias localhost "
        f"-validity 365 -keyalg RSA -storepass {password} -keypass {password} "
        "-dname CN=localhost -noprompt"
    )
    subprocess.run(cmd.split(), check=True)

    # Export Cert for Truststore and Client PEM
    cert_file = tmp_dir / "server.crt"
    cmd = (
        f"keytool -exportcert -keystore {str(keystore_file)} -alias localhost "
        f"-file {str(cert_file)} -storepass {password} -noprompt"
    )
    subprocess.run(cmd.split(), check=True)

    # Create Truststore
    cmd = (
        f"keytool -genkey -keystore {str(truststore_file)} -alias temp "
        f"-validity 365 -keyalg RSA -storepass {password} -keypass {password} "
        "-dname CN=temp -noprompt"
    )
    subprocess.run(cmd.split(), check=True)

    # Set file permissions for the container user
    for f in [jaas_file, keystore_file, truststore_file, dummy_password_file]:
        f.chmod(0o777)

    # 3. Define Containers
    network = Network()
    zookeeper = (
        DockerContainer("confluentinc/cp-zookeeper:7.3.0")
        .with_name("zksecure")
        .with_network(network)
        .with_env("ZOOKEEPER_CLIENT_PORT", "2181")
        .with_env("ZOOKEEPER_TICK_TIME", "2000")
    )

    broker = (
        DockerContainer("confluentinc/cp-kafka:7.3.0")
        .with_name("kafkasecure")
        .with_network(network)
        .with_bind_ports(9093, 9093)
        .with_volume_mapping(str(jaas_file), "/etc/kafka/kafka_server_jaas.conf")
        .with_volume_mapping(str(tmp_dir), "/etc/kafka/secrets", mode="rw")
        # General Config
        .with_env("KAFKA_BROKER_ID", "1")
        # Increase the ZK connection timeout in case the network bridge is slow
        .with_env("KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS", "30000")
        # Disable the Confluent "Self-Help" health checks that can hang
        .with_env("KAFKA_CONFLUENT_BALANCER_ENABLE", "false")
        .with_env("KAFKA_ZOOKEEPER_CONNECT", "zksecure:2181")
        # Critical: Disable SASL for Zookeeper connections (applies to all utilities)
        .with_env("ZOOKEEPER_SASL_ENABLED", "false")
        # Listeners: External uses SASL_SSL, Internal uses PLAINTEXT to avoid handshake hangs
        .with_env("KAFKA_LISTENERS", "SASL_SSL://0.0.0.0:9093,PLAINTEXT://0.0.0.0:9092")
        .with_env(
            "KAFKA_ADVERTISED_LISTENERS",
            "SASL_SSL://localhost:9093,PLAINTEXT://kafkasecure:9092",
        )
        .with_env(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "SASL_SSL:SASL_SSL,PLAINTEXT:PLAINTEXT",
        )
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        # SASL Config
        .with_env("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
        .with_env("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
        # SSL Config - Explicit Locations
        .with_env("KAFKA_SSL_KEYSTORE_LOCATION", "/etc/kafka/secrets/server.keystore.jks")
        .with_env(
            "KAFKA_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/server.truststore.jks"
        )
        .with_env("KAFKA_SSL_KEYSTORE_FILENAME", "server.keystore.jks")
        .with_env("KAFKA_SSL_TRUSTSTORE_FILENAME", "server.truststore.jks")
        # Passwords & Credentials (DUB logic bypass)
        .with_env("KAFKA_SSL_KEY_CREDENTIALS", password)
        .with_env("KAFKA_SSL_KEYSTORE_CREDENTIALS", password)
        .with_env("KAFKA_SSL_TRUSTSTORE_CREDENTIALS", password)
        .with_env("KAFKA_SSL_KEYSTORE_PASSWORD", password)
        .with_env("KAFKA_SSL_KEY_PASSWORD", password)
        .with_env("KAFKA_SSL_TRUSTSTORE_PASSWORD", password)
        # JVM Options: Disable ZK SASL and point to JAAS
        .with_env(
            "KAFKA_OPTS",
            (
                "-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf "
                "-Dzookeeper.sasl.client=false"
            ),
        )
        # Single Node Optimization
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
    )

    # 4. Lifecycle Management
    with network:
        with zookeeper:
            with broker:
                # Use a loop with log printing for easier debugging if it hangs
                print("Container started. Waiting for Kafka 'started' signal...")
                wait_for_logs(broker, r".*\[KafkaServer id=\d+\] started.*", timeout=90)

                # Export PEM for Python client (Confluent-Kafka needs PEM)
                truststore_pem = tmp_dir / "server.truststore.pem"
                subprocess.run(
                    [
                        "keytool",
                        "-exportcert",
                        "-rfc",
                        "-keystore",
                        str(keystore_file),
                        "-alias",
                        "localhost",
                        "-file",
                        str(truststore_pem),
                        "-storepass",
                        password,
                    ],
                    check=True,
                )

                broker.truststore_pem = str(truststore_pem)
                broker.sasl_password = password
                yield broker


@pytest.fixture(scope="session")
def kafka_conf(secure_kafka_container):
    return {
        "bootstrap.servers": "localhost:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "admin",
        "sasl.password": secure_kafka_container.sasl_password,
        "ssl.ca.location": secure_kafka_container.truststore_pem,
        "ssl.endpoint.identification.algorithm": "none",
    }


@pytest.fixture
def kafka_config_file(tmp_path, kafka_conf):
    def _kafka_config_file(node_id=None):
        if node_id is None:
            raise ValueError("node_id must be provided to kafka_config_file")
        # 1. Define your configuration as a nested dictionary
        config_dict = {
            "kafkacrypto": {"node_id": node_id},
            "broker": {
                "bootstrap.servers": kafka_conf["bootstrap.servers"],
                "security.protocol": kafka_conf["security.protocol"],
                "sasl.mechanism": kafka_conf["sasl.mechanism"],
                "sasl.username": kafka_conf["sasl.username"],
                "sasl.password": kafka_conf["sasl.password"],
                # confluent-kafka naming (for Producer/Consumer/KafkaCrypto)
                "ssl.ca.location": kafka_conf["ssl.ca.location"],
                "ssl.endpoint.identification.algorithm": kafka_conf[
                    "ssl.endpoint.identification.algorithm"
                ],
            },
            "producer": {
                "batch.size": "200000",
                "linger.ms": "100",
                "compression.type": "lz4",
                "key.serializer": "StringSerializer",
                "value.serializer": "DataFileChunkSerializer",
            },
            "consumer": {
                "group.id": "create_new",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "False",
                "fetch.min.bytes": "100000",
                "key.deserializer": "StringDeserializer",
                "value.deserializer": "DataFileChunkDeserializer",
            },
        }

        # 2. Initialize ConfigParser and load the dictionary
        config = configparser.ConfigParser()
        config.read_dict(config_dict)

        # 3. Define path and write to file
        config_path = tmp_path / f"test_{node_id}.config"
        with open(config_path, "w") as configfile:
            config.write(configfile)

        return str(config_path)

    return _kafka_config_file


@pytest.fixture()
def encrypted_kafka_node_config():
    def _encrypted_kafka_node_config(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        config_file, node_id, topic, rot_password, password, keytype, key
    ):
        base_path = pathlib.Path(config_file).parent / node_id
        base_path.mkdir(exist_ok=True)
        base_path = base_path / node_id

        _lifetime = 316224000  # lifetime (10 years)
        _usages = {
            "producer": ["key-encrypt"],
            "consumer": ["key-encrypt-request", "key-encrypt-subscribe"],
            "prodcon": ["key-encrypt", "key-encrypt-request", "key-encrypt-subscribe"],
            "prodcon-limited": ["key-encrypt", "key-encrypt-subscribe"],
            "consumer-limited": ["key-encrypt-subscribe"],
            "controller": ["key-encrypt-request"],
        }
        _keys = {
            "producer": "producer",
            "consumer": "consumer",
            "prodcon": "prodcon",
            "prodcon-limited": "prodcon",
            "consumer-limited": "consumer",
            "controller": "consumer",
        }

        rot = PasswordROT(rot_password, keytype)
        _msgrot = msgpack.packb(
            [0, b"\x90", rot._pk], default=msgpack_default_pack, use_bin_type=True
        )
        _chainrot = rot._pk
        _msgchainrot = _msgrot
        prov = PasswordProvisioner(password, rot._pk, keytype)

        _signedprov = {
            "producer": None,
            "consumer": None,
            "prodcon": None,
        }
        for kn in _signedprov:
            k = prov._pk[kn]
            poison = msgpack.packb(
                [["usages", _usages[kn]]],
                default=msgpack_default_pack,
                use_bin_type=True,
            )
            tosign = msgpack.packb(
                [0, poison, k], default=msgpack_default_pack, use_bin_type=True
            )
            _signedprov[kn] = rot._sk.crypto_sign(tosign)

        _msgchains = {
            k: msgpack.packb([_signedprov[v]], use_bin_type=True)
            for k, v in _keys.items()
        }

        sole = False
        assert key in _keys, "Invalid key type specified!"

        # Check we have appropriate chains
        if key == "controller":
            _msgchkrot = _msgrot  # Controllers must be signed by ROT
        else:
            _msgchkrot = _msgchainrot  # Everyone else by Chain ROT (may = ROT)
        assert len(_msgchains[key]) > 0
        pk = process_chain(_msgchains[key], None, None, allowlist=[_msgchkrot])[0]
        assert len(pk) >= 3 and pk[2] == prov._pk[_keys[key]]

        topics = None
        while topics is None:
            topics = list(set(map(lambda i: i.split(".", 1)[0], topic.split())))
            if len(topics) < 1:
                ans = input("Are you sure you want to allow all topics (Y/n)?")
                if len(ans) > 0 and (ans[0].lower() == "n"):
                    topics = None
                else:
                    topics = ["^.*$"]

        pathlen = -1
        with open(base_path.with_suffix(".seed"), "wb") as f:
            seedidx = 0
            rb = randombytes(Ratchet.SECRETSIZE)
            f.write(msgpack.packb([seedidx, rb], use_bin_type=True))

        eck = CryptoKey(base_path.with_suffix(".crypto").as_posix(), keytypes=[keytype])
        for idx in range(0, eck.get_num_spk()):
            if eck.get_spk(idx).same_type(keytype):
                break
        pk = eck.get_spk(idx)

        poison = [["usages", _usages[key]]]
        if len(topics) > 0:
            poison.append(["topics", topics])
        if pathlen != -1:
            poison.append(["pathlen", pathlen])
        poison = msgpack.packb(poison, default=msgpack_default_pack, use_bin_type=True)
        msg = [time.time() + _lifetime, poison, pk]
        msg = prov._sk[_keys[key]].crypto_sign(
            msgpack.packb(msg, default=msgpack_default_pack, use_bin_type=True)
        )
        chain = msgpack.packb(
            msgpack.unpackb(_msgchains[key], raw=False) + [msg],
            default=msgpack_default_pack,
            use_bin_type=True,
        )

        kcs = KafkaCryptoStore(base_path.with_suffix(".config").as_posix(), node_id)
        kcs.store_value("maxage", _lifetime, section="crypto")
        kcs.store_value("chain" + str(idx), chain, section="chains")
        kcs.store_value("rot" + str(idx), _msgrot, section="allowlist")
        if _msgchkrot != _msgrot:
            kcs.store_value("chainrot" + str(idx), _msgchkrot, section="allowlist")
        if kcs.load_value("temporary", section="allowlist"):
            kcs.store_value("temporary", None, section="allowlist")
        # If controller, list of provisioners
        if key == "controller" and _msgchainrot != _msgrot and _msgchainrot != _msgchkrot:
            kcs.store_value("provisioners" + str(idx), _msgchainrot, section="allowlist")
        if kcs.load_value("cryptokey") is None:
            kcs.store_value("cryptokey", "file#" + node_id + ".crypto")
        if kcs.load_value("ratchet") is None:
            kcs.store_value("ratchet", "file#" + node_id + ".seed")
        if key == "producer" or key.startswith("prodcon"):
            if sole:
                kcs.store_value("mgmt_long_keyindex", False)
            else:
                kcs.store_value("mgmt_long_keyindex", True)
        kcs.store_value(
            "crypto_sub_interval", 5
        )  # Decrease crypto sub interval for testing

    return _encrypted_kafka_node_config
