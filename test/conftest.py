# conftest.py (preferred - uses testcontainers)
import shutil
import pytest
import logging
import os
from testcontainers.kafka import KafkaContainer
from openmsitoolbox.logging.openmsi_logger import OpenMSILogger
from confluent_kafka.admin import AdminClient, NewTopic
from openmsistream.utilities.config import RUN_CONST
from test_scripts.config import TEST_CONST

@pytest.fixture
def output_dir(tmp_path):
    """Temporary output directory for test outputs."""
    return tmp_path / "output"


@pytest.fixture
def logger():
    """Provide a real OpenMSILogger for tests."""
    test_logger = OpenMSILogger(
        logger_name="pytest_test",
        streamlevel=logging.DEBUG,
        logger_filepath=None,  # no file logging
        filelevel=logging.DEBUG,
        conf_global_logger=False,  # prevents hijacking all logging during tests
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


@pytest.fixture(scope="function", autouse=True)
def apply_kafka_env(monkeypatch, kafka_container):
    """
    Uses the *session* container,
    but applies env vars *per test*, avoiding ScopeMismatch.
    """
    bootstrap = kafka_container.get_bootstrap_server()

    monkeypatch.setenv("KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS", bootstrap)
    monkeypatch.setenv("KAFKA_TEST_CLUSTER_USERNAME", "testuser")
    monkeypatch.setenv("KAFKA_TEST_CLUSTER_PASSWORD", "testpass")


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

