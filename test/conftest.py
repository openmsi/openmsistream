# conftest.py (preferred - uses testcontainers)
import shutil
import pytest
import logging
# from pathlib import Path
from testcontainers.core.container import DockerContainer
from testcontainers.kafka import KafkaContainer
from openmsistream.utilities.config import RUN_CONST

@pytest.fixture
def output_dir(tmp_path):
    """Temporary output directory for test outputs."""
    return tmp_path / "output"

# pytest hook to attach reports (needed for rep_call)
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)

# Kafka fixture: starts a disposable Kafka and returns bootstrap address
@pytest.fixture(scope="session")
def kafka_service():
    """
    Starts Kafka via testcontainers and yields bootstrap server string.
    Session-scoped so tests share the container. Use marker to run these tests only when needed.
    """
    # image selection: choose a suitable confluent image
    with KafkaContainer("confluentinc/cp-kafka:7.3.0") as kafka:
        # KafkaContainer exposes bootstrap_server via get_bootstrap_server
        bootstrap = kafka.get_bootstrap_server()
        yield bootstrap
    # container stops automatically