# conftest.py (preferred - uses testcontainers)
import shutil
import pytest
import logging
import os
from testcontainers.core.container import DockerContainer
from testcontainers.kafka import KafkaContainer
from openmsitoolbox.logging.openmsi_logger import OpenMSILogger
from openmsistream.utilities.config import RUN_CONST
from test_scripts.config import TEST_CONST

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


@pytest.fixture(autouse=True)
def env_vars_setup(monkeypatch):
    """
    Automatically sets placeholder environment variables required for tests,
    reproducing the old TestWithEnvVars behavior.
    """
    reset_vars = []

    for env_var in TEST_CONST.ENV_VAR_NAMES:
        if os.path.expandvars(f"${env_var}") == f"${env_var}":
            monkeypatch.setenv(env_var, f"PLACEHOLDER_{env_var}")
            reset_vars.append(env_var)

    # No teardown needed — monkeypatch handles reset automatically