import pathlib
import platform
import time
import pytest
from subprocess import check_output

from openmsistream.utilities.config import RUN_CONST
from openmsistream.services.config import SERVICE_CONST
from openmsistream.services.utilities import run_cmd_in_subprocess
from openmsistream.services.windows_service_manager import WindowsServiceManager
from openmsistream.services.linux_service_manager import LinuxServiceManager
from openmsistream.services.install_service import main as install_service_main
from openmsistream.services.manage_service import main as manage_service_main
from openmsistream.services.examples import runnable_example

try:
    from .config import TEST_CONST
except ImportError:
    from config import TEST_CONST

SKIP_CLASS_NAMES = ["GirderUploadStreamProcessor"]


DEF_TOPIC_NAME = RUN_CONST.DEFAULT_TOPIC_NAME
S3_TOPIC_NAME = "test_s3_transfer_stream_processor"
TOPICS = {DEF_TOPIC_NAME: {}, S3_TOPIC_NAME: {}}


# ---------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------
def assert_error_log_empty(service_name):
    error_log_path = (
        pathlib.Path().resolve() / f"{service_name}{SERVICE_CONST.ERROR_LOG_STEM}"
    )
    if error_log_path.is_file():
        with open(error_log_path, "r") as f:
            contents = f.read()
        raise AssertionError(f"Service {service_name} logged errors:\n\n{contents}")


# ---------------------------------------------------------------------
# Shared args setup
# ---------------------------------------------------------------------
@pytest.fixture
def service_args(openmsistream_output_dir):
    """
    Returns the argslists_by_class_name used by all service tests.
    """

    return {
        "DataFileUploadDirectory": [
            openmsistream_output_dir,
            "--config",
            TEST_CONST.TEST_CFG_FILE_PATH,
        ],
        "DataFileDownloadDirectory": [
            openmsistream_output_dir,
            "--config",
            TEST_CONST.TEST_CFG_FILE_PATH,
        ],
        "S3TransferStreamProcessor": [
            TEST_CONST.TEST_BUCKET_NAME,
            "--output_dir",
            openmsistream_output_dir,
            "--config",
            TEST_CONST.TEST_CFG_FILE_PATH_S3,
            "--topic_name",
            S3_TOPIC_NAME,
            "--consumer_group_id",
            "create_new",
        ],
    }


# ---------------------------------------------------------------------
# WINDOWS TEST
# ---------------------------------------------------------------------
@pytest.mark.kafka
@pytest.mark.skipif(platform.system() != "Windows", reason="Windows only")
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("kafka_topics", "apply_kafka_env")
def test_windows_services_kafka(service_args):
    assert len(SERVICE_CONST.available_services) > 0

    for service_dict in SERVICE_CONST.available_services:
        service_class = service_dict["class"]
        class_name = service_class.__name__

        if class_name in SKIP_CLASS_NAMES:
            continue
        if class_name not in service_args:
            raise ValueError(f"Missing args for class {class_name}")

        service_name = class_name + "_test"
        argslist = [str(a) for a in service_args[class_name]]

        manager = WindowsServiceManager(
            service_name,
            service_spec_string=class_name,
            argslist=argslist,
            interactive=False,
        )

        try:
            manager.install_service()

            for mode in ("start", "status", "stop", "remove", "reinstall"):
                time.sleep(5)
                manager.run_manage_command(mode, False, False)

            time.sleep(5)
            assert_error_log_empty(service_name)

        finally:
            # cleanup files
            for stem in ("env_vars", "install_args"):
                fp = SERVICE_CONST.WORKING_DIR / f"{service_name}_{stem}.txt"
                if fp.exists():
                    fp.unlink()


# ---------------------------------------------------------------------
# LINUX TEST (systemd)
# ---------------------------------------------------------------------
@pytest.mark.kafka
@pytest.mark.skipif(
    platform.system() != "Linux"
    or check_output(["ps", "--no-headers", "-o", "comm", "1"]).decode().strip()
    != "systemd",
    reason="Linux+systemd only",
)
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("kafka_topics", "apply_kafka_env")
def test_linux_services_kafka(service_args, openmsistream_output_dir):
    assert len(SERVICE_CONST.available_services) > 0

    for service_dict in SERVICE_CONST.available_services:
        service_class = service_dict["class"]
        class_name = service_class.__name__

        if class_name in SKIP_CLASS_NAMES:
            continue
        if class_name not in service_args:
            raise ValueError(f"Missing args for {class_name}")

        service_name = class_name + "_test"
        argslist = [str(a) for a in service_args[class_name]]

        manager = LinuxServiceManager(
            service_name,
            service_spec_string=class_name,
            argslist=argslist,
            interactive=False,
        )

        service_file = SERVICE_CONST.DAEMON_SERVICE_DIR / f"{service_name}.service"

        try:
            openmsistream_output_dir.mkdir(exist_ok=True)

            manager.install_service()

            for mode in ("start", "status", "stop", "remove", "reinstall"):
                time.sleep(5)
                manager.run_manage_command(mode, False, False)

            time.sleep(5)

            # removed after uninstall
            assert not service_file.exists()

            assert_error_log_empty(service_name)

        finally:
            # cleanup
            if openmsistream_output_dir.exists():
                run_cmd_in_subprocess(
                    ["sudo", "rm", "-rf", str(openmsistream_output_dir)]
                )
            if service_file.exists():
                run_cmd_in_subprocess(["sudo", "rm", str(service_file)])

            # env + args files
            for stem in ("env_vars", "install_args", "service"):
                fp = SERVICE_CONST.WORKING_DIR / f"{service_name}_{stem}.txt"
                if fp.exists():
                    fp.unlink()


# ---------------------------------------------------------------------
# CUSTOM RUNNABLE SERVICE
# ---------------------------------------------------------------------
@pytest.mark.kafka
@pytest.mark.skipif(
    platform.system() not in ("Windows", "Linux"),
    reason="Windows or Linux only",
)
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("kafka_topics", "apply_kafka_env")
def test_custom_runnable_service(openmsistream_output_dir):
    service_name = "RunnableExampleServiceTest"
    test_file = openmsistream_output_dir / "runnable_example_service_test.txt"
    error_log = pathlib.Path().resolve() / f"{service_name}{SERVICE_CONST.ERROR_LOG_STEM}"
    service_file = SERVICE_CONST.DAEMON_SERVICE_DIR / f"{service_name}.service"

    if test_file.exists():
        test_file.unlink()

    try:
        install_service_main(
            [
                f"RunnableExample={runnable_example.__name__}",
                str(openmsistream_output_dir),
                "--service_name",
                service_name,
            ]
        )

        for mode in ("start", "status", "stop", "remove", "reinstall"):
            manage_service_main([service_name, mode])
            time.sleep(5 if mode in ("start", "reinstall") else 1)

        assert test_file.exists()
        assert_error_log_empty(service_name)

        if platform.system() == "Linux":
            assert not service_file.exists()

    finally:
        for fp in (
            test_file,
            error_log,
            SERVICE_CONST.WORKING_DIR / f"{service_name}_env_vars.txt",
            SERVICE_CONST.WORKING_DIR / f"{service_name}_install_args.txt",
        ):
            if fp.exists():
                fp.unlink()
        if platform.system() == "Linux":
            if service_file.exists():
                run_cmd_in_subprocess(["sudo", "rm", str(service_file)])
            cleanup = SERVICE_CONST.WORKING_DIR / f"{service_name}.service"
            if cleanup.exists():
                cleanup.unlink()


# ---------------------------------------------------------------------
# CUSTOM SCRIPT SERVICE
# ---------------------------------------------------------------------
@pytest.mark.kafka
@pytest.mark.skipif(
    platform.system() not in ("Windows", "Linux"),
    reason="Windows or Linux only",
)
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("kafka_topics", "apply_kafka_env")
def test_custom_script_service(openmsistream_output_dir):
    service_name = "ScriptExampleServiceTest"
    test_file = openmsistream_output_dir / "script_example_service_test.txt"
    error_log = pathlib.Path().resolve() / f"{service_name}{SERVICE_CONST.ERROR_LOG_STEM}"
    service_file = SERVICE_CONST.DAEMON_SERVICE_DIR / f"{service_name}.service"

    if test_file.exists():
        test_file.unlink()

    try:
        install_service_main(
            [
                "openmsistream.services.examples.script_example:main",
                str(openmsistream_output_dir),
                "--service_name",
                service_name,
            ]
        )

        for mode in ("start", "status", "stop", "remove", "reinstall"):
            manage_service_main([service_name, mode])
            time.sleep(5 if mode in ("start", "reinstall") else 1)

        assert test_file.exists()
        assert_error_log_empty(service_name)

        if platform.system() == "Linux":
            assert not service_file.exists()

    finally:
        for fp in (
            test_file,
            error_log,
            SERVICE_CONST.WORKING_DIR / f"{service_name}_env_vars.txt",
            SERVICE_CONST.WORKING_DIR / f"{service_name}_install_args.txt",
        ):
            if fp.exists():
                fp.unlink()

        if platform.system() == "Linux":
            if service_file.exists():
                run_cmd_in_subprocess(["sudo", "rm", str(service_file)])
            cleanup = SERVICE_CONST.WORKING_DIR / f"{service_name}.service"
            if cleanup.exists():
                cleanup.unlink()
