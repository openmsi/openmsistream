# imports
import unittest, platform, pathlib, time
from subprocess import check_output
from openmsistream.utilities.config import RUN_CONST
from openmsistream.services.config import SERVICE_CONST
from openmsistream.services.utilities import run_cmd_in_subprocess
from openmsistream.services.windows_service_manager import WindowsServiceManager
from openmsistream.services.linux_service_manager import LinuxServiceManager
from openmsistream.services.install_service import main as install_service_main
from openmsistream.services.manage_service import main as manage_service_main

try:
    from .config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from .base_classes import (
        TestWithKafkaTopics,
        TestWithOpenMSIStreamOutputLocation,
    )
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from base_classes import TestWithKafkaTopics, TestWithOpenMSIStreamOutputLocation


# some classes to skip because they're more complex
SKIP_CLASS_NAMES = ["GirderUploadStreamProcessor"]


class TestServices(TestWithKafkaTopics, TestWithOpenMSIStreamOutputLocation):
    """
    Class for testing that Services can be installed/started/stopped/removed
    without any errors on Linux OS
    """

    DEF_TOPIC_NAME = RUN_CONST.DEFAULT_TOPIC_NAME
    S3_TOPIC_NAME = "test_s3_transfer_stream_processor"

    TOPICS = {DEF_TOPIC_NAME: {}, S3_TOPIC_NAME: {}}

    def setUp(self):  # pylint: disable=invalid-name
        """
        Create a dictionary of arguments that each service should use
        """
        super().setUp()
        self.argslists_by_class_name = {
            "DataFileUploadDirectory": [
                self.output_dir,
                "--config",
                TEST_CONST.TEST_CFG_FILE_PATH,
            ],
            "DataFileDownloadDirectory": [
                self.output_dir,
                "--config",
                TEST_CONST.TEST_CFG_FILE_PATH,
            ],
            "S3TransferStreamProcessor": [
                TEST_CONST.TEST_BUCKET_NAME,
                "--output_dir",
                self.output_dir,
                "--config",
                TEST_CONST.TEST_CFG_FILE_PATH_S3,
                "--topic_name",
                self.S3_TOPIC_NAME,
                "--consumer_group_id",
                "create_new",
            ],
        }

    def assert_error_log_empty(self, service_name):
        """
        Assert that the error log for a service is empty
        """
        error_log_path = (
            pathlib.Path().resolve() / f"{service_name}{SERVICE_CONST.ERROR_LOG_STEM}"
        )
        if error_log_path.is_file():
            # if there was an error, print the contents of the error log
            with open(error_log_path, "r") as error_log_file:
                error_log_contents = error_log_file.read()
            self.log_at_error(error_log_contents)
            raise AssertionError(
                f"ERROR: {service_name} had an error during installation or execution"
                "(see above for details)!"
            )

    @unittest.skipIf(platform.system() != "Windows", "test can only be run on Windows")
    def test_windows_services_kafka(self):
        """
        Make sure every possible Windows service can be installed, started, checked,
        stopped, removed, and reinstalled
        """
        self.assertTrue(len(SERVICE_CONST.available_services) > 0)
        for service_dict in SERVICE_CONST.available_services:
            try:
                service_class_name = service_dict["class"].__name__
                if service_class_name in SKIP_CLASS_NAMES:
                    continue
                if service_class_name not in self.argslists_by_class_name:
                    raise ValueError(
                        f'ERROR: no arguments to use found for class "{service_class_name}"!'
                    )
                service_name = service_class_name + "_test"
                argslist_to_use = []
                for arg in self.argslists_by_class_name[service_class_name]:
                    argslist_to_use.append(str(arg))
                manager = WindowsServiceManager(
                    service_name,
                    service_spec_string=service_class_name,
                    argslist=argslist_to_use,
                    interactive=False,
                    logger=self.logger,
                )
                manager.install_service()
                for run_mode in ("start", "status", "stop", "remove", "reinstall"):
                    self.log_at_info(f"Running {run_mode} for {service_name}....")
                    time.sleep(5)
                    manager.run_manage_command(run_mode, False, False)
                time.sleep(5)
                self.assert_error_log_empty(service_name)
            except Exception as exc:
                raise exc
            finally:
                fps_to_unlink = [
                    (SERVICE_CONST.WORKING_DIR / f"{service_name}_env_vars.txt"),
                    (SERVICE_CONST.WORKING_DIR / f"{service_name}_install_args.txt"),
                ]
                for fp in fps_to_unlink:
                    if fp.exists():
                        fp.unlink()
        self.success = True  # pylint: disable=attribute-defined-outside-init

    @unittest.skipIf(
        platform.system() != "Linux"
        or check_output(["ps", "--no-headers", "-o", "comm", "1"]).decode().strip()
        != "systemd",
        "test requires systemd running on Linux",
    )
    def test_linux_services_kafka(self):
        """
        Make sure every possible Linux service can be installed, started, checked,
        stopped, removed, and reinstalled
        """
        self.assertTrue(len(SERVICE_CONST.available_services) > 0)
        for service_dict in SERVICE_CONST.available_services:
            try:
                if not self.output_dir.is_dir():
                    self.output_dir.mkdir()
                service_class_name = service_dict["class"].__name__
                if service_class_name in SKIP_CLASS_NAMES:
                    continue
                if service_class_name not in self.argslists_by_class_name:
                    raise ValueError(
                        f'ERROR: no arguments to use found for class "{service_class_name}"!'
                    )
                service_name = service_class_name + "_test"
                argslist_to_use = []
                for arg in self.argslists_by_class_name[service_class_name]:
                    argslist_to_use.append(str(arg))
                manager = LinuxServiceManager(
                    service_name,
                    service_spec_string=service_class_name,
                    argslist=argslist_to_use,
                    interactive=False,
                    logger=self.logger,
                )
                self.log_at_info(f"Installing {service_name}...")
                manager.install_service()
                for run_mode in ("start", "status", "stop", "remove", "reinstall"):
                    time.sleep(5)
                    self.log_at_info(f"Running {run_mode} for {service_name}....")
                    manager.run_manage_command(run_mode, False, False)
                time.sleep(5)
                self.assertFalse(
                    (
                        SERVICE_CONST.DAEMON_SERVICE_DIR / f"{service_name}.service"
                    ).exists()
                )
                self.assert_error_log_empty(service_name)
            except Exception as exc:
                raise exc
            finally:
                if self.output_dir.exists():
                    run_cmd_in_subprocess(
                        ["sudo", "rm", "-rf", f"{self.output_dir}"], logger=self.logger
                    )
                service_path = (
                    SERVICE_CONST.DAEMON_SERVICE_DIR / f"{service_name}.service"
                )
                if service_path.exists():
                    run_cmd_in_subprocess(
                        ["sudo", "rm", f'"{service_path}"'], logger=self.logger
                    )
                fps_to_unlink = [
                    (SERVICE_CONST.WORKING_DIR / f"{service_name}_env_vars.txt"),
                    (SERVICE_CONST.WORKING_DIR / f"{service_name}_install_args.txt"),
                    (SERVICE_CONST.WORKING_DIR / f"{service_name}.service"),
                ]
                for fp in fps_to_unlink:
                    if fp.exists():
                        fp.unlink()
        self.success = True  # pylint: disable=attribute-defined-outside-init

    @unittest.skipIf(
        (platform.system() != "Windows")
        and (
            platform.system() != "Linux"
            or check_output(["ps", "--no-headers", "-o", "comm", "1"]).decode().strip()
            != "systemd"
        ),
        "test can only be run on Windows or on Linux with systemd installed",
    )
    def test_custom_runnable_service(self):
        """
        Make sure the example custom Runnable service can be installed,
        started, checked, stopped, removed, and reinstalled
        """
        service_name = "RunnableExampleServiceTest"
        test_file_path = self.output_dir / "runnable_example_service_test.txt"
        error_log_path = (
            pathlib.Path().resolve() / f"{service_name}{SERVICE_CONST.ERROR_LOG_STEM}"
        )
        service_path = SERVICE_CONST.DAEMON_SERVICE_DIR / f"{service_name}.service"
        if test_file_path.exists():
            test_file_path.unlink()
        try:
            install_service_main(
                [
                    "RunnableExample=openmsistream.services.examples.runnable_example",
                    str(test_file_path.parent.resolve()),
                    "--service_name",
                    service_name,
                ]
            )
            for run_mode in ("start", "status", "stop", "remove", "reinstall"):
                manage_service_main([service_name, run_mode])
                time.sleep(5 if run_mode in ("start", "reinstall") else 1)
            self.assertTrue(test_file_path.is_file())
            self.assert_error_log_empty(service_name)
            if platform.system() == "Linux":
                self.assertFalse(service_path.exists())
        except Exception as exc:
            raise exc
        finally:
            fps_to_unlink = [
                test_file_path,
                error_log_path,
                (SERVICE_CONST.WORKING_DIR / f"{service_name}_env_vars.txt"),
                (SERVICE_CONST.WORKING_DIR / f"{service_name}_install_args.txt"),
            ]
            if platform.system() == "Linux":
                if service_path.exists():
                    run_cmd_in_subprocess(
                        ["sudo", "rm", f'"{service_path}"'], logger=self.logger
                    )
                fps_to_unlink.append(
                    SERVICE_CONST.WORKING_DIR / f"{service_name}.service"
                )
            for fp in fps_to_unlink:
                if fp.exists():
                    fp.unlink()
        self.success = True  # pylint: disable=attribute-defined-outside-init

    @unittest.skipIf(
        (platform.system() != "Windows")
        and (
            platform.system() != "Linux"
            or check_output(["ps", "--no-headers", "-o", "comm", "1"]).decode().strip()
            != "systemd"
        ),
        "test can only be run on Windows or on Linux with systemd installed",
    )
    def test_custom_script_service(self):
        """
        Make sure the example custom standalone script service can be installed,
        started, checked, stopped, removed, and reinstalled
        """
        service_name = "ScriptExampleServiceTest"
        test_file_path = self.output_dir / "script_example_service_test.txt"
        error_log_path = (
            pathlib.Path().resolve() / f"{service_name}{SERVICE_CONST.ERROR_LOG_STEM}"
        )
        if test_file_path.exists():
            test_file_path.unlink()
        try:
            install_service_main(
                [
                    "openmsistream.services.examples.script_example:main",
                    str(test_file_path.parent.resolve()),
                    "--service_name",
                    service_name,
                ]
            )
            for run_mode in ("start", "status", "stop", "remove", "reinstall"):
                manage_service_main([service_name, run_mode])
                time.sleep(5 if run_mode in ("start", "reinstall") else 1)
            self.assertTrue(test_file_path.is_file())
            self.assert_error_log_empty(service_name)
            if platform.system() == "Linux":
                self.assertFalse(
                    (
                        SERVICE_CONST.DAEMON_SERVICE_DIR / f"{service_name}.service"
                    ).exists()
                )
        except Exception as exc:
            raise exc
        finally:
            fps_to_unlink = [
                test_file_path,
                error_log_path,
                (SERVICE_CONST.WORKING_DIR / f"{service_name}_env_vars.txt"),
                (SERVICE_CONST.WORKING_DIR / f"{service_name}_install_args.txt"),
            ]
            if platform.system() == "Linux":
                if (
                    SERVICE_CONST.DAEMON_SERVICE_DIR / f"{service_name}.service"
                ).exists():
                    run_cmd_in_subprocess(
                        [
                            "sudo",
                            "rm",
                            str(
                                (
                                    SERVICE_CONST.DAEMON_SERVICE_DIR
                                    / f"{service_name}.service"
                                )
                            ),
                        ],
                        logger=self.logger,
                    )
                fps_to_unlink.append(
                    SERVICE_CONST.WORKING_DIR / f"{service_name}.service"
                )
            for fp in fps_to_unlink:
                if fp.exists():
                    fp.unlink()
        self.success = True  # pylint: disable=attribute-defined-outside-init
