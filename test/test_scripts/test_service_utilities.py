import pathlib
import importlib

from openmsistream.services.config import SERVICE_CONST
from openmsistream.services.linux_service_manager import LinuxServiceManager

try:
    from .config import TEST_CONST  # pylint: disable=import-error,wrong-import-order
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order


TEST_SERVICE_CLASS_NAME = "DataFileUploadDirectory"
TEST_SERVICE_NAME = "testing_service"
TEST_SERVICE_EXECUTABLE_ARGSLIST = ["test_upload"]


def test_some_configs():
    """
    Make sure that some config variables can be created successfully
    """
    for service_dict in SERVICE_CONST.available_services:
        importlib.import_module(service_dict["filepath"])

    SERVICE_CONST.logger.info("testing")


def test_write_executable_file(tmp_path):
    """
    Make sure an executable file is written to the expected location
    with the expected format
    """

    # construct test exec fp
    test_exec_fp = (
        pathlib.Path(__file__).parent.parent.parent
        / "openmsistream"
        / "services"
        / "working_dir"
    )
    test_exec_fp = (
        test_exec_fp / f"{TEST_SERVICE_NAME}{SERVICE_CONST.SERVICE_EXECUTABLE_NAME_STEM}"
    )

    manager = LinuxServiceManager(
        TEST_SERVICE_NAME,
        service_spec_string=TEST_SERVICE_CLASS_NAME,
        argslist=TEST_SERVICE_EXECUTABLE_ARGSLIST,
    )

    # write file
    manager._write_executable_file(
        filepath=test_exec_fp
    )  # pylint: disable=protected-access
    assert test_exec_fp.is_file()

    test_lines = test_exec_fp.read_text().splitlines(True)

    ref_exec_fp = TEST_CONST.TEST_DATA_DIR_PATH / test_exec_fp.name
    assert ref_exec_fp.is_file()

    ref_lines = ref_exec_fp.read_text().splitlines(True)
    real_ref_lines = [ref_line for ref_line in ref_lines if ref_line.strip() != ""]

    for test_line, ref_line in zip(test_lines, real_ref_lines):
        if ref_line.lstrip().startswith(
            "output_filepath = "
        ) or ref_line.lstrip().startswith("main(["):
            continue
        assert test_line == ref_line
