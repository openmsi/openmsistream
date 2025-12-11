import pathlib
import shutil
import json
import datetime
import time
import re
import filecmp
import pytest
import threading
from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread

from openmsistream.utilities.dataclass_table import DataclassTableReadOnly
from openmsistream.data_file_io.actor.file_registry.producer_file_registry import (
    RegistryLineInProgress,
    RegistryLineCompleted,
)
from openmsistream.data_file_io.actor.data_file_upload_directory import (
    DataFileUploadDirectory,
)
from openmsistream.data_file_io.actor.data_file_download_directory import (
    DataFileDownloadDirectory,
)
from .config import TEST_CONST
from openmsistream.utilities.config import RUN_CONST


# ------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------


@pytest.fixture
def upload_state(tmp_path):
    """A simple mutable dict holding runtime state per test."""
    return {}


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------


def create_upload_directory(state, cfg_file=TEST_CONST.TEST_CFG_FILE_PATH, **kwargs):
    watched_dir = pathlib.Path(TEST_CONST.TEST_DATA_DIR_PATH / "watched")
    watched_dir.mkdir(exist_ok=True)
    state["watched_dir"] = watched_dir
    state["upload_directory"] = DataFileUploadDirectory(watched_dir, cfg_file, **kwargs)
    return state["upload_directory"]


def create_download_directory(state, topic_name, **kwargs):
    cfg_file = kwargs.pop("cfg_file", TEST_CONST.TEST_CFG_FILE_PATH)
    reco_dir = pathlib.Path(TEST_CONST.TEST_DATA_DIR_PATH / "reco")
    reco_dir.mkdir(exist_ok=True)
    state["reco_dir"] = reco_dir
    state["download_directory"] = DataFileDownloadDirectory(
        reco_dir,
        cfg_file,
        topic_name,
        **kwargs,
    )
    return state["download_directory"]


def start_upload_thread(
    state,
    topic_name,
    n_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS,
    chunk_size=TEST_CONST.TEST_CHUNK_SIZE,
    max_queue_size=RUN_CONST.DEFAULT_MAX_UPLOAD_QUEUE_MEGABYTES,
    upload_existing=True,
):
    """
    Start the upload directory running in a new thread with Exceptions tracked
    """
    upload_thread = ExceptionTrackingThread(
        target=state["upload_directory"].upload_files_as_added,
        args=(topic_name,),
        kwargs={
            "n_threads": n_threads,
            "chunk_size": chunk_size,
            "max_queue_size": max_queue_size,
            "upload_existing": upload_existing,
        },
    )
    upload_thread.start()
    state["upload_thread"] = upload_thread


def stop_upload_thread(state, timeout_secs=90):
    # state["upload_directory"].shutdown()
    state["upload_directory"].control_command_queue.put("q")
    # wait for the uploading thread to complete
    upload_thread = state["upload_thread"]
    upload_thread.join(timeout=timeout_secs)
    if upload_thread.is_alive():
        raise TimeoutError(
            f"ERROR: upload thread timed out after {timeout_secs} seconds!"
        )


def start_download_thread(state):
    """
    Start running the "reconstruct" function in a new thread
    """
    download_thread = ExceptionTrackingThread(
        target=state["download_directory"].reconstruct
    )
    download_thread.start()
    state["download_thread"] = download_thread


def wait_for_files_to_reconstruct(
    state, rel_paths, timeout_secs=90, before_close_callback=lambda: None
):
    """
    Wait for the download thread to process and reconstruct the specified files,
    then stop the download thread safely.

    Args:
        state: dict containing 'download_directory' and 'download_thread'.
        rel_paths: list of relative file paths to wait for.
        timeout_secs: maximum seconds to wait before raising TimeoutError.
        before_close_callback: optional callable to run before shutting down the thread.
    """
    download_dir = state["download_directory"]
    download_thread = state["download_thread"]

    if isinstance(rel_paths, str):
        rel_paths = [rel_paths]

    # Track which files have been reconstructed
    files_found = {rp: False for rp in rel_paths}

    start_time = time.time()
    while not all(files_found.values()) and (time.time() - start_time) < timeout_secs:
        # Check which files have been processed by the download thread
        for rp in rel_paths:
            if not files_found[rp] and rp in download_dir.recent_processed_filepaths:
                files_found[rp] = True
        time.sleep(0.25)  # short sleep to yield to thread

    if not all(files_found.values()):
        raise TimeoutError(f"Files not reconstructed in {timeout_secs} seconds")

    # Run any pre-close actions
    before_close_callback()

    # Signal the download thread to quit
    download_dir.control_command_queue.put("q")

    # Wait for the thread to finish
    download_thread.join(timeout=30)
    if download_thread.is_alive():
        raise TimeoutError("Download thread timed out after 30 seconds")


# def wait_for_files_to_reconstruct(state, rel_paths, timeout_secs=90):
#     reco_dir = state["reco_dir"]
#     deadline = time.time() + timeout_secs
#     while time.time() < deadline:
#         if all((reco_dir / rp).exists() for rp in rel_paths):
#             return
#         time.sleep(0.25)
#     raise TimeoutError("Files not reconstructed in time")


# ------------------------------------------------------------
# Core Test Logic
# ------------------------------------------------------------


def run_upload(state, files_roots, logger, topic, **kwargs):
    # create_upload_directory(state, **kwargs)
    start_upload_thread(state, topic)

    # copy files into watched dir
    for filepath, meta in files_roots.items():
        rootdir = meta.get("rootdir")
        dest = filepath.relative_to(rootdir) if rootdir else filepath.name
        dest_path = state["watched_dir"] / dest
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(filepath, dest_path)

    d = state["upload_directory"]
    time.sleep(5)
    d.control_command_queue.put("c")
    d.control_command_queue.put("check")

    stop_upload_thread(state)

    # validate registry tables
    log_subdir = state["watched_dir"] / DataFileUploadDirectory.LOG_SUBDIR_NAME

    inprog = log_subdir / f"upload_to_{topic}_in_progress.csv"
    comp = log_subdir / f"uploaded_to_{topic}.csv"

    assert inprog.is_file()
    in_table = DataclassTableReadOnly(
        RegistryLineInProgress, filepath=inprog, logger=logger
    )
    assert in_table.obj_addresses_by_key_attr("filename") == {}

    assert comp.is_file()
    ctable = DataclassTableReadOnly(RegistryLineCompleted, filepath=comp, logger=logger)
    addrs = ctable.obj_addresses_by_key_attr("rel_filepath")

    for fp, meta in files_roots.items():
        if meta["upload_expected"]:
            rootdir = meta.get("rootdir")
            rel = fp.relative_to(rootdir) if rootdir else pathlib.Path(fp.name)
            assert rel in addrs


def run_download(state, files_roots, topic, **kwargs):
    relevant = {}
    for fp, meta in files_roots.items():
        if meta["download_expected"]:
            rootdir = meta.get("rootdir")
            rel = fp.relative_to(rootdir) if rootdir else pathlib.Path(fp.name)
            relevant[fp] = rel

    create_download_directory(state, topic_name=topic, **kwargs)
    start_download_thread(state)

    d = state["download_directory"]
    time.sleep(5)
    d.control_command_queue.put("c")
    d.control_command_queue.put("check")

    wait_for_files_to_reconstruct(state, relevant.values())

    for orig, rel in relevant.items():
        reco = state["reco_dir"] / rel
        assert reco.is_file()
        assert filecmp.cmp(orig, reco, shallow=False)


# ------------------------------------------------------------
# Pytest Tests
# ------------------------------------------------------------

TOPICS = {
    "test_data_file_directories": {},
    "heartbeats": {"--partitions": 1},
    "logs": {"--partitions": 1},
}


@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics", "apply_kafka_env")
def test_upload_and_download_directories_kafka(upload_state, logger, apply_kafka_env):
    files = {
        TEST_CONST.TEST_DATA_FILE_PATH: {
            "rootdir": TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
            "upload_expected": True,
            "download_expected": True,
        },
        TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH: {
            "rootdir": TEST_CONST.TEST_DATA_DIR_PATH,
            "upload_expected": True,
            "download_expected": False,
        },
        TEST_CONST.TEST_METADATA_DICT_PICKLE_FILE: {
            "rootdir": TEST_CONST.TEST_DATA_DIR_PATH,
            "upload_expected": False,
            "download_expected": False,
        },
    }

    upload_regex = re.compile(r"^.*\.(dat|config)$")
    download_regex = re.compile(r"^.*\.dat$")
    topic = "test_data_file_directories"

    create_upload_directory(
        upload_state, TEST_CONST.TEST_CFG_FILE_PATH, upload_regex=upload_regex
    )
    run_upload(upload_state, files, logger, topic, upload_regex=upload_regex)
    time.sleep(1)

    gid = f"run_data_file_download_directory_with_regexes_{TEST_CONST.PY_VERSION}"

    run_download(
        upload_state,
        files,
        topic,
        consumer_group_id=gid,
        filepath_regex=download_regex,
    )


def test_filepath_should_be_uploaded(upload_state):
    d = create_upload_directory(upload_state, TEST_CONST.TEST_CFG_FILE_PATH)

    with pytest.raises(TypeError):
        d.filepath_should_be_uploaded(None)
    with pytest.raises(TypeError):
        d.filepath_should_be_uploaded(5)
    with pytest.raises(TypeError):
        d.filepath_should_be_uploaded("not a path")

    assert not d.filepath_should_be_uploaded(TEST_CONST.TEST_DATA_DIR_PATH / ".hidden")
    assert not d.filepath_should_be_uploaded(TEST_CONST.TEST_DATA_DIR_PATH / "abc.log")

    for fp in TEST_CONST.TEST_DATA_DIR_PATH.rglob("*"):
        check = not (fp.is_dir() or fp.name.startswith(".") or fp.name.endswith(".log"))
        assert d.filepath_should_be_uploaded(fp.resolve()) == check

    subdir = TEST_CONST.TEST_DATA_DIR_PATH / "this_subdirectory_should_not_be_uploaded"
    subdir.mkdir()
    try:
        assert not d.filepath_should_be_uploaded(subdir)
    finally:
        shutil.rmtree(subdir)
