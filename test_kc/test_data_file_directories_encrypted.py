import filecmp
import logging
import pathlib
import shutil
import sys
import time

import pytest
from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread

from openmsistream.data_file_io.actor.data_file_download_directory import (
    DataFileDownloadDirectory,
)
from openmsistream.data_file_io.actor.data_file_upload_directory import (
    DataFileUploadDirectory,
)
from openmsistream.data_file_io.actor.file_registry.producer_file_registry import (
    RegistryLineCompleted,
    RegistryLineInProgress,
)
from openmsistream.utilities.config import RUN_CONST
from openmsistream.utilities.dataclass_table import DataclassTableReadOnly

# logging.basicConfig(level=logging.WARNING)
logging.getLogger("kafkacrypto").setLevel(
    level=logging.DEBUG
)  # set to logging.DEBUG for more verbosity


TOPIC_NAME = "test_oms_encrypted"
TOPICS = {
    TOPIC_NAME: {},
    f"{TOPIC_NAME}.keys": {"--partitions": 1},
    f"{TOPIC_NAME}.reqs": {"--partitions": 1},
    f"{TOPIC_NAME}.subs": {"--partitions": 1},
}

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------


def create_upload_directory(state, cfg_file=None, **kwargs):
    watched_dir = state["tmp_path"] / "watched"
    watched_dir.mkdir(exist_ok=True)
    state["watched_dir"] = watched_dir
    state["upload_directory"] = DataFileUploadDirectory(watched_dir, cfg_file, **kwargs)
    return state["upload_directory"]


def create_download_directory(state, topic_name, **kwargs):
    cfg_file = kwargs.pop("cfg_file")
    reco_dir = state["tmp_path"] / "reco"
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
    chunk_size=16384,
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

    if isinstance(rel_paths, (str, pathlib.PurePath)):
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


# ------------------------------------------------------------
# Core Test Logic
# ------------------------------------------------------------


def run_upload(state, files_roots, logger, topic, **kwargs):
    # copy files into watched dir before starting the thread so upload_existing=True
    # finds them via __scrape_dir_for_files() without a race condition
    for filepath, meta in files_roots.items():
        rootdir = meta.get("rootdir")
        dest = filepath.relative_to(rootdir) if rootdir else filepath.name
        dest_path = state["watched_dir"] / dest
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(filepath, dest_path)

    start_upload_thread(state, topic)

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
    assert not in_table.obj_addresses_by_key_attr("filename")

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


@pytest.mark.kafka
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics")
@pytest.mark.integration
def test_encrypted_upload_and_download_kafka(
    state, kafka_config_file, encrypted_kafka_node_config
):
    """Test sending and receiving encrypted messages."""

    test_rel_filepath = (
        pathlib.Path("test_file_sub_dir") / "1a0ceb89-b5f0-45dc-9c12-63d3020e2217.dat"
    )

    rot_password = "arglebargle123"
    password = "123456"

    consumer_config_path = kafka_config_file(nodeID="testing_node_2")
    choice = 4  # 1 - controller, 2 - producer, 3 - consumer, 4 - prodcon
    encrypted_kafka_node_config(
        consumer_config_path,
        "testing_node_2",
        TOPIC_NAME,
        rot_password,
        password,
        1,
        choice,
        False,
    )
    choice = 4  # 1 - controller, 2 - producer, 3 - consumer, 4 - prodcon
    producer_config_path = kafka_config_file(nodeID="testing_node")
    encrypted_kafka_node_config(
        producer_config_path,
        "testing_node",
        TOPIC_NAME,
        rot_password,
        password,
        1,
        choice,
        False,
    )

    # Create upload directory, copy file first, then start thread so upload_existing=True
    # finds it via __scrape_dir_for_files() without a race condition
    create_upload_directory(state, cfg_file=producer_config_path)
    watched_dir = state["watched_dir"]
    target_fp = watched_dir / test_rel_filepath
    target_fp.parent.mkdir(exist_ok=True, parents=True)
    spath = (
        "../openmsistream/test/data/test_file_root_dir/"
        "test_file_sub_dir/1a0ceb89-b5f0-45dc-9c12-63d3020e2217.dat"
    )  # TODO: FIX ME just temporary
    target_fp.write_bytes(pathlib.Path(spath).read_bytes())

    start_upload_thread(state, TOPIC_NAME, chunk_size=16 * 16384)

    # Start download directory and thread
    py_version = f"python_{sys.version.split()[0].replace('.', '_')}"
    create_download_directory(
        state,
        topic_name=TOPIC_NAME,
        cfg_file=consumer_config_path,
        consumer_group_id=f"test_encrypted_data_file_directories_{py_version}",
    )
    start_download_thread(state)

    time.sleep(5)  # allow initial processing

    # Trigger 'check' commands
    # state["upload_directory"].control_command_queue.put("c")
    # state["download_directory"].control_command_queue.put("c")
    # state["upload_directory"].control_command_queue.put("check")
    # state["download_directory"].control_command_queue.put("check")

    # Wait for the file reconstruction
    wait_for_files_to_reconstruct(state, test_rel_filepath, timeout_secs=30)

    # Stop the upload thread
    stop_upload_thread(state)

    # Assert reconstructed file exists and matches original
    reco_fp = state["reco_dir"] / test_rel_filepath
    assert reco_fp.is_file(), f"Reconstructed file {reco_fp} not found"
    assert filecmp.cmp(spath, reco_fp, shallow=False), (
        "Files are not identical after reconstruction. "
        "Check timeout or upload/download process."
    )

    # Assert ProducerFileRegistry files
    log_subdir = state["watched_dir"] / DataFileUploadDirectory.LOG_SUBDIR_NAME
    in_prog_filepath = log_subdir / f"upload_to_{TOPIC_NAME}_in_progress.csv"
    completed_filepath = log_subdir / f"uploaded_to_{TOPIC_NAME}.csv"

    assert in_prog_filepath.is_file(), "In-progress registry file missing"
    in_prog_table = DataclassTableReadOnly(
        RegistryLineInProgress, filepath=in_prog_filepath, logger=None
    )
    assert not in_prog_table.obj_addresses_by_key_attr("filename"), (
        "In-progress table not empty"
    )

    assert completed_filepath.is_file(), "Completed registry file missing"
    completed_table = DataclassTableReadOnly(
        RegistryLineCompleted, filepath=completed_filepath, logger=None
    )
    addrs_by_fp = completed_table.obj_addresses_by_key_attr("rel_filepath")
    assert test_rel_filepath in addrs_by_fp, "Test file not listed in completed table"
