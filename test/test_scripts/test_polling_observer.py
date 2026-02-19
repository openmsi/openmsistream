# test_scripts/test_polling_observer.py
import pathlib
import filecmp
import shutil
import pytest
import time
from openmsistream.utilities.dataclass_table import DataclassTableReadOnly
from openmsistream.data_file_io.actor.file_registry.producer_file_registry import (
    RegistryLineInProgress,
    RegistryLineCompleted,
)
from openmsistream.data_file_io.actor.data_file_upload_directory import (
    DataFileUploadDirectory,
)

from .config import TEST_CONST
from .test_data_file_directories import (
    create_upload_directory,
    create_download_directory,
    start_upload_thread,
    stop_upload_thread,
    start_download_thread,
    wait_for_files_to_reconstruct,
)

# topic(s) for pytest param fixture
TOPIC_NAME = "test_polling_observer"
TOPICS = {TOPIC_NAME: {}}


@pytest.mark.kafka
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics", "apply_kafka_env")
def test_polling_observer_kafka(state, logger):
    """
    Pytest conversion of the old TestPollingObserver test.
    Uses the provided `state` fixture and the helper functions from
    test_data_file_directories.py exactly as you requested.
    """

    files_roots = {
        TEST_CONST.TEST_DATA_FILE_PATH: {
            "rootdir": TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
            "upload_expected": True,
            "download_expected": True,
        },
        TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH: {
            "rootdir": TEST_CONST.TEST_DATA_DIR_PATH,
            "upload_expected": True,
            "download_expected": True,
        },
        TEST_CONST.TEST_METADATA_DICT_PICKLE_FILE: {
            "rootdir": TEST_CONST.TEST_DATA_DIR_PATH,
            "upload_expected": True,
            "download_expected": True,
        },
    }

    create_upload_directory(
        state, cfg_file=TEST_CONST.TEST_CFG_FILE_PATH, use_polling_observer=True
    )

    # copy files into watched dir before starting the thread so upload_existing=True
    # finds them via __scrape_dir_for_files() without a race condition
    for filepath, meta in files_roots.items():
        rootdir = meta.get("rootdir")
        dest = filepath.relative_to(rootdir) if rootdir else pathlib.Path(filepath.name)
        dest_path = state["watched_dir"] / dest
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(filepath, dest_path)

    start_upload_thread(state, TOPIC_NAME)

    # exercise the "check" commands like the original test did
    state["upload_directory"].control_command_queue.put("c")
    state["upload_directory"].control_command_queue.put("check")

    time.sleep(5)

    # stop and validate upload (this uses your stop_upload_thread)
    stop_upload_thread(state)

    # validate registry tables exactly as in the unittest
    log_subdir = state["watched_dir"] / DataFileUploadDirectory.LOG_SUBDIR_NAME
    in_prog_filepath = log_subdir / f"upload_to_{TOPIC_NAME}_in_progress.csv"
    completed_filepath = log_subdir / f"uploaded_to_{TOPIC_NAME}.csv"

    assert in_prog_filepath.is_file()
    in_prog_table = DataclassTableReadOnly(
        RegistryLineInProgress, filepath=in_prog_filepath, logger=logger
    )
    assert in_prog_table.obj_addresses_by_key_attr("filename") == {}

    assert completed_filepath.is_file()
    completed_table = DataclassTableReadOnly(
        RegistryLineCompleted, filepath=completed_filepath, logger=logger
    )
    addrs_by_fp = completed_table.obj_addresses_by_key_attr("rel_filepath")

    for filepath, filedict in files_roots.items():
        if filedict["upload_expected"]:
            rootdir = filedict.get("rootdir")
            rel_path = (
                filepath.relative_to(rootdir) if rootdir else pathlib.Path(filepath.name)
            )
            assert rel_path in addrs_by_fp

    # -------------------------
    # run download phase
    # -------------------------
    _consumer_group_id = (
        f"run_data_file_download_directory_polling_observer_{TEST_CONST.PY_VERSION}"
    )

    # create download directory and start it
    create_download_directory(
        state, topic_name=TOPIC_NAME, cfg_file=TEST_CONST.TEST_CFG_FILE_PATH
    )
    start_download_thread(state)

    # exercise the "check" commands like original
    state["download_directory"].control_command_queue.put("c")
    state["download_directory"].control_command_queue.put("check")

    # wait for reconstruction of the files we expect
    rels = []
    for filepath, filedict in files_roots.items():
        if filedict["download_expected"]:
            rootdir = filedict.get("rootdir")
            rel_path = (
                filepath.relative_to(rootdir) if rootdir else pathlib.Path(filepath.name)
            )
            rels.append(rel_path)

    wait_for_files_to_reconstruct(state, rels, timeout_secs=180)

    # verify reconstructed files match originals
    for orig_fp, rel_fp in zip(files_roots.keys(), rels):
        reco_fp = state["reco_dir"] / rel_fp
        assert reco_fp.is_file()
        assert filecmp.cmp(orig_fp, reco_fp, shallow=False)

    # stop download thread cleanly
    # (we signalled quit in wait_for_files_to_reconstruct; join is done there,
    # but keep defensive cleanup if necessary)
    if state.get("download_thread") is not None:
        state["download_thread"].join(timeout=1)
