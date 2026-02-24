import pathlib
import time
import filecmp
import pytest
from openmsistream.utilities.dataclass_table import DataclassTableReadOnly
from openmsistream.data_file_io.actor.file_registry.producer_file_registry import (
    RegistryLineInProgress,
    RegistryLineCompleted,
)
from openmsistream import DataFileUploadDirectory
from .config import TEST_CONST
from .test_data_file_directories import (  # assuming your helper functions are saved in helpers.py
    create_upload_directory,
    create_download_directory,
    start_upload_thread,
    start_download_thread,
    stop_upload_thread,
    wait_for_files_to_reconstruct,
)

TOPIC_NAME = "test_oms_encrypted"
TOPICS = {TOPIC_NAME: {}}


@pytest.mark.kafka
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics", "apply_kafka_env")
@pytest.mark.integration
def test_encrypted_upload_and_download_kafka(state):
    """Test sending and receiving encrypted messages."""

    test_rel_filepath = (
        pathlib.Path(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME)
        / TEST_CONST.TEST_DATA_FILE_NAME
    )

    # Create upload directory, copy file first, then start thread so upload_existing=True
    # finds it via __scrape_dir_for_files() without a race condition
    create_upload_directory(state, cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC)
    watched_dir = state["watched_dir"]
    target_fp = watched_dir / test_rel_filepath
    target_fp.parent.mkdir(exist_ok=True, parents=True)
    target_fp.write_bytes(pathlib.Path(TEST_CONST.TEST_DATA_FILE_PATH).read_bytes())

    start_upload_thread(state, TOPIC_NAME, chunk_size=16 * TEST_CONST.TEST_CHUNK_SIZE)

    # Start download directory and thread
    create_download_directory(
        state,
        topic_name=TOPIC_NAME,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
        consumer_group_id=f"test_encrypted_data_file_directories_{TEST_CONST.PY_VERSION}",
    )
    start_download_thread(state)

    time.sleep(10)  # allow initial processing

    # Trigger 'check' commands
    state["upload_directory"].control_command_queue.put("c")
    state["download_directory"].control_command_queue.put("c")
    state["upload_directory"].control_command_queue.put("check")
    state["download_directory"].control_command_queue.put("check")

    # Wait for the file reconstruction
    wait_for_files_to_reconstruct(state, test_rel_filepath, timeout_secs=300)

    # Stop the upload thread
    stop_upload_thread(state)

    # Assert reconstructed file exists and matches original
    reco_fp = state["reco_dir"] / test_rel_filepath
    assert reco_fp.is_file(), f"Reconstructed file {reco_fp} not found"
    assert filecmp.cmp(TEST_CONST.TEST_DATA_FILE_PATH, reco_fp, shallow=False), (
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
    assert not in_prog_table.obj_addresses_by_key_attr(
        "filename"
    ), "In-progress table not empty"

    assert completed_filepath.is_file(), "Completed registry file missing"
    completed_table = DataclassTableReadOnly(
        RegistryLineCompleted, filepath=completed_filepath, logger=None
    )
    addrs_by_fp = completed_table.obj_addresses_by_key_attr("rel_filepath")
    assert test_rel_filepath in addrs_by_fp, "Test file not listed in completed table"
