import hashlib
import pathlib
import sys
import pytest
from openmsistream import S3TransferStreamProcessor, DataFileUploadDirectory
from openmsistream.s3_buckets.s3_data_transfer import S3DataTransfer
import os
from .config import TEST_CONST


#
# ==== Helper functions ====
#


@pytest.fixture
def set_s3_env(monkeypatch):
    endpoint_url = TEST_CONST.TEST_ENDPOINT_URL
    if not endpoint_url.startswith("https://"):
        endpoint_url = "https://" + endpoint_url

    monkeypatch.setenv("ACCESS_KEY_ID", TEST_CONST.TEST_ACCESS_KEY_ID)
    monkeypatch.setenv("SECRET_KEY_ID", TEST_CONST.TEST_SECRET_KEY_ID)
    monkeypatch.setenv("ENDPOINT_URL", endpoint_url)
    monkeypatch.setenv("REGION", TEST_CONST.TEST_REGION)

    yield


def md5_file(path):
    md5 = hashlib.md5()
    with open(path, "rb") as fp:
        while True:
            chunk = fp.read(65536)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()


def validate_s3_transfer(watched_dir, topic_name, logger):
    """
    Make sure contents on disk match the contents in the bucket
    """
    endpoint_url = TEST_CONST.TEST_ENDPOINT_URL
    if not endpoint_url.startswith("https://"):
        endpoint_url = "https://" + endpoint_url

    s3_config = {
        "endpoint_url": endpoint_url,
        "access_key_id": TEST_CONST.TEST_ACCESS_KEY_ID,
        "secret_key_id": TEST_CONST.TEST_SECRET_KEY_ID,
        "region": TEST_CONST.TEST_REGION,
        "bucket_name": TEST_CONST.TEST_BUCKET_NAME,
    }

    s3d = S3DataTransfer(s3_config, logger=logger)
    log_subdir = watched_dir / DataFileUploadDirectory.LOG_SUBDIR_NAME

    for fp in watched_dir.rglob("*"):
        if fp.is_dir():
            continue

        # Skip log directory files
        try:
            if fp.is_relative_to(log_subdir):
                continue
        except AttributeError:  # Py 3.7 fallback
            if str(fp).startswith(str(log_subdir)):
                continue

        file_hash = md5_file(fp)
        object_key = f"{topic_name}/{fp.relative_to(watched_dir)}"

        matched = s3d.compare_producer_datafile_with_s3_object_stream(
            TEST_CONST.TEST_BUCKET_NAME, object_key, file_hash
        )

        if not matched:
            raise AssertionError("ERROR: S3 object does not match original file")

        # Clean up
        s3d.delete_object_from_bucket(TEST_CONST.TEST_BUCKET_NAME, object_key)


#
# ==== Main Test ====
#

TOPIC_NAME = "test_s3_transfer_stream_processor"

TOPICS = {TOPIC_NAME: {}}


@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("kafka_topics", "stream_processor_helper", "set_s3_env")
def test_s3_transfer_stream_processor(
    state,
    logger,
    stream_processor_helper,
    tmp_path,
):
    """
    Pytest replacement for `test_upload_and_transfer_into_s3_bucket_kafka`
    """

    topic_name = TOPIC_NAME
    #
    # Create upload directory
    #
    from .test_data_file_directories import (
        create_upload_directory,
        start_upload_thread,
        stop_upload_thread,
    )

    create_upload_directory(
        state,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_S3,
    )

    watched_dir = state["watched_dir"]

    # Build relative filename exactly like unittest version
    prefix = f"py{sys.version_info.major}{sys.version_info.minor}-"
    fname = prefix + TEST_CONST.TEST_DATA_FILE_NAME
    rel_fp = pathlib.Path(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME) / fname

    #
    # Start upload thread
    #
    start_upload_thread(state, topic_name)

    # Copy test file into watched dir
    dest = watched_dir / rel_fp
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(pathlib.Path(TEST_CONST.TEST_DATA_FILE_PATH).read_bytes())

    #
    # Stop upload thread (flush + shutdown)
    #
    stop_upload_thread(state)

    #
    # Run the S3 transfer processor
    #
    sp = stream_processor_helper
    sp["create_stream_processor"](
        S3TransferStreamProcessor,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_S3,
        topic_name=topic_name,
        consumer_group_id=f"test_s3_transfer_py{sys.version_info.major}{sys.version_info.minor}",
        other_init_args=(TEST_CONST.TEST_BUCKET_NAME,),
    )

    sp["start_stream_processor_thread"](func=None)

    # Wait for processing
    sp["wait_for_files_to_be_processed"](rel_fp, timeout_secs=300)

    #
    # Validate that S3 matches disk
    #
    validate_s3_transfer(watched_dir, topic_name, logger)

    #
    # Reset stream processor
    #
    sp["reset_stream_processor"]()
