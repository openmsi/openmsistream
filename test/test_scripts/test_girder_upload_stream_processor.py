import re
import json
from hashlib import sha512
from threading import Lock

import pytest
import responses
import girder_client

from openmsistream import GirderUploadStreamProcessor
from openmsistream.data_file_io.entity.download_data_file import (
    DownloadDataFileToDisk,
    DownloadDataFileToMemory,
)
from .config import TEST_CONST

COLLECTION_NAME = "testing_collection"
TOPIC_NAME = "test_girder_upload_stream_processor"


@pytest.fixture(params=["memory", "disk"])
def mock_datafile(request, tmp_path):
    mode = request.param

    def _create_datafile(
        content,
        filename,
        subdir_str="",
        filepath=None,
    ):
        """
        Create a mock DownloadDataFile with the given properties.

        :param content: The file content as bytes
        :param filename: The filename
        :param subdir_str: Subdirectory string (empty string for root)
        :param filepath: Optional custom filepath (defaults to tmp_path/filename)
        :return: DownloadDataFileToDisk or DownloadDataFileToMemory instance
        """
        if filepath is None:
            filepath = tmp_path / filename

        if mode == "memory":
            datafile = DownloadDataFileToMemory(filepath)
            datafile.filename = filename
            datafile.full_filepath = filepath
            datafile.subdir_str = subdir_str
            datafile.n_total_chunks = 1
            datafile.bytestring = content
        else:  # disk mode
            # Ensure parent directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
            # Write content to disk
            filepath.write_bytes(content)

            datafile = DownloadDataFileToDisk(filepath)
            datafile.filename = filename
            datafile.full_filepath = filepath
            datafile.subdir_str = subdir_str
            datafile.n_total_chunks = 1

        return datafile

    return _create_datafile


def _produce_single_file(
    filepath,
    *,
    topic_name,
    api_url,
    api_key,
    upload_file_helper,
    stream_processor_helper,
    mode="memory",
):
    """
    Shared helper for all tests.
    """
    upload_file_helper(
        filepath,
        topic_name=topic_name,
        rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
    )

    stream_processor_helper["create_stream_processor"](
        stream_processor_type=GirderUploadStreamProcessor,
        topic_name=topic_name,
        consumer_group_id=f"pytest_{topic_name}",
        other_init_args=(api_url, api_key),
        other_init_kwargs={
            "collection_name": COLLECTION_NAME,
            "metadata": json.dumps({"somekey": "somevalue"}),
            "mode": mode,
        },
    )

    stream_processor_helper["start_stream_processor_thread"]()


@pytest.mark.kafka
@pytest.mark.parametrize(
    "kafka_topics",
    [{TOPIC_NAME: {}}],
    indirect=True,
)
def test_girder_upload_stream_processor_kafka(
    kafka_topics,
    girder_instance,
    stream_processor_helper,
    upload_file_helper,
):
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    _produce_single_file(
        TEST_CONST.TEST_DATA_FILE_2_PATH,
        topic_name=TOPIC_NAME,
        api_url=api_url,
        api_key=api_key,
        upload_file_helper=upload_file_helper,
        stream_processor_helper=stream_processor_helper,
    )

    rel = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)

    stream_processor_helper["wait_for_files_to_be_processed"](rel, timeout_secs=180)

    original_hash = sha512()
    with open(TEST_CONST.TEST_DATA_FILE_2_PATH, "rb") as fp:
        original_hash.update(fp.read())
    original_hash = original_hash.digest()

    girder = girder_client.GirderClient(apiUrl=api_url)
    girder.authenticate(apiKey=api_key)

    coll_id = next(
        c["_id"] for c in girder.listCollection() if c["name"] == COLLECTION_NAME
    )

    coll_folder_id = next(
        f["_id"]
        for f in girder.listFolder(coll_id, parentFolderType="collection")
        if f["name"] == COLLECTION_NAME
    )

    topic_folder_id = next(
        f["_id"] for f in girder.listFolder(coll_folder_id) if f["name"] == TOPIC_NAME
    )

    item = next(girder.listItem(topic_folder_id, name=rel.name))
    assert item["meta"]["somekey"] == "somevalue"

    file_id = next(f["_id"] for f in girder.listFile(item["_id"]))

    girder_hash = sha512()
    for chunk in girder.downloadFileAsIterator(file_id):
        girder_hash.update(chunk)

    assert original_hash == girder_hash.digest()

    girder.delete(f"/item/{item['_id']}")


@pytest.mark.kafka
def test_girder_invalid_metadata_json(girder_instance, apply_kafka_env):
    """Test handling of invalid JSON metadata."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    # This should fail when trying to parse invalid JSON metadata
    with pytest.raises(Exception) as exc_info:
        GirderUploadStreamProcessor(
            api_url,
            api_key,
            config_file=TEST_CONST.TEST_CFG_FILE_PATH,
            topic_name=TOPIC_NAME,
            consumer_group_id=f"pytest_{TOPIC_NAME}",
            collection_name=COLLECTION_NAME,
            metadata="this is not valid JSON",
            mode="memory",
        )

    # Check that it's a JSON decode error
    assert "Expecting value" in str(exc_info.value)


@pytest.mark.kafka
@responses.activate
def test_girder_authentication_failure(girder_instance, apply_kafka_env):
    """Test handling of Girder authentication failure."""
    api_url = girder_instance["api_url"]

    # Mock authentication failure - need to mock the POST to api_key/token
    responses.add(
        responses.POST,
        f"{api_url}/api_key/token",
        status=401,
        json={"message": "Invalid API key"},
    )

    with pytest.raises(Exception) as exc_info:
        GirderUploadStreamProcessor(
            api_url,
            "invalid_api_key",
            config_file=TEST_CONST.TEST_CFG_FILE_PATH,
            topic_name=TOPIC_NAME,
            consumer_group_id=f"pytest_{TOPIC_NAME}",
            collection_name=COLLECTION_NAME,
            mode="memory",
        )

    assert "authenticate" in str(exc_info.value).lower() or "401" in str(exc_info.value)


# ------------------------------------------------------------


@pytest.mark.kafka
def test_girder_upload_simple_file(
    mock_datafile, girder_instance, tmp_path, apply_kafka_env
):
    """Test uploading a simple file using direct _process_downloaded_data_file call."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    # Create a processor instance
    processor = GirderUploadStreamProcessor(
        api_url,
        api_key,
        config_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name=TOPIC_NAME,
        collection_name=COLLECTION_NAME,
    )

    # Create a mock datafile
    content = b"Test file content"
    datafile = mock_datafile(
        content=content,
        filename="test_file.txt",
        subdir_str="",
    )

    result = processor._process_downloaded_data_file(datafile, Lock())

    # Should succeed (return None)
    assert result is None

    # Verify the file was uploaded to Girder
    gc = girder_client.GirderClient(apiUrl=api_url)
    gc.authenticate(apiKey=api_key)

    gpath = f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/{TOPIC_NAME}/test_file.txt"
    item = gc.get("resource/lookup", parameters={"path": gpath})
    assert item["_modelType"] == "item"

    # Verify content
    file_id = next(f["_id"] for f in gc.listFile(item["_id"]))
    downloaded_content = b""
    for chunk in gc.downloadFileAsIterator(file_id):
        downloaded_content += chunk
    assert downloaded_content == content

    # Cleanup
    gc.delete(f"/item/{item['_id']}")


@pytest.mark.kafka
def test_girder_upload_with_subdirectories(
    mock_datafile, girder_instance, tmp_path, apply_kafka_env
):
    """Test uploading files with subdirectories using direct method call."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    processor = GirderUploadStreamProcessor(
        api_url,
        api_key,
        config_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name=TOPIC_NAME,
        collection_name=COLLECTION_NAME,
    )

    content = b"Nested file content"
    datafile = mock_datafile(
        content=content,
        filename="nested.txt",
        subdir_str="level1/level2/level3",
    )

    result = processor._process_downloaded_data_file(datafile, Lock())
    assert result is None

    # Verify the nested folder structure was created
    gc = girder_client.GirderClient(apiUrl=api_url)
    gc.authenticate(apiKey=api_key)

    gpath = (
        f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/"
        f"{TOPIC_NAME}/level1/level2/level3/nested.txt"
    )
    item = gc.get("resource/lookup", parameters={"path": gpath})
    assert item["_modelType"] == "item"

    # Cleanup
    gc.delete(f"/item/{item['_id']}")


@pytest.mark.kafka
def test_girder_upload_mimetype(
    mock_datafile, girder_instance, tmp_path, apply_kafka_env
):
    """Test that MIME types are correctly detected."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    processor = GirderUploadStreamProcessor(
        api_url,
        api_key,
        config_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name=TOPIC_NAME,
        collection_name=COLLECTION_NAME,
    )

    # Create a PNG file
    content = b"Fake PNG content"
    datafile = mock_datafile(
        content=content,
        filename="test_image.png",
        subdir_str="",
    )

    result = processor._process_downloaded_data_file(datafile, Lock())
    assert result is None

    gc = girder_client.GirderClient(apiUrl=api_url)
    gc.authenticate(apiKey=api_key)

    gpath = f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/{TOPIC_NAME}/test_image.png"
    item = gc.get("resource/lookup", parameters={"path": gpath})

    file_obj = next(gc.listFile(item["_id"]))
    assert file_obj["mimeType"] == "image/png"

    gc.delete(f"/item/{item['_id']}")


@pytest.mark.kafka
@responses.activate
@pytest.mark.parametrize(
    "girder_instance",
    [[], ["girder_hashsum_download"]],
    indirect=True,
    ids=["plain", "with_hashsum_download"],
)
def test_girder_replace_existing(
    mock_datafile, girder_instance, tmp_path, apply_kafka_env, caplog
):
    """Test replace_existing flag using direct method calls."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]
    responses.add_passthru(api_url)  # Allow real requests to Girder
    gc = girder_client.GirderClient(apiUrl=api_url)
    gc.authenticate(apiKey=api_key)
    folder_path = f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/{TOPIC_NAME}"

    # First upload without replace_existing
    processor1 = GirderUploadStreamProcessor(
        api_url,
        api_key,
        config_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name=TOPIC_NAME,
        collection_name=COLLECTION_NAME,
        replace_existing=False,
    )

    content_v1 = b"Version 1 content"
    datafile1 = mock_datafile(
        content=content_v1,
        filename="versioned.txt",
        subdir_str="",
    )

    result = processor1._process_downloaded_data_file(datafile1, Lock())
    assert result is None
    root_folder = gc.get("resource/lookup", parameters={"path": folder_path})
    assert len(list(gc.listItem(root_folder["_id"]))) == 1

    # Try to upload again without replace - should skip
    datafile1 = mock_datafile(
        content=content_v1,
        filename="versioned.txt",
        subdir_str="",
    )
    with caplog.at_level("INFO"):
        processor1._process_downloaded_data_file(datafile1, Lock())
    assert "Skipping upload" in caplog.text

    content_v2 = b"Version 2 content (different)"
    datafile2 = mock_datafile(
        content=content_v2,
        filename="versioned.txt",
        subdir_str="",
    )

    result = processor1._process_downloaded_data_file(datafile2, Lock())
    assert result is None  # Should succeed but skip upload
    root_folder = gc.get("resource/lookup", parameters={"path": folder_path})
    assert len(list(gc.listItem(root_folder["_id"]))) == 1

    # Verify content is still v1
    gpath = f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/{TOPIC_NAME}/versioned.txt"
    item = gc.get("resource/lookup", parameters={"path": gpath})
    file_id = next(f["_id"] for f in gc.listFile(item["_id"]))

    downloaded = b""
    for chunk in gc.downloadFileAsIterator(file_id):
        downloaded += chunk
    assert downloaded == content_v1  # Still version 1

    # Now upload with replace_existing=True
    processor2 = GirderUploadStreamProcessor(
        api_url,
        api_key,
        config_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name=TOPIC_NAME,
        collection_name=COLLECTION_NAME,
        replace_existing=True,
    )

    datafile3 = mock_datafile(
        content=content_v2,
        filename="versioned.txt",
        subdir_str="",
    )

    # fail first
    responses.add(
        responses.PUT,
        f"{api_url}/file/{file_id}/contents",
        status=400,
    )
    with caplog.at_level("ERROR"):
        result = processor2._process_downloaded_data_file(datafile3, Lock())
        assert "ERROR: failed to replace the file at" in caplog.text
        assert result is not None
        assert isinstance(result, Exception)

    responses.reset()  # Clear previous mock
    responses.add_passthru(api_url)  # Allow real requests again
    result = processor2._process_downloaded_data_file(datafile3, Lock())
    assert result is None

    root_folder = gc.get("resource/lookup", parameters={"path": folder_path})
    assert len(list(gc.listItem(root_folder["_id"]))) == 1

    # Verify content is now v2
    item = gc.get("resource/lookup", parameters={"path": gpath})
    file_id = next(f["_id"] for f in gc.listFile(item["_id"]))

    downloaded = b""
    for chunk in gc.downloadFileAsIterator(file_id):
        downloaded += chunk
    assert downloaded == content_v2  # Now version 2

    gc.delete(f"/item/{item['_id']}")


@pytest.mark.kafka
@responses.activate
def test_girder_upload_failure_direct(
    mock_datafile, girder_instance, tmp_path, apply_kafka_env, caplog
):
    """Test error handling when upload fails."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    # Mock a failure
    responses.add_passthru(api_url)
    responses.add(
        responses.POST,
        f"{api_url}/file",
        status=500,
        json={"message": "Server error"},
    )

    processor = GirderUploadStreamProcessor(
        api_url,
        api_key,
        config_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name=TOPIC_NAME,
        collection_name=COLLECTION_NAME,
    )

    datafile = mock_datafile(
        content=b"Content",
        filename="fail.txt",
        subdir_str="",
    )

    with caplog.at_level("ERROR"):
        result = processor._process_downloaded_data_file(datafile, Lock())
        assert "failed to upload the file at" in caplog.text
        assert result is not None
        assert isinstance(result, Exception)


@pytest.mark.kafka
@responses.activate
def test_girder_retry_on_timeout(
    mock_datafile, girder_instance, tmp_path, apply_kafka_env
):
    """Test that transient failures are retried."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    responses.add_passthru(api_url)

    # First call fails with 502
    responses.add(
        responses.POST,
        f"{api_url}/file",
        status=502,
    )

    # Second call succeeds (passthrough)
    responses.add(responses.PassthroughResponse(responses.POST, f"{api_url}/file"))

    processor = GirderUploadStreamProcessor(
        api_url,
        api_key,
        config_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name=TOPIC_NAME,
        collection_name=COLLECTION_NAME,
    )

    datafile = mock_datafile(
        content=b"Retry test content",
        filename="retry.txt",
        subdir_str="",
    )

    result = processor._process_downloaded_data_file(datafile, Lock())

    # Should succeed after retry
    assert result is None

    # Verify file was uploaded
    gc = girder_client.GirderClient(apiUrl=api_url)
    gc.authenticate(apiKey=api_key)

    gpath = f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/{TOPIC_NAME}/retry.txt"
    item = gc.get("resource/lookup", parameters={"path": gpath})
    assert item is not None

    gc.delete(f"/item/{item['_id']}")


@pytest.mark.kafka
@responses.activate
def test_girder_odd_failures(
    mock_datafile, girder_instance, tmp_path, apply_kafka_env, caplog
):
    """Test handling of unexpected exceptions."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]
    responses.add_passthru(api_url)
    gc = girder_client.GirderClient(apiUrl=api_url)
    gc.authenticate(apiKey=api_key)
    # Mock an unexpected exception (e.g. connection error)
    responses.add(responses.PUT, re.compile(f"{api_url}/item/.*/metadata"), status=400)

    processor = GirderUploadStreamProcessor(
        api_url,
        api_key,
        config_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name=TOPIC_NAME,
        collection_name=COLLECTION_NAME,
    )

    datafile = mock_datafile(
        content=b"Odd failure test",
        filename="odd_failure.txt",
        subdir_str="test_folder",
    )

    with caplog.at_level("ERROR"):
        result = processor._process_downloaded_data_file(datafile, Lock())
        assert "failed to set metadata" in caplog.text
        assert result is not None
        assert isinstance(result, Exception)

    responses.reset()  # Clear previous mock
    responses.add_passthru(api_url)
    gpath = (
        f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/"
        f"{TOPIC_NAME}/test_folder/odd_failure.txt"
    )
    item = gc.get("resource/lookup", parameters={"path": gpath})
    assert item is not None
    gc.delete(f"/item/{item['_id']}")

    # fail to create / get root folder
    responses.add(responses.POST, re.compile(f"{api_url}/folder"), status=400)
    with caplog.at_level("ERROR"):
        result = processor._process_downloaded_data_file(datafile, Lock())
        assert "failed to create the 'test_folder' folder" in caplog.text
        assert result is not None
        assert isinstance(result, Exception)
    responses.reset()
    responses.add_passthru(api_url)

    @pytest.mark.kafka
    @pytest.mark.parametrize(
        "kafka_topics",
        [{TOPIC_NAME: {}}],
        indirect=True,
    )
    def test_run_from_command_line(
        kafka_topics,
        girder_instance,
        upload_file_helper,
        tmp_path,
        monkeypatch,
        caplog,
    ):
        """Test run_from_command_line method."""
        api_url = girder_instance["api_url"]
        api_key = girder_instance["api_key"]

        # Upload a file to Kafka first
        upload_file_helper(
            TEST_CONST.TEST_DATA_FILE_2_PATH,
            topic_name=TOPIC_NAME,
            rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
        )

        # Mock process_files_as_read to return after processing one file
        original_process = GirderUploadStreamProcessor.process_files_as_read

        def mock_process(self):
            result = original_process(self)
            # Stop after processing
            return result

        monkeypatch.setattr(
            GirderUploadStreamProcessor, "process_files_as_read", mock_process
        )

        # Prepare command line arguments
        args = [
            api_url,
            api_key,
            str(TEST_CONST.TEST_CFG_FILE_PATH),
            TOPIC_NAME,
            "--consumer_group_id",
            f"pytest_{TOPIC_NAME}",
            "--collection_name",
            COLLECTION_NAME,
            "--n_most_recent_files_to_check",
            "1",
        ]

        with caplog.at_level("INFO"):
            GirderUploadStreamProcessor.run_from_command_line(args=args)

            # Verify logging messages
            assert f"Listening to the {TOPIC_NAME} topic" in caplog.text
            assert "Girder upload stream processor" in caplog.text
            assert "shut down" in caplog.text
            assert "total messages were consumed" in caplog.text
            assert "files were uploaded to Girder" in caplog.text

        # Verify the file was uploaded
        gc = girder_client.GirderClient(apiUrl=api_url)
        gc.authenticate(apiKey=api_key)

        rel = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
        gpath = f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/{TOPIC_NAME}/{rel.name}"
        item = gc.get("resource/lookup", parameters={"path": gpath})
        assert item["_modelType"] == "item"

        # Cleanup
        gc.delete(f"/item/{item['_id']}")

    @pytest.mark.kafka
    @pytest.mark.parametrize(
        "kafka_topics",
        [{TOPIC_NAME: {}}],
        indirect=True,
    )
    def test_run_from_command_line_with_output_dir(
        kafka_topics,
        girder_instance,
        upload_file_helper,
        tmp_path,
        monkeypatch,
        caplog,
    ):
        """Test run_from_command_line with output_dir specified."""
        api_url = girder_instance["api_url"]
        api_key = girder_instance["api_key"]
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Upload a file to Kafka
        upload_file_helper(
            TEST_CONST.TEST_DATA_FILE_2_PATH,
            topic_name=TOPIC_NAME,
            rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
        )

        # Mock to stop after one file
        original_process = GirderUploadStreamProcessor.process_files_as_read

        def mock_process(self):
            return original_process(self)

        monkeypatch.setattr(
            GirderUploadStreamProcessor, "process_files_as_read", mock_process
        )

        args = [
            api_url,
            api_key,
            str(TEST_CONST.TEST_CFG_FILE_PATH),
            TOPIC_NAME,
            "--consumer_group_id",
            f"pytest_{TOPIC_NAME}_output",
            "--collection_name",
            COLLECTION_NAME,
            "--output_dir",
            str(output_dir),
            "--n_most_recent_files_to_check",
            "1",
        ]

        with caplog.at_level("INFO"):
            GirderUploadStreamProcessor.run_from_command_line(args=args)

            # Verify the output_dir is mentioned in shutdown message
            assert f"writing to {output_dir}" in caplog.text
            assert "shut down" in caplog.text

    @pytest.mark.kafka
    @pytest.mark.parametrize(
        "kafka_topics",
        [{TOPIC_NAME: {}}],
        indirect=True,
    )
    def test_run_from_command_line_debug_logging(
        kafka_topics,
        girder_instance,
        upload_file_helper,
        tmp_path,
        monkeypatch,
        caplog,
    ):
        """Test that debug logging shows uploaded filepaths."""
        api_url = girder_instance["api_url"]
        api_key = girder_instance["api_key"]

        upload_file_helper(
            TEST_CONST.TEST_DATA_FILE_2_PATH,
            topic_name=TOPIC_NAME,
            rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
        )

        original_process = GirderUploadStreamProcessor.process_files_as_read

        def mock_process(self):
            return original_process(self)

        monkeypatch.setattr(
            GirderUploadStreamProcessor, "process_files_as_read", mock_process
        )

        args = [
            api_url,
            api_key,
            str(TEST_CONST.TEST_CFG_FILE_PATH),
            TOPIC_NAME,
            "--consumer_group_id",
            f"pytest_{TOPIC_NAME}_debug",
            "--collection_name",
            COLLECTION_NAME,
            "--n_most_recent_files_to_check",
            "1",
        ]

        with caplog.at_level("DEBUG"):
            GirderUploadStreamProcessor.run_from_command_line(args=args)

            # Should see debug message with filepath
            assert "successfully uploaded to Girder" in caplog.text
            assert "Uploaded filepaths" in caplog.text

    @pytest.mark.kafka
    def test_run_from_command_line_no_files(
        girder_instance,
        tmp_path,
        monkeypatch,
        caplog,
        apply_kafka_env,
    ):
        """Test run_from_command_line when no files are processed."""
        api_url = girder_instance["api_url"]
        api_key = girder_instance["api_key"]

        # Mock to return immediately with no files
        def mock_process(self):
            return 0, 0, 0, []

        monkeypatch.setattr(
            GirderUploadStreamProcessor, "process_files_as_read", mock_process
        )

        args = [
            api_url,
            api_key,
            str(TEST_CONST.TEST_CFG_FILE_PATH),
            f"{TOPIC_NAME}_empty",
            "--consumer_group_id",
            f"pytest_{TOPIC_NAME}_empty",
            "--collection_name",
            COLLECTION_NAME,
        ]

        with caplog.at_level("INFO"):
            GirderUploadStreamProcessor.run_from_command_line(args=args)

            # Should still show completion messages
            assert "0 total messages were consumed" in caplog.text
            assert "0 files were uploaded to Girder" in caplog.text
            assert "shut down" in caplog.text

        # Should NOT have debug filepath message since no files processed
        with caplog.at_level("DEBUG"):
            assert "Uploaded filepaths" not in caplog.text
