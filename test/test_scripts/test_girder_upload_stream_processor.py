import json
import pathlib
import tempfile
from hashlib import sha512

import pytest
import responses
import girder_client

from openmsistream import GirderUploadStreamProcessor
from .config import TEST_CONST

# ----------------------------
# Constants
# ----------------------------

COLLECTION_NAME = "testing_collection"
TOPIC_NAME = "test_girder_upload_stream_processor"


# ============================================================
# Helpers
# ============================================================


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


# ============================================================
# Tests
# ============================================================


@pytest.mark.kafka
@pytest.mark.parametrize(
    "kafka_topics",
    [{TOPIC_NAME: {}}],
    indirect=True,
)
def test_girder_mimetype(
    kafka_topics,
    girder_instance,
    stream_processor_helper,
    upload_file_helper,
):
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    with tempfile.NamedTemporaryFile(
        dir=TEST_CONST.TEST_DATA_DIR_PATH, suffix=".png"
    ) as mock_png:
        mock_png.write(b"Pretend to be a PNG")
        mock_png.flush()
        png_path = pathlib.Path(mock_png.name)

        _produce_single_file(
            png_path,
            topic_name=TOPIC_NAME,
            api_url=api_url,
            api_key=api_key,
            upload_file_helper=upload_file_helper,
            stream_processor_helper=stream_processor_helper,
        )

        rel_path = png_path.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
        stream_processor_helper["wait_for_files_to_be_processed"](
            rel_path, timeout_secs=180
        )

        girder = girder_client.GirderClient(apiUrl=api_url)
        girder.authenticate(apiKey=api_key)

        gpath = (
            f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/"
            f"{TOPIC_NAME}/{rel_path.name}"
        )

        item = girder.get(
            "resource/lookup",
            parameters={"path": gpath},
        )
        assert item["_modelType"] == "item"

        fobj = next(girder.listFile(item["_id"]), None)
        assert fobj is not None
        assert fobj["_modelType"] == "file"
        assert fobj["mimeType"] == "image/png"

        girder.delete(f"/item/{item['_id']}")


# ------------------------------------------------------------


@pytest.mark.kafka
@responses.activate
@pytest.mark.parametrize(
    "kafka_topics",
    [{TOPIC_NAME: {}}],
    indirect=True,
)
def test_girder_handle_timeout(
    kafka_topics,
    girder_instance,
    stream_processor_helper,
    upload_file_helper,
):
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    responses.add_passthru(api_url)

    # First call to /file should return 502 though
    responses.add(
        responses.POST,
        f"{api_url}/file",
        status=502,
    )

    # but work after retry
    responses.add(responses.PassthroughResponse(responses.POST, f"{api_url}/file"))

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


# ------------------------------------------------------------


@pytest.mark.kafka
@pytest.mark.parametrize(
    "kafka_topics",
    [{TOPIC_NAME: {}}],
    indirect=True,
)
def test_girder_repeated_upload(
    kafka_topics,
    girder_instance,
    stream_processor_helper,
    upload_file_helper,
):
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    for mode in ["disk", "memory"]:
        _produce_single_file(
            TEST_CONST.TEST_DATA_FILE_2_PATH,
            topic_name=TOPIC_NAME,
            api_url=api_url,
            api_key=api_key,
            upload_file_helper=upload_file_helper,
            stream_processor_helper=stream_processor_helper,
            mode=mode,
        )

        rel = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)

        stream_processor_helper["wait_for_files_to_be_processed"](rel, timeout_secs=180)

        stream_processor_helper["reset_stream_processor"]()

    girder = girder_client.GirderClient(apiUrl=api_url)
    girder.authenticate(apiKey=api_key)

    gpath = f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/{TOPIC_NAME}"
    folder = girder.get(
        "/resource/lookup",
        parameters={"path": gpath},
    )
    assert folder
    items = girder.listItem(folder["_id"])
    assert len(list(items)) == 1


# ------------------------------------------------------------


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


# ------------------------------------------------------------


@pytest.mark.kafka
@pytest.mark.parametrize(
    "kafka_topics",
    [{TOPIC_NAME: {}}],
    indirect=True,
)
def test_girder_invalid_metadata_json(
    kafka_topics,
    girder_instance,
    stream_processor_helper,
    upload_file_helper,
):
    """Test handling of invalid JSON metadata."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    # This should fail when trying to parse invalid JSON metadata
    with pytest.raises(Exception) as exc_info:
        stream_processor_helper["create_stream_processor"](
            stream_processor_type=GirderUploadStreamProcessor,
            topic_name=TOPIC_NAME,
            consumer_group_id=f"pytest_{TOPIC_NAME}",
            other_init_args=(api_url, api_key),
            other_init_kwargs={
                "collection_name": COLLECTION_NAME,
                "metadata": "this is not valid JSON",
                "mode": "memory",
            },
        )

    # Check that it's a JSON decode error
    assert "Expecting value" in str(exc_info.value)


# ------------------------------------------------------------


@pytest.mark.kafka
@responses.activate
@pytest.mark.parametrize(
    "kafka_topics",
    [{TOPIC_NAME: {}}],
    indirect=True,
)
def test_girder_authentication_failure(
    kafka_topics,
    girder_instance,
    stream_processor_helper,
):
    """Test handling of Girder authentication failure."""
    api_url = girder_instance["api_url"]

    # Mock authentication failure - need to mock the POST to api_key/token
    responses.add(
        responses.POST,
        f"{api_url}/api_key/token",
        status=401,
        json={"message": "Invalid API key"},
    )

    # This should fail when trying to authenticate
    with pytest.raises(Exception) as exc_info:
        stream_processor_helper["create_stream_processor"](
            stream_processor_type=GirderUploadStreamProcessor,
            topic_name=TOPIC_NAME,
            consumer_group_id=f"pytest_{TOPIC_NAME}",
            other_init_args=(api_url, "invalid_api_key"),
            other_init_kwargs={
                "collection_name": COLLECTION_NAME,
                "mode": "memory",
            },
        )

    # The error could be about authentication or connection refused
    assert "authenticate" in str(exc_info.value).lower() or "401" in str(exc_info.value)


# ------------------------------------------------------------


@pytest.mark.kafka
@pytest.mark.parametrize(
    "kafka_topics",
    [{TOPIC_NAME: {}}],
    indirect=True,
)
def test_girder_with_root_folder_id(
    kafka_topics,
    girder_instance,
    stream_processor_helper,
    upload_file_helper,
):
    """Test using a specific root folder ID instead of collection name."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    # First, create a folder to use
    girder = girder_client.GirderClient(apiUrl=api_url)
    girder.authenticate(apiKey=api_key)

    # Get collection
    coll_id = None
    for c in girder.listCollection():
        if c["name"] == COLLECTION_NAME:
            coll_id = c["_id"]
            break

    if not coll_id:
        coll = girder.createCollection(COLLECTION_NAME, public=True)
        coll_id = coll["_id"]

    # Create a test folder
    test_folder = girder.createFolder(
        coll_id,
        "test_root_folder",
        parentType="collection",
        reuseExisting=True,
    )

    try:
        upload_file_helper(
            TEST_CONST.TEST_DATA_FILE_2_PATH,
            topic_name=TOPIC_NAME,
            rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
        )

        stream_processor_helper["create_stream_processor"](
            stream_processor_type=GirderUploadStreamProcessor,
            topic_name=TOPIC_NAME,
            consumer_group_id=f"pytest_{TOPIC_NAME}",
            other_init_args=(api_url, api_key),
            other_init_kwargs={
                "girder_root_folder_id": test_folder["_id"],
                "mode": "memory",
            },
        )

        stream_processor_helper["start_stream_processor_thread"]()

        rel = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
        stream_processor_helper["wait_for_files_to_be_processed"](rel, timeout_secs=180)

        # Verify the file was uploaded to the correct folder
        items = list(girder.listItem(test_folder["_id"], name=rel.name))
        assert len(items) > 0

        # Clean up
        for item in items:
            girder.delete(f"/item/{item['_id']}")
    finally:
        girder.delete(f"/folder/{test_folder['_id']}")


# ------------------------------------------------------------


@pytest.mark.kafka
@pytest.mark.parametrize(
    "kafka_topics",
    [{TOPIC_NAME: {}}],
    indirect=True,
)
def test_girder_nested_subdirectories(
    kafka_topics,
    girder_instance,
    stream_processor_helper,
    upload_file_helper,
):
    """Test uploading files with nested subdirectory structure."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    # Create a nested directory structure
    nested_dir = TEST_CONST.TEST_DATA_DIR_PATH / "level1" / "level2" / "level3"
    nested_dir.mkdir(parents=True, exist_ok=True)

    test_file = nested_dir / "nested_test.txt"
    test_file.write_text("Nested file content")

    try:
        upload_file_helper(
            test_file,
            topic_name=TOPIC_NAME,
            rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
        )

        stream_processor_helper["create_stream_processor"](
            stream_processor_type=GirderUploadStreamProcessor,
            topic_name=TOPIC_NAME,
            consumer_group_id=f"pytest_{TOPIC_NAME}",
            other_init_args=(api_url, api_key),
            other_init_kwargs={
                "collection_name": COLLECTION_NAME,
                "metadata": json.dumps({"nested": "true"}),
                "mode": "memory",
            },
        )

        stream_processor_helper["start_stream_processor_thread"]()

        rel_path = test_file.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
        stream_processor_helper["wait_for_files_to_be_processed"](
            rel_path, timeout_secs=180
        )

        girder = girder_client.GirderClient(apiUrl=api_url)
        girder.authenticate(apiKey=api_key)

        gpath = (
            f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/"
            f"{TOPIC_NAME}/level1/level2/level3/{test_file.name}"
        )

        item = girder.get(
            "resource/lookup",
            parameters={"path": gpath},
        )
        assert item["_modelType"] == "item"
        assert item["meta"]["nested"] == "true"

        girder.delete(f"/item/{item['_id']}")
    finally:
        test_file.unlink(missing_ok=True)
        # Clean up nested directories
        for parent in [nested_dir, nested_dir.parent, nested_dir.parent.parent]:
            try:
                parent.rmdir()
            except (OSError, FileNotFoundError):
                pass


# ------------------------------------------------------------


@pytest.mark.kafka
@pytest.mark.parametrize(
    "kafka_topics",
    [{TOPIC_NAME: {}}],
    indirect=True,
)
def test_girder_with_custom_folder_path(
    kafka_topics,
    girder_instance,
    stream_processor_helper,
    upload_file_helper,
):
    """Test uploading files with a custom girder root folder path."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    upload_file_helper(
        TEST_CONST.TEST_DATA_FILE_2_PATH,
        topic_name=TOPIC_NAME,
        rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
    )

    stream_processor_helper["create_stream_processor"](
        stream_processor_type=GirderUploadStreamProcessor,
        topic_name=TOPIC_NAME,
        consumer_group_id=f"pytest_{TOPIC_NAME}",
        other_init_args=(api_url, api_key),
        other_init_kwargs={
            "collection_name": COLLECTION_NAME,
            "girder_root_folder_path": f"{COLLECTION_NAME}/custom_path/{TOPIC_NAME}",
            "mode": "memory",
        },
    )

    stream_processor_helper["start_stream_processor_thread"]()

    rel = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
    stream_processor_helper["wait_for_files_to_be_processed"](rel, timeout_secs=180)

    girder = girder_client.GirderClient(apiUrl=api_url)
    girder.authenticate(apiKey=api_key)

    gpath = (
        f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/custom_path/"
        f"{TOPIC_NAME}/{rel.name}"
    )

    item = girder.get(
        "resource/lookup",
        parameters={"path": gpath},
    )
    assert item["_modelType"] == "item"

    girder.delete(f"/item/{item['_id']}")


# ------------------------------------------------------------


@pytest.mark.kafka
@pytest.mark.parametrize(
    "kafka_topics",
    [{TOPIC_NAME: {}}],
    indirect=True,
)
def test_girder_disk_mode(
    kafka_topics,
    girder_instance,
    stream_processor_helper,
    upload_file_helper,
):
    """Test file upload in disk mode (as opposed to memory mode)."""
    api_url = girder_instance["api_url"]
    api_key = girder_instance["api_key"]

    _produce_single_file(
        TEST_CONST.TEST_DATA_FILE_2_PATH,
        topic_name=TOPIC_NAME,
        api_url=api_url,
        api_key=api_key,
        upload_file_helper=upload_file_helper,
        stream_processor_helper=stream_processor_helper,
        mode="disk",
    )

    rel = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)

    stream_processor_helper["wait_for_files_to_be_processed"](rel, timeout_secs=180)

    girder = girder_client.GirderClient(apiUrl=api_url)
    girder.authenticate(apiKey=api_key)

    gpath = f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/{TOPIC_NAME}/{rel.name}"
    item = girder.get(
        "resource/lookup",
        parameters={"path": gpath},
    )
    assert item["_modelType"] == "item"

    girder.delete(f"/item/{item['_id']}")
