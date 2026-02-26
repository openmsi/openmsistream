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

    for _ in range(2):
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

        stream_processor_helper["reset_stream_processor"]()

    girder = girder_client.GirderClient(apiUrl=api_url)
    girder.authenticate(apiKey=api_key)

    gpath = (
        f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/" f"{TOPIC_NAME}/{rel.name} (1)"
    )

    item = girder.get(
        "/resource/lookup",
        parameters={"path": gpath, "test": True},
    )

    assert item is None


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
