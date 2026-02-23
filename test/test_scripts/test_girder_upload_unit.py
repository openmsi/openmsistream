"""
Unit tests for GirderUploadStreamProcessor._process_downloaded_data_file.

These tests mock the Girder client to exercise upload logic without requiring
a running Girder instance or Kafka broker.
"""

import threading
from hashlib import sha256
from unittest.mock import MagicMock, patch

import pytest
import girder_client

from openmsistream.girder.girder_upload_stream_processor import (
    GirderUploadStreamProcessor,
)


# ── Helpers ──────────────────────────────────────────────────────


class MockDataFile:
    """Lightweight stand-in for DownloadDataFile with only the attributes
    that __process_downloaded_data_file inspects."""

    def __init__(
        self,
        filename="test.dat",
        content=b"hello world",
        subdir_str="",
        full_filepath=None,
    ):
        self.filename = filename
        self.bytestring = content
        self.subdir_str = subdir_str
        self.relative_filepath = f"{subdir_str}/{filename}" if subdir_str else filename
        if full_filepath is not None:
            self.full_filepath = full_filepath
        # intentionally omit full_filepath when not provided so
        # hasattr(self, "full_filepath") returns False


def _sha256_hex(data: bytes) -> str:
    return sha256(data).hexdigest()


def _mock_logger_error(*args, **kwargs):
    """Mimic OpenMSILogger.error: re-raise exc_info when reraise=True."""
    if kwargs.get("reraise") and isinstance(kwargs.get("exc_info"), Exception):
        raise kwargs["exc_info"]


@pytest.fixture
def processor():
    """Build a GirderUploadStreamProcessor with its heavy __init__ bypassed."""
    with patch.object(
        GirderUploadStreamProcessor, "__init__", lambda self, *a, **kw: None
    ):
        proc = GirderUploadStreamProcessor.__new__(GirderUploadStreamProcessor)

    mock_client = MagicMock(spec=girder_client.GirderClient)
    # wire up name-mangled private attributes
    # pylint: disable=invalid-name
    proc._GirderUploadStreamProcessor__girder_client = mock_client
    proc._GirderUploadStreamProcessor__root_folder_id = "root_folder_id"
    # pylint: enable=invalid-name
    proc.minimal_metadata_dict = {
        "OpenMSIStreamVersion": "test",
        "KafkaTopic": "test_topic",
    }

    mock_logger = MagicMock()
    mock_logger.error.side_effect = _mock_logger_error
    proc.logger = mock_logger

    return proc, mock_client, mock_logger


@pytest.fixture
def lock():
    return threading.Lock()


# ── Duplicate detection ──────────────────────────────────────────


class TestDuplicateDetection:

    def test_same_name_same_checksum_skips_upload(self, processor, lock):
        """Item with matching name AND checksum already exists -> skip."""
        proc, client, logger = processor
        content = b"original content"
        df = MockDataFile(content=content)

        client.listItem.return_value = [
            {"meta": {"checksum": {"sha256": _sha256_hex(content)}}}
        ]

        result = proc._process_downloaded_data_file(df, lock)

        assert result is None
        client.uploadStreamToFolder.assert_not_called()
        logger.warning.assert_called_once()
        assert "same checksum" in logger.warning.call_args[0][0].lower()

    def test_same_name_different_checksum_uploads_anyway(self, processor, lock):
        """Item with matching name but DIFFERENT checksum -> warn and upload."""
        proc, client, logger = processor
        content = b"updated content"
        df = MockDataFile(content=content)

        client.listItem.return_value = [{"meta": {"checksum": {"sha256": "00" * 32}}}]
        client.uploadStreamToFolder.return_value = {"itemId": "item_123"}

        result = proc._process_downloaded_data_file(df, lock)

        assert result is None
        client.uploadStreamToFolder.assert_called_once()
        warning_msg = logger.warning.call_args[0][0].lower()
        assert "different checksum" in warning_msg

    def test_same_name_no_checksum_in_metadata_uploads(self, processor, lock):
        """Existing item has no checksum metadata -> treat as different, upload."""
        proc, client, _ = processor
        df = MockDataFile(content=b"data")

        client.listItem.return_value = [{"meta": {}}]
        client.uploadStreamToFolder.return_value = {"itemId": "item_456"}

        result = proc._process_downloaded_data_file(df, lock)

        assert result is None
        client.uploadStreamToFolder.assert_called_once()

    def test_no_existing_items_uploads(self, processor, lock):
        """No items with the same name -> upload normally."""
        proc, client, _ = processor
        df = MockDataFile(content=b"brand new")

        client.listItem.return_value = []
        client.uploadStreamToFolder.return_value = {"itemId": "item_new"}

        result = proc._process_downloaded_data_file(df, lock)

        assert result is None
        client.uploadStreamToFolder.assert_called_once()


# ── Successful upload paths ──────────────────────────────────────


class TestUploadSuccess:

    def test_metadata_set_after_upload(self, processor, lock):
        """After upload, metadata with checksum is added to the item."""
        proc, client, _ = processor
        content = b"file bytes"
        df = MockDataFile(content=content)

        client.listItem.return_value = []
        client.uploadStreamToFolder.return_value = {"itemId": "item_meta"}

        result = proc._process_downloaded_data_file(df, lock)

        assert result is None
        client.addMetadataToItem.assert_called_once()
        item_id_arg, meta_arg = client.addMetadataToItem.call_args[0]
        assert item_id_arg == "item_meta"
        assert meta_arg["checksum"]["sha256"] == _sha256_hex(content)
        assert meta_arg["OpenMSIStreamVersion"] == "test"
        assert meta_arg["KafkaTopic"] == "test_topic"

    def test_subdirectory_folders_created(self, processor, lock):
        """Nested subdirectories in datafile.subdir_str create folders."""
        proc, client, _ = processor
        df = MockDataFile(subdir_str="sub1/sub2", content=b"nested")

        client.createFolder.side_effect = [
            {"_id": "folder_sub1"},
            {"_id": "folder_sub2"},
        ]
        client.listItem.return_value = []
        client.uploadStreamToFolder.return_value = {"itemId": "item_nested"}

        result = proc._process_downloaded_data_file(df, lock)

        assert result is None
        assert client.createFolder.call_count == 2
        # first folder under root
        assert client.createFolder.call_args_list[0][0] == (
            "root_folder_id",
            "sub1",
        )
        # second folder under first
        assert client.createFolder.call_args_list[1][0] == (
            "folder_sub1",
            "sub2",
        )
        # upload into the deepest folder
        assert client.uploadStreamToFolder.call_args[0][0] == "folder_sub2"

    def test_mimetype_from_extension(self, processor, lock):
        """Mimetype is guessed from filename extension."""
        proc, client, _ = processor
        df = MockDataFile(filename="photo.png", content=b"png")

        client.listItem.return_value = []
        client.uploadStreamToFolder.return_value = {"itemId": "item_png"}

        proc._process_downloaded_data_file(df, lock)

        assert client.uploadStreamToFolder.call_args[1]["mimeType"] == "image/png"

    def test_unknown_extension_uses_octet_stream(self, processor, lock):
        """Unrecognised extension falls back to application/octet-stream."""
        proc, client, _ = processor
        df = MockDataFile(filename="data.xyz999", content=b"binary")

        client.listItem.return_value = []
        client.uploadStreamToFolder.return_value = {"itemId": "item_bin"}

        proc._process_downloaded_data_file(df, lock)

        assert (
            client.uploadStreamToFolder.call_args[1]["mimeType"]
            == "application/octet-stream"
        )


# ── Error handling ───────────────────────────────────────────────


class TestUploadFailures:

    def test_upload_exception_returned(self, processor, lock):
        """When uploadStreamToFolder raises, the exception is returned."""
        proc, client, logger = processor
        df = MockDataFile(content=b"data")

        client.listItem.return_value = []
        client.uploadStreamToFolder.side_effect = ConnectionError("network down")

        result = proc._process_downloaded_data_file(df, lock)

        assert isinstance(result, ConnectionError)
        logger.error.assert_called()

    def test_missing_item_id_returns_runtime_error(self, processor, lock):
        """Upload response without itemId -> RuntimeError returned."""
        proc, client, _ = processor
        df = MockDataFile(content=b"data")

        client.listItem.return_value = []
        client.uploadStreamToFolder.return_value = {}

        result = proc._process_downloaded_data_file(df, lock)

        assert isinstance(result, RuntimeError)
        assert "could not determine the Item ID" in str(result)

    def test_metadata_failure_returned(self, processor, lock):
        """When addMetadataToItem fails, the exception is returned."""
        proc, client, _ = processor
        df = MockDataFile(content=b"data")

        client.listItem.return_value = []
        client.uploadStreamToFolder.return_value = {"itemId": "item_ok"}
        client.addMetadataToItem.side_effect = RuntimeError("meta API down")

        result = proc._process_downloaded_data_file(df, lock)

        assert isinstance(result, RuntimeError)

    def test_folder_creation_failure_returned(self, processor, lock):
        """When creating a subdirectory folder fails, the exception is returned."""
        proc, client, _ = processor
        df = MockDataFile(subdir_str="bad_folder", content=b"data")

        client.createFolder.side_effect = RuntimeError("folder API error")

        result = proc._process_downloaded_data_file(df, lock)

        assert isinstance(result, RuntimeError)


# ── AttributeError fallback ──────────────────────────────────────


class TestUploadFallback:

    def test_fallback_to_file_upload(self, processor, lock):
        """AttributeError on stream upload -> falls back to uploadFileToFolder."""
        proc, client, _ = processor
        df = MockDataFile(content=b"data", full_filepath="/tmp/test.dat")

        client.listItem.return_value = []
        client.uploadStreamToFolder.side_effect = AttributeError("no bytestring")
        client.uploadFileToFolder.return_value = {"itemId": "item_file"}

        result = proc._process_downloaded_data_file(df, lock)

        assert result is None
        client.uploadFileToFolder.assert_called_once()

    def test_fallback_without_filepath_returns_error(self, processor, lock):
        """Stream upload fails + no full_filepath -> ValueError returned."""
        proc, client, _ = processor
        df = MockDataFile(content=b"data")  # no full_filepath attribute

        client.listItem.return_value = []
        client.uploadStreamToFolder.side_effect = AttributeError("no bytestring")

        result = proc._process_downloaded_data_file(df, lock)

        assert isinstance(result, ValueError)
        assert "Stream upload unavailable" in str(result)

    def test_fallback_with_none_filepath_returns_error(self, processor, lock):
        """Stream upload fails + full_filepath is None -> ValueError returned."""
        proc, client, _ = processor
        df = MockDataFile(content=b"data", full_filepath=None)

        client.listItem.return_value = []
        client.uploadStreamToFolder.side_effect = AttributeError("no bytestring")

        result = proc._process_downloaded_data_file(df, lock)

        assert isinstance(result, ValueError)
