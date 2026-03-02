import pathlib
import filecmp
from hashlib import sha512
import pytest

from openmsistream.data_file_io.config import DATA_FILE_HANDLING_CONST
from openmsistream.data_file_io.entity.data_file_chunk import DataFileChunk
from openmsistream.data_file_io.entity.upload_data_file import UploadDataFile
from openmsistream.data_file_io.entity.download_data_file import (
    DownloadDataFileToDisk,
    DownloadDataFileToMemory,
)

from .config import TEST_CONST


# -------------------------------------------------
# FIXTURES
# -------------------------------------------------


@pytest.fixture
def ul_datafile(logger):
    """UploadDataFile object prepared with chunks ready to download."""
    ul = UploadDataFile(
        TEST_CONST.TEST_DATA_FILE_PATH,
        rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
        logger=logger,
    )
    # pylint: disable=protected-access
    ul._build_list_of_file_chunks(TEST_CONST.TEST_CHUNK_SIZE)
    ul.add_chunks_to_upload()
    return ul


@pytest.fixture
def dl_datafile_holder():
    """
    Allows tests to mutate dl_datafile during execution.
    A tiny mutable container so pytest can replicate the old self.dl_datafile behavior.
    """
    return {"dl": None}


# -------------------------------------------------
# HELPERS
# -------------------------------------------------


def add_all_chunks(ul_datafile, dl_datafile_holder, disk_or_memory, output_dir, logger):
    """
    Add chunks one-by-one, identical to old TestDownloadDataFile.add_all_chunks()
    """
    for i_chunk, dfc in enumerate(ul_datafile.chunks_to_upload):
        dfc.populate_with_file_data(logger=logger)

        subdir = pathlib.PurePosixPath(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME)
        dfc_as_dl = DataFileChunk(
            subdir / dfc.filename,
            dfc.filename,
            dfc.file_hash,
            dfc.chunk_hash,
            None,
            dfc.chunk_offset_write,
            dfc.chunk_size,
            dfc.chunk_i,
            dfc.n_total_chunks,
            data=dfc.data,
        )
        dfc_as_dl.rootdir = output_dir

        # Create dl_datafile on first chunk
        if dl_datafile_holder["dl"] is None:
            if disk_or_memory == "disk":
                dl_datafile_holder["dl"] = DownloadDataFileToDisk(
                    dfc_as_dl.filepath, logger=logger
                )
            else:
                dl_datafile_holder["dl"] = DownloadDataFileToMemory(
                    dfc_as_dl.filepath, logger=logger
                )

        check = dl_datafile_holder["dl"].add_chunk(dfc_as_dl)

        # Every 10th chunk: add again → should be already-written
        if i_chunk % 10 == 0:
            check2 = dl_datafile_holder["dl"].add_chunk(dfc_as_dl)
            assert check2 == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

        expected = DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        if i_chunk == len(ul_datafile.chunks_to_upload) - 1:
            expected = DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

        assert check == expected


def run_download_chunks(
    ul_datafile, dl_datafile_holder, disk_or_memory, output_dir, logger
):
    """
    Converts original TestDownloadDataFile.run_download_chunks()
    """
    dl_datafile_holder["dl"] = None

    # Add chunks normally
    add_all_chunks(ul_datafile, dl_datafile_holder, disk_or_memory, output_dir, logger)

    dl = dl_datafile_holder["dl"]

    # Validate reconstructed output
    if disk_or_memory == "disk":
        fp = output_dir / TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME / dl.filename
        assert filecmp.cmp(TEST_CONST.TEST_DATA_FILE_PATH, fp, shallow=False)
        fp.unlink()
    else:  # memory mode
        with open(TEST_CONST.TEST_DATA_FILE_PATH, "rb") as f:
            ref_data = f.read()
        assert dl.bytestring == ref_data

    # ---------------------------------------
    # Test hash mismatch behavior
    # ---------------------------------------
    # pylint: disable=protected-access
    dl._chunk_offsets_downloaded = []

    hash_missing = sha512()
    for i_chunk, dfc in enumerate(ul_datafile.chunks_to_upload):
        if i_chunk % 3 == 0:
            hash_missing.update(dfc.data)
    hash_missing.digest()

    for i_chunk, dfc in enumerate(ul_datafile.chunks_to_upload):
        subdir = pathlib.PurePosixPath(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME)
        dfc_as_dl = DataFileChunk(
            subdir / dfc.filename,
            dfc.filename,
            dfc.file_hash,
            dfc.chunk_hash,
            None,
            dfc.chunk_offset_write,
            dfc.chunk_size,
            dfc.chunk_i,
            dfc.n_total_chunks,
            data=dfc.data,
        )
        dfc_as_dl.rootdir = output_dir

        # Only change the hash of the last chunk
        if i_chunk == len(ul_datafile.chunks_to_upload) - 1:
            dfc_as_dl.file_hash = hash_missing

        check = dl.add_chunk(dfc_as_dl)

        expected = DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        if i_chunk == len(ul_datafile.chunks_to_upload) - 1:
            expected = DATA_FILE_HANDLING_CONST.FILE_HASH_MISMATCH_CODE

        assert check == expected


# -------------------------------------------------
# TESTS
# -------------------------------------------------


def test_download_chunks_to_disk(ul_datafile, dl_datafile_holder, output_dir, logger):
    run_download_chunks(
        ul_datafile,
        dl_datafile_holder,
        "disk",
        output_dir,
        logger,
    )


def test_download_chunks_to_memory(ul_datafile, dl_datafile_holder, output_dir, logger):
    run_download_chunks(
        ul_datafile,
        dl_datafile_holder,
        "memory",
        output_dir,
        logger,
    )
