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
    dl.n_total_chunks = None
    dl._expected_file_hash = None

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

        # Set wrong hash on every chunk so generation resilience doesn't trigger
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


def _make_test_chunks(data, filename, subdir, chunk_size, file_hash):
    """Helper to create DataFileChunk objects from raw data."""
    chunks = []
    offset = 0
    chunk_i = 1
    n_total = -(-len(data) // chunk_size)  # ceiling division
    while offset < len(data):
        end = min(offset + chunk_size, len(data))
        chunk_data = data[offset:end]
        chunk_hash = sha512(chunk_data).digest()
        chunks.append(
            DataFileChunk(
                subdir / filename,
                filename,
                file_hash,
                chunk_hash,
                None,
                offset,
                len(chunk_data),
                chunk_i,
                n_total,
                data=chunk_data,
            )
        )
        offset = end
        chunk_i += 1
    return chunks


def test_generation_reset_to_memory(output_dir, logger):
    """
    Verify that DownloadDataFileToMemory resets when a newer (larger)
    generation arrives, and reconstructs the newer version correctly.
    """
    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "growing_file.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    # --- Generation 1: a small file (2 chunks) ---
    gen1_data = b"A" * int(chunk_size * 1.5)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)

    # --- Generation 2: a larger file (3 chunks) ---
    gen2_data = b"B" * int(chunk_size * 2.5)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    # Feed Gen1 chunk 1 — establishes n_total_chunks=2, hash=gen1
    result = dl.add_chunk(gen1_chunks[0])
    assert result == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    # Feed Gen2 chunk 1 — different hash, strictly higher n_total_chunks → RESET
    result = dl.add_chunk(gen2_chunks[0])
    assert result == DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE

    # Feed all Gen2 chunks from the start
    for i, chunk in enumerate(gen2_chunks):
        result = dl.add_chunk(chunk)
        if i == 0:
            # Chunk 0 may be ALREADY_WRITTEN (was the trigger for reset)
            assert result in (
                DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS,
                DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE,
            )
        elif i < len(gen2_chunks) - 1:
            assert result == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        else:
            assert result == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    assert dl.bytestring == gen2_data


def test_interleaved_generations_to_memory(output_dir, logger):
    """
    Verify that interleaved chunks from two generations are handled correctly:
    newer generation wins, older generation chunks are skipped.
    """
    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "interleaved.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    # Gen1: 3 chunks
    gen1_data = b"X" * (chunk_size * 3)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)

    # Gen2: 5 chunks (file grew)
    gen2_data = b"Y" * (chunk_size * 5)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    # Gen1 chunk 1 → accept, lock in Gen1
    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    # Gen2 chunk 1 → different hash, 5 > 3 → RESET to Gen2
    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE

    # Gen1 chunk 2 → stale (n=3 < 5) → SKIP
    r = dl.add_chunk(gen1_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    # Gen2 chunk 1 again → the reset didn't write it, so it gets accepted now
    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    # Gen1 chunk 3 → stale (n=3 < 5) → SKIP
    r = dl.add_chunk(gen1_chunks[2])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    # Feed remaining Gen2 chunks 2-5 → all accepted
    for i in range(1, len(gen2_chunks)):
        r = dl.add_chunk(gen2_chunks[i])
        if i < len(gen2_chunks) - 1:
            assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        else:
            assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    # Verify the reconstructed file is Gen2
    assert dl.bytestring == gen2_data


def test_equal_chunk_count_no_thrashing(output_dir, logger):
    """
    Verify that two generations with the SAME n_total_chunks but different
    hashes do NOT cause thrashing. The first generation committed to wins;
    chunks from the other generation are skipped.
    """
    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "same_size.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    # Gen1 and Gen2: same size (3 chunks each) but different content
    gen1_data = b"P" * (chunk_size * 3)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)

    gen2_data = b"Q" * (chunk_size * 3)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)

    # Sanity: hashes must differ, chunk counts must be equal
    assert gen1_hash != gen2_hash
    assert gen1_chunks[0].n_total_chunks == gen2_chunks[0].n_total_chunks

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    # Interleave: Gen1[0], Gen2[0], Gen1[1], Gen2[1], Gen1[2], Gen2[2]
    # Gen1 should win (committed first); all Gen2 chunks should be skipped.

    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE  # skipped, not reset

    r = dl.add_chunk(gen1_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    r = dl.add_chunk(gen2_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE  # skipped

    r = dl.add_chunk(gen1_chunks[2])
    assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    # Gen1 completed; Gen2 was entirely skipped
    assert dl.bytestring == gen1_data


def test_stale_chunks_skipped_after_reset_to_disk(output_dir, logger):
    """
    Verify that DownloadDataFileToDisk skips stale (older generation) chunks
    after resetting to a newer generation, and reconstructs correctly.
    """
    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "growing_on_disk.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    # Gen1: 2 chunks
    gen1_data = b"M" * int(chunk_size * 1.5)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)

    # Gen2: 4 chunks
    gen2_data = b"N" * int(chunk_size * 3.5)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToDisk(gen1_chunks[0].filepath, logger=logger)

    # Gen1 chunk 1 → accept
    dl.add_chunk(gen1_chunks[0])

    # Gen2 chunk 2 → reset (newer, 4 > 2)
    r = dl.add_chunk(gen2_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE

    # Gen1 chunk 2 → skip (stale, n_total_chunks=2 < 4)
    r = dl.add_chunk(gen1_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    # Feed all Gen2 chunks
    for i, chunk in enumerate(gen2_chunks):
        r = dl.add_chunk(chunk)
        if i < len(gen2_chunks) - 1:
            assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        else:
            assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    # Verify reconstructed file matches Gen2
    assert dl.bytestring == gen2_data


def test_generation_change_with_hash_mismatch(output_dir, logger):
    """
    Verify that a generation reset followed by hash corruption is caught:
    gen1 partially downloads, gen2 arrives with more chunks but a wrong
    file_hash on every chunk. The generation reset fires first, then
    hash verification catches the corruption on the final chunk.
    """
    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "gen_hash_mismatch.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    # Gen1: a single-chunk file
    gen1_data = b"G" * (chunk_size // 2)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)
    assert len(gen1_chunks) == 1  # single chunk

    # Gen2: a larger file (3 chunks)
    gen2_data = b"H" * int(chunk_size * 2.5)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)
    assert len(gen2_chunks) > len(gen1_chunks)

    # Corrupt the file_hash on every gen2 chunk
    bad_hash = sha512(b"deliberate corruption").digest()
    for c in gen2_chunks:
        c.file_hash = bad_hash

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    # Feed gen1's only chunk — establishes gen1 baseline
    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    # Feed first gen2 chunk — different hash, more chunks → GENERATION_RESET
    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE

    # Feed all gen2 chunks (including first again after reset)
    for i, chunk in enumerate(gen2_chunks):
        r = dl.add_chunk(chunk)
        if i < len(gen2_chunks) - 1:
            assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        else:
            # Last chunk: hash verification catches corruption
            assert r == DATA_FILE_HANDLING_CONST.FILE_HASH_MISMATCH_CODE


def test_equal_count_mtime_tiebreaker(output_dir, logger):
    """When two generations have the same chunk count but different
    hashes and mtimes, the newer mtime wins."""
    from hashlib import sha512

    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "log_file.csv"
    subdir = pathlib.PurePosixPath("test_subdir")

    gen1_data = b"R" * (chunk_size * 3)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)
    for c in gen1_chunks:
        c.file_mtime = 1000.0

    gen2_data = b"S" * (chunk_size * 3)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)
    for c in gen2_chunks:
        c.file_mtime = 2000.0

    assert gen1_hash != gen2_hash

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE

    r = dl.add_chunk(gen1_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    for i, chunk in enumerate(gen2_chunks):
        r = dl.add_chunk(chunk)
        if i < len(gen2_chunks) - 1:
            assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        else:
            assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    assert dl.bytestring == gen2_data


def test_equal_count_no_mtime_skips(output_dir, logger):
    """When mtime is unavailable (old producer), equal-count
    different-hash chunks are skipped (backward compat)."""
    from hashlib import sha512

    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "old_format.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    gen1_data = b"T" * (chunk_size * 2)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)

    gen2_data = b"U" * (chunk_size * 2)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    r = dl.add_chunk(gen1_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    assert dl.bytestring == gen1_data
