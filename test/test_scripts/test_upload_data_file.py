import pytest
from queue import Queue
from openmsistream.utilities.config import RUN_CONST
from openmsistream.data_file_io.entity.upload_data_file import UploadDataFile

from .config import TEST_CONST  # works both relative/absolute depending on your layout


@pytest.fixture
def datafile(logger):
    """Construct a fresh UploadDataFile for each test."""
    return UploadDataFile(
        TEST_CONST.TEST_DATA_FILE_PATH,
        rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
        logger=logger,
    )


@pytest.mark.parametrize(
    "kafka_topics",
    [{RUN_CONST.DEFAULT_TOPIC_NAME: {}}],
    indirect=True,
)
@pytest.mark.usefixtures("kafka_topics")
def test_upload_whole_file_kafka(datafile):
    """
    Just ensure the upload runs without error.
    """
    datafile.upload_whole_file(
        TEST_CONST.TEST_CFG_FILE_PATH,
        RUN_CONST.DEFAULT_TOPIC_NAME,
        n_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS,
        chunk_size=TEST_CONST.TEST_CHUNK_SIZE,
    )


def test_initial_properties(datafile):
    """
    Make sure datafiles are initialized with the right properties.
    """
    assert datafile.filename == TEST_CONST.TEST_DATA_FILE_NAME
    assert datafile.to_upload is True
    assert datafile.fully_enqueued is False
    assert datafile.waiting_to_upload is True
    assert datafile.upload_in_progress is False


def test_enqueue_chunks_for_upload(datafile):
    """
    Test enqueueing chunks to be uploaded.
    """

    # ---------- FULL QUEUE SHOULD NOT CHANGE ----------
    full_queue = Queue(maxsize=3)
    items = [
        "I am going to",
        "fill this Queue completely",
        "so giving it to enqueue_chunks_for_upload should not change it!",
    ]
    for x in items:
        full_queue.put(x)

    datafile.enqueue_chunks_for_upload(full_queue)

    assert full_queue.get() == items[0]
    assert full_queue.get() == items[1]
    assert full_queue.get() == items[2]
    assert full_queue.qsize() == 0

    # ---------- REAL QUEUE CASE ----------
    real_queue = Queue()

    # n_threads=0 should only build the chunks list, enqueue none
    datafile.enqueue_chunks_for_upload(real_queue, n_threads=0)

    n_total_chunks = len(datafile.chunks_to_upload)

    assert datafile.waiting_to_upload is False
    assert datafile.upload_in_progress is True
    assert datafile.fully_enqueued is False

    # Call multiple times until fully enqueued
    datafile.enqueue_chunks_for_upload(
        real_queue, n_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS
    )
    datafile.enqueue_chunks_for_upload(
        real_queue, n_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS
    )
    datafile.enqueue_chunks_for_upload(
        real_queue, n_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS
    )

    # Final call with no n_threads default
    datafile.enqueue_chunks_for_upload(real_queue)

    assert real_queue.qsize() == n_total_chunks
    assert datafile.waiting_to_upload is False
    assert datafile.upload_in_progress is False
    assert datafile.fully_enqueued is True

    # Should do nothing when already fully enqueued
    datafile.enqueue_chunks_for_upload(real_queue)
