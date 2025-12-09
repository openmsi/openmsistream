import pathlib
import pytest
from confluent_kafka.error import SerializationError
from openmsistream.utilities.config import RUN_CONST
from openmsistream.data_file_io.entity.upload_data_file import UploadDataFile
from openmsistream.data_file_io.entity.data_file_chunk import DataFileChunk
from openmsistream.kafka_wrapper.openmsistream_producer import OpenMSIStreamProducer

from .config import TEST_CONST


@pytest.mark.parametrize(
    "kafka_topics",
    [{RUN_CONST.DEFAULT_TOPIC_NAME: {}}], 
    indirect=True
)
@pytest.mark.usefixtures("logger", "kafka_topics")
class TestDataFileChunk:
    """
    Tests for DataFileChunk using pytest fixtures.
    """

    @pytest.fixture(autouse=True)
    def setup_chunks(self, logger):
        """
        Replacement for unittest setUp().
        Creates chunk_1 and chunk_2 for every test.
        """
        udf = UploadDataFile(
            TEST_CONST.TEST_DATA_FILE_PATH,
            rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
            logger=logger,
        )
        udf._build_list_of_file_chunks(TEST_CONST.TEST_CHUNK_SIZE)
        udf.add_chunks_to_upload()

        self.test_chunk_1 = udf.chunks_to_upload[0]
        self.test_chunk_2 = udf.chunks_to_upload[1]

        self.test_chunk_1.populate_with_file_data(logger=logger)
        self.test_chunk_2.populate_with_file_data(logger=logger)

    def test_produce_to_topic_kafka(self, logger):
        producer = OpenMSIStreamProducer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH, logger=logger
        )

        producer.produce(
            topic=RUN_CONST.DEFAULT_TOPIC_NAME,
            key=self.test_chunk_1.msg_key,
            value=self.test_chunk_1.msg_value,
        )
        producer.flush()

        producer.produce(
            topic=RUN_CONST.DEFAULT_TOPIC_NAME,
            key=self.test_chunk_2.msg_key,
            value=self.test_chunk_2.msg_value,
        )
        producer.flush()

    def test_chunk_of_nonexistent_file_kafka(self, logger):
        nonexistent_file_path = pathlib.Path(__file__).parent / "never_name_a_file_this.txt"
        assert not nonexistent_file_path.is_file()

        chunk_to_fail = DataFileChunk(
            nonexistent_file_path,
            nonexistent_file_path.name,
            self.test_chunk_1.file_hash,
            self.test_chunk_1.chunk_hash,
            self.test_chunk_1.chunk_offset_read,
            self.test_chunk_1.chunk_offset_write,
            self.test_chunk_1.chunk_size,
            self.test_chunk_1.chunk_i,
            self.test_chunk_1.n_total_chunks,
        )

        with pytest.raises(FileNotFoundError):
            chunk_to_fail.populate_with_file_data(logger=logger)

        producer = OpenMSIStreamProducer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH, logger=logger
        )

        with pytest.raises(SerializationError):
            producer.produce(
                topic=RUN_CONST.DEFAULT_TOPIC_NAME,
                key=chunk_to_fail.msg_key,
                value=chunk_to_fail.msg_value,
            )

    def test_eq(self):
        test_chunk_1_copy_no_data = DataFileChunk(
            self.test_chunk_1.filepath,
            self.test_chunk_1.filename,
            self.test_chunk_1.file_hash,
            self.test_chunk_1.chunk_hash,
            self.test_chunk_1.chunk_offset_read,
            self.test_chunk_1.chunk_offset_write,
            self.test_chunk_1.chunk_size,
            self.test_chunk_1.chunk_i,
            self.test_chunk_1.n_total_chunks,
        )

        test_chunk_2_copy_no_data = DataFileChunk(
            self.test_chunk_2.filepath,
            self.test_chunk_2.filename,
            self.test_chunk_2.file_hash,
            self.test_chunk_2.chunk_hash,
            self.test_chunk_2.chunk_offset_read,
            self.test_chunk_2.chunk_offset_write,
            self.test_chunk_2.chunk_size,
            self.test_chunk_2.chunk_i,
            self.test_chunk_2.n_total_chunks,
        )

        assert self.test_chunk_1 != test_chunk_1_copy_no_data
        assert self.test_chunk_2 != test_chunk_2_copy_no_data
        assert self.test_chunk_1 != self.test_chunk_2
        assert test_chunk_1_copy_no_data != test_chunk_2_copy_no_data

        test_chunk_1_copy = DataFileChunk(
            self.test_chunk_1.filepath,
            self.test_chunk_1.filename,
            self.test_chunk_1.file_hash,
            self.test_chunk_1.chunk_hash,
            self.test_chunk_1.chunk_offset_read,
            self.test_chunk_1.chunk_offset_write,
            self.test_chunk_1.chunk_size,
            self.test_chunk_1.chunk_i,
            self.test_chunk_1.n_total_chunks,
            rootdir=self.test_chunk_1.rootdir,
            filename_append=self.test_chunk_1.filename_append,
            data=self.test_chunk_1.data,
        )

        test_chunk_2_copy = DataFileChunk(
            self.test_chunk_2.filepath,
            self.test_chunk_2.filename,
            self.test_chunk_2.file_hash,
            self.test_chunk_2.chunk_hash,
            self.test_chunk_2.chunk_offset_read,
            self.test_chunk_2.chunk_offset_write,
            self.test_chunk_2.chunk_size,
            self.test_chunk_2.chunk_i,
            self.test_chunk_2.n_total_chunks,
            rootdir=self.test_chunk_1.rootdir,
            filename_append=self.test_chunk_2.filename_append,
            data=self.test_chunk_2.data,
        )

        assert self.test_chunk_1 == test_chunk_1_copy
        assert self.test_chunk_2 == test_chunk_2_copy
        assert not (self.test_chunk_1 == 2)
        assert not (self.test_chunk_1 == "this is a string, not a DataFileChunk!")

    def test_props(self):
        assert self.test_chunk_1.subdir_str == TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME
        assert self.test_chunk_2.subdir_str == TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME

        subdir_as_path = pathlib.Path("").joinpath(
            *(pathlib.PurePosixPath(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME).parts)
        )

        copied_as_downloaded_1 = DataFileChunk(
            subdir_as_path / self.test_chunk_1.filename,
            self.test_chunk_1.filename,
            self.test_chunk_1.file_hash,
            self.test_chunk_1.chunk_hash,
            self.test_chunk_1.chunk_offset_read,
            self.test_chunk_1.chunk_offset_write,
            self.test_chunk_1.chunk_size,
            self.test_chunk_1.chunk_i,
            self.test_chunk_1.n_total_chunks,
            filename_append=self.test_chunk_1.filename_append,
            data=self.test_chunk_1.data,
        )

        copied_as_downloaded_2 = DataFileChunk(
            subdir_as_path / self.test_chunk_2.filename,
            self.test_chunk_2.filename,
            self.test_chunk_2.file_hash,
            self.test_chunk_2.chunk_hash,
            self.test_chunk_2.chunk_offset_read,
            self.test_chunk_2.chunk_offset_write,
            self.test_chunk_2.chunk_size,
            self.test_chunk_2.chunk_i,
            self.test_chunk_2.n_total_chunks,
            filename_append=self.test_chunk_1.filename_append,
            data=self.test_chunk_2.data,
        )

        assert copied_as_downloaded_1.rootdir is None
        assert copied_as_downloaded_2.rootdir is None

        assert copied_as_downloaded_1.subdir_str == subdir_as_path.as_posix()
        assert copied_as_downloaded_2.subdir_str == subdir_as_path.as_posix()

        copied_as_downloaded_1.rootdir = copied_as_downloaded_1.filepath.parent
        assert copied_as_downloaded_1.subdir_str == ""

        copied_as_downloaded_2.rootdir = (
            TEST_CONST.TEST_DATA_DIR_PATH / TEST_CONST.TEST_DATA_FILE_ROOT_DIR_NAME
        )
        assert copied_as_downloaded_2.rootdir == self.test_chunk_2.rootdir
        assert copied_as_downloaded_2.filepath == self.test_chunk_2.filepath
