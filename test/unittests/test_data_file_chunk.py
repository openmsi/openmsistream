#imports
import unittest, pathlib, logging
from confluent_kafka.error import SerializationError
from openmsistream.shared.logging import Logger
from openmsistream.data_file_io.config import RUN_OPT_CONST
from openmsistream.data_file_io.upload_data_file import UploadDataFile
from openmsistream.data_file_io.data_file_chunk import DataFileChunk
from openmsistream.my_kafka.my_producer import MyProducer
from config import TEST_CONST

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.ERROR)

class TestDataFileChunk(unittest.TestCase) :
    """
    Class for testing behavior of DataFileChunks
    """

    def setUp(self) :
        #use a DataFile to get a couple chunks to test
        udf = UploadDataFile(TEST_CONST.TEST_DATA_FILE_PATH,
                             rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,logger=LOGGER)
        udf._build_list_of_file_chunks(RUN_OPT_CONST.DEFAULT_CHUNK_SIZE)
        udf.add_chunks_to_upload()
        self.test_chunk_1 = udf.chunks_to_upload[0]
        self.test_chunk_2 = udf.chunks_to_upload[1]
        self.test_chunk_1.populate_with_file_data(logger=LOGGER)
        self.test_chunk_2.populate_with_file_data(logger=LOGGER)

    def test_produce_to_topic_kafka(self) :
        producer = MyProducer.from_file(TEST_CONST.TEST_CONFIG_FILE_PATH,logger=LOGGER)
        producer.produce(topic=RUN_OPT_CONST.DEFAULT_TOPIC_NAME,
                         key=self.test_chunk_1.msg_key,value=self.test_chunk_1.msg_value)
        producer.flush()
        producer.produce(topic=RUN_OPT_CONST.DEFAULT_TOPIC_NAME,
                         key=self.test_chunk_2.msg_key,value=self.test_chunk_2.msg_value)
        producer.flush()

    def test_chunk_of_nonexistent_file_kafka(self) :
        nonexistent_file_path = pathlib.Path(__file__).parent / 'never_name_a_file_this.txt'
        self.assertFalse(nonexistent_file_path.is_file())
        chunk_to_fail = DataFileChunk(nonexistent_file_path,nonexistent_file_path.name,
                                      self.test_chunk_1.file_hash,self.test_chunk_1.chunk_hash,
                                      self.test_chunk_1.chunk_offset_read,self.test_chunk_1.chunk_offset_write,
                                      self.test_chunk_1.chunk_size,
                                      self.test_chunk_1.chunk_i,self.test_chunk_1.n_total_chunks)
        LOGGER.set_stream_level(logging.INFO)
        LOGGER.info('\nExpecting two errors below:')
        LOGGER.set_stream_level(logging.ERROR)
        with self.assertRaises(FileNotFoundError) :
            chunk_to_fail.populate_with_file_data(logger=LOGGER)
        producer = MyProducer.from_file(TEST_CONST.TEST_CONFIG_FILE_PATH,logger=LOGGER)
        with self.assertRaises(SerializationError) :
            producer.produce(topic=RUN_OPT_CONST.DEFAULT_TOPIC_NAME,
                             key=chunk_to_fail.msg_key,value=chunk_to_fail.msg_value)

    def test_eq(self) :
        test_chunk_1_copied_no_data = DataFileChunk(self.test_chunk_1.filepath,self.test_chunk_1.filename,
                                                    self.test_chunk_1.file_hash,self.test_chunk_1.chunk_hash,
                                                    self.test_chunk_1.chunk_offset_read,
                                                    self.test_chunk_1.chunk_offset_write,
                                                    self.test_chunk_1.chunk_size,
                                                    self.test_chunk_1.chunk_i,self.test_chunk_1.n_total_chunks)
        test_chunk_2_copied_no_data = DataFileChunk(self.test_chunk_2.filepath,self.test_chunk_2.filename,
                                                    self.test_chunk_2.file_hash,self.test_chunk_2.chunk_hash,
                                                    self.test_chunk_2.chunk_offset_read,
                                                    self.test_chunk_2.chunk_offset_write,
                                                    self.test_chunk_2.chunk_size,
                                                    self.test_chunk_2.chunk_i,self.test_chunk_2.n_total_chunks)
        self.assertNotEqual(self.test_chunk_1,test_chunk_1_copied_no_data)
        self.assertNotEqual(self.test_chunk_2,test_chunk_2_copied_no_data)
        self.assertNotEqual(self.test_chunk_1,self.test_chunk_2)
        self.assertNotEqual(test_chunk_1_copied_no_data,test_chunk_2_copied_no_data)
        test_chunk_1_copied = DataFileChunk(self.test_chunk_1.filepath,self.test_chunk_1.filename,
                                            self.test_chunk_1.file_hash,self.test_chunk_1.chunk_hash,
                                            self.test_chunk_1.chunk_offset_read,self.test_chunk_1.chunk_offset_write,
                                            self.test_chunk_1.chunk_size,self.test_chunk_1.chunk_i,
                                            self.test_chunk_1.n_total_chunks,rootdir=self.test_chunk_1.rootdir,
                                            filename_append=self.test_chunk_1.filename_append,
                                            data=self.test_chunk_1.data)
        test_chunk_2_copied = DataFileChunk(self.test_chunk_2.filepath,self.test_chunk_2.filename,
                                            self.test_chunk_2.file_hash,self.test_chunk_2.chunk_hash,
                                            self.test_chunk_2.chunk_offset_read,self.test_chunk_2.chunk_offset_write,
                                            self.test_chunk_2.chunk_size,self.test_chunk_2.chunk_i,
                                            self.test_chunk_2.n_total_chunks,rootdir=self.test_chunk_1.rootdir,
                                            filename_append=self.test_chunk_2.filename_append,
                                            data=self.test_chunk_2.data)
        self.assertEqual(self.test_chunk_1,test_chunk_1_copied)
        self.assertEqual(self.test_chunk_2,test_chunk_2_copied)
        self.assertFalse(self.test_chunk_1==2)
        self.assertFalse(self.test_chunk_1=='this is a string, not a DataFileChunk!')

    def test_props(self) :
        self.assertEqual(self.test_chunk_1.subdir_str,TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME)
        self.assertEqual(self.test_chunk_2.subdir_str,TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME)
        subdir_as_path = pathlib.Path('').joinpath(*(pathlib.PurePosixPath(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME).parts))
        copied_as_downloaded_1 = DataFileChunk(subdir_as_path/self.test_chunk_1.filename,self.test_chunk_1.filename,
                                               self.test_chunk_1.file_hash,self.test_chunk_1.chunk_hash,
                                               self.test_chunk_1.chunk_offset_read,self.test_chunk_1.chunk_offset_write,
                                               self.test_chunk_1.chunk_size,
                                               self.test_chunk_1.chunk_i,self.test_chunk_1.n_total_chunks,
                                               filename_append=self.test_chunk_1.filename_append,
                                               data=self.test_chunk_1.data)
        copied_as_downloaded_2 = DataFileChunk(subdir_as_path/self.test_chunk_2.filename,self.test_chunk_2.filename,
                                               self.test_chunk_2.file_hash,self.test_chunk_2.chunk_hash,
                                               self.test_chunk_2.chunk_offset_read,self.test_chunk_2.chunk_offset_write,
                                               self.test_chunk_2.chunk_size,
                                               self.test_chunk_2.chunk_i,self.test_chunk_2.n_total_chunks,
                                               filename_append=self.test_chunk_1.filename_append,
                                               data=self.test_chunk_2.data)
        self.assertIsNone(copied_as_downloaded_1.rootdir)
        self.assertIsNone(copied_as_downloaded_2.rootdir)
        self.assertEqual(copied_as_downloaded_1.subdir_str,subdir_as_path.as_posix())
        self.assertEqual(copied_as_downloaded_2.subdir_str,subdir_as_path.as_posix())
        copied_as_downloaded_1.rootdir = copied_as_downloaded_1.filepath.parent
        self.assertEqual(copied_as_downloaded_1.subdir_str,'')
        copied_as_downloaded_2.rootdir = TEST_CONST.TEST_DATA_DIR_PATH/TEST_CONST.TEST_DATA_FILE_ROOT_DIR_NAME
        self.assertEqual(copied_as_downloaded_2.rootdir,self.test_chunk_2.rootdir)
        self.assertEqual(copied_as_downloaded_2.filepath,self.test_chunk_2.filepath)
