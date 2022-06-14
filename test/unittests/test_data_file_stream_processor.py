#imports
import unittest, time, pathlib, logging
from openmsistream.shared.logging import Logger
from openmsistream.shared.my_thread import MyThread
from openmsistream.data_file_io.config import RUN_OPT_CONST
from openmsistream.data_file_io.upload_data_file import UploadDataFile
from openmsistream.data_file_io.data_file_stream_processor import DataFileStreamProcessor
from config import TEST_CONST

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.ERROR)
TIMEOUT_SECS = 30
JOIN_TIMEOUT_SECS = 30
TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[:-len('.py')]]

class DataFileStreamProcessorForTesting(DataFileStreamProcessor) :
    """
    Class to use for testing DataFileStreamProcessor functions since it's an abstract base class
    """

    def __init__(self,*args,**kwargs) :
        self.checked = False
        self.completed_filenames_bytestrings = []
        super().__init__(*args,**kwargs)

    def _process_downloaded_data_file(self,datafile,lock) :
        with lock :
            self.completed_filenames_bytestrings.append((datafile.filename,datafile.bytestring))
        return None

    def _on_check(self) :
        self.checked = True
        super()._on_check()

class TestDataFileStreamProcessor(unittest.TestCase) :
    """
    Class for testing behavior of a DataFileStreamProcessor
    """

    def test_data_file_stream_processor_kafka(self) :
        """
        Upload a data file and then use a DataFileStreamProcessor to read its data back
        """
        #upload the data file
        upload_datafile = UploadDataFile(TEST_CONST.TEST_DATA_FILE_2_PATH,
                                         rootdir=TEST_CONST.TEST_DATA_DIR_PATH,logger=LOGGER)
        upload_datafile.upload_whole_file(TEST_CONST.TEST_CONFIG_FILE_PATH,TOPIC_NAME,
                                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                                          chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE)
        #Use a stream processor to read its data back into memory
        dfsp = DataFileStreamProcessorForTesting(TEST_CONST.TEST_CONFIG_FILE_PATH,
                                                 TOPIC_NAME,
                                                 n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                                                 consumer_group_ID='test_data_file_stream_processor',
                                                 logger=LOGGER,
                                                )
        stream_thread = MyThread(target=dfsp.process_files_as_read)
        stream_thread.start()
        try :
            self.assertFalse(dfsp.checked)
            dfsp.control_command_queue.put('c')
            dfsp.control_command_queue.put('check')
            time.sleep(1)
            self.assertTrue(dfsp.checked)
            current_messages_read = -1
            time_waited = 0
            LOGGER.set_stream_level(logging.INFO)
            msg = f'Waiting to read other test file from the "{TOPIC_NAME}" topic in test_data_file_stream_processor '
            msg+= f'(will timeout after {TIMEOUT_SECS} seconds)...'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            while ( (TEST_CONST.TEST_DATA_FILE_2_NAME not in [t[0] for t in dfsp.completed_filenames_bytestrings]) and 
                    time_waited<TIMEOUT_SECS ) :
                current_messages_read = dfsp.n_msgs_read
                LOGGER.set_stream_level(logging.INFO)
                LOGGER.info(f'\t{current_messages_read} messages read after waiting {time_waited} seconds....')
                LOGGER.set_stream_level(logging.ERROR)
                time.sleep(5)
                time_waited+=5
            LOGGER.set_stream_level(logging.INFO)
            msg = 'Quitting download thread in test_data_file_stream_processor after processing '
            msg+= f'{dfsp.n_msgs_read} messages; will timeout after {JOIN_TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            dfsp.control_command_queue.put('q')
            dfsp.close()
            stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if stream_thread.is_alive() :
                errmsg = 'ERROR: download thread in test_data_file_stream_processor timed out after '
                errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            #make sure the contents of the file in memory are the same as the original
            ref_bytestring = None
            with open(TEST_CONST.TEST_DATA_FILE_2_PATH,'rb') as fp :
                ref_bytestring = fp.read()
            self.assertTrue((TEST_CONST.TEST_DATA_FILE_2_NAME,ref_bytestring) in dfsp.completed_filenames_bytestrings)
        except Exception as e :
            raise e
        finally :
            if stream_thread.is_alive() :
                try :
                    dfsp.control_command_queue.put('q')
                    stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    if stream_thread.is_alive() :
                        errmsg = 'ERROR: download thread in test_data_file_stream_processor timed out after '
                        errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
