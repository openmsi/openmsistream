#imports
import unittest, time, pathlib, logging, shutil
from openmsistream.utilities.logging import Logger
from openmsistream.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream.data_file_io.config import RUN_OPT_CONST
from openmsistream.data_file_io.stream_processor_registry import StreamProcessorRegistry
from openmsistream.data_file_io.upload_data_file import UploadDataFile
from openmsistream.data_file_io.data_file_stream_processor import DataFileStreamProcessor
from config import TEST_CONST

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.ERROR)
TIMEOUT_SECS = 90
JOIN_TIMEOUT_SECS = 30

class DataFileStreamProcessorForTesting(DataFileStreamProcessor) :
    """
    Class to use for testing DataFileStreamProcessor functions since it's an abstract base class
    """

    def __init__(self,*args,**kwargs) :
        self.checked = False
        self.completed_filenames_bytestrings = []
        self.filenames_to_fail = []
        super().__init__(*args,**kwargs)

    def _process_downloaded_data_file(self,datafile,lock) :
        self.logger.error(f'PROCESS FUNCTION CALLED FOR {datafile.filename}')
        if datafile.filename in self.filenames_to_fail :
            with lock :
                self.completed_filenames_bytestrings.append((datafile.filename,None))
            return ValueError(f'ERROR: testing processing for {datafile.filename} is set to fail!')
        else :
            with lock :
                self.completed_filenames_bytestrings.append((datafile.filename,datafile.bytestring))
            return None

    def _failed_processing_callback(self,datafile,lock) :
        if datafile.filename not in self.filenames_to_fail :
            raise RuntimeError('ERROR: _failed_processing_callback invoked in test!')

    def _mismatched_hash_callback(self,datafile,lock) :
        raise RuntimeError('ERROR: _mismatched_hash_callback invoked in test!')

    def _on_check(self) :
        self.checked = True
        super()._on_check()

    @classmethod
    def run_from_command_line(cls,args=None) :
        pass

class TestDataFileStreamProcessor(unittest.TestCase) :
    """
    Class for testing behavior of a DataFileStreamProcessor
    """

    def test_data_file_stream_processor_kafka(self) :
        """
        Upload a data file and then use a DataFileStreamProcessor to read its data back
        """
        TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES['test_data_file_stream_processor_kafka']
        #upload the data file
        upload_datafile = UploadDataFile(TEST_CONST.TEST_DATA_FILE_2_PATH,
                                         rootdir=TEST_CONST.TEST_DATA_DIR_PATH,logger=LOGGER)
        upload_datafile.upload_whole_file(TEST_CONST.TEST_CONFIG_FILE_PATH,TOPIC_NAME,
                                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                                          chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE)
        #Use a stream processor to read its data back into memory
        dfsp = DataFileStreamProcessorForTesting(TEST_CONST.TEST_CONFIG_FILE_PATH,
                                                 TOPIC_NAME,
                                                 output_dir=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR,
                                                 n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                                                 consumer_group_ID='test_data_file_stream_processor',
                                                 logger=LOGGER,
                                                )
        stream_thread = ExceptionTrackingThread(target=dfsp.process_files_as_read)
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
                finally :
                    shutil.rmtree(TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR)
            del dfsp
            if TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR.is_dir() :
                shutil.rmtree(TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR)

    def test_data_file_stream_processor_restart_kafka(self) :
        """
        Upload two data files. Use a DataFileStreamProcessor to successfully process one of them and fail the other.
        Completely restart the DataFileStreamProcessor and successfully process the previously-failed file.
        Make sure both files have been successfully processed in the right order, without reading new messages for the previously successful one.
        """
        TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES['test_data_file_stream_processor_restart_kafka']
        CONSUMER_GROUP_ID = 'test_data_file_stream_processor_restart'
        #upload the data files
        upload_datafile = UploadDataFile(TEST_CONST.TEST_DATA_FILE_PATH,
                                         rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,logger=LOGGER)
        upload_datafile.upload_whole_file(TEST_CONST.TEST_CONFIG_FILE_PATH,TOPIC_NAME,
                                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                                          chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE)
        upload_datafile = UploadDataFile(TEST_CONST.TEST_DATA_FILE_2_PATH,
                                         rootdir=TEST_CONST.TEST_DATA_DIR_PATH,logger=LOGGER)
        upload_datafile.upload_whole_file(TEST_CONST.TEST_CONFIG_FILE_PATH,TOPIC_NAME,
                                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                                          chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE)
        #Use a stream processor to read their data back into memory one time, deliberately failing the first file
        self.assertFalse(TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART.is_dir())
        dfsp = DataFileStreamProcessorForTesting(TEST_CONST.TEST_CONFIG_FILE_PATH,
                                                 TOPIC_NAME,
                                                 output_dir=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART,
                                                 n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                                                 consumer_group_ID=CONSUMER_GROUP_ID,
                                                 logger=LOGGER,
                                                )
        dfsp.filenames_to_fail = [TEST_CONST.TEST_DATA_FILE_NAME]
        stream_thread = ExceptionTrackingThread(target=dfsp.process_files_as_read)
        stream_thread.start()
        try :
            current_messages_read = -1
            time_waited = 0
            LOGGER.set_stream_level(logging.INFO)
            msg = f'Waiting to read test files from "{TOPIC_NAME}" in test_data_file_stream_processor_restart '
            msg+= f'(will timeout after {TIMEOUT_SECS} seconds)...'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            while ( ( (TEST_CONST.TEST_DATA_FILE_NAME not in [t[0] for t in dfsp.completed_filenames_bytestrings]) or
                    (TEST_CONST.TEST_DATA_FILE_2_NAME not in [t[0] for t in dfsp.completed_filenames_bytestrings]) ) and 
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
                errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart timed out after '
                errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            #make sure the content of the failed file has been added as "None"
            self.assertTrue((TEST_CONST.TEST_DATA_FILE_NAME,None) in dfsp.completed_filenames_bytestrings)
            #make sure the contents of the successful file in memory are the same as the original
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
                        errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart timed out after '
                        errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
        #read the .csv table to make sure it registers one file each succeeded and failed
        dfsp.file_registry.table.dump_to_file()
        time.sleep(1.0)
        spr = StreamProcessorRegistry(dirpath=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART,
                                      topic_name=TOPIC_NAME,
                                      logger=LOGGER)
        self.assertEqual(len(spr.filepaths_to_rerun),1)
        existing_entries = spr.table.obj_addresses_by_key_attr('status')
        self.assertEqual(len(existing_entries[spr.SUCCESS]),1)
        self.assertEqual(len(existing_entries[spr.FAILED]),1)
        #get the attributes of the succeeded file to make sure the entry doesn't change
        succeeded_entry_attrs = spr.table.get_entry_attrs(existing_entries[spr.SUCCESS][0])
        #upload a third file (fake config file)
        third_filepath = TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH
        upload_datafile = UploadDataFile(third_filepath,
                                         rootdir=TEST_CONST.TEST_DATA_DIR_PATH,logger=LOGGER)
        upload_datafile.upload_whole_file(TEST_CONST.TEST_CONFIG_FILE_PATH,TOPIC_NAME,
                                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                                          chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE)
        #recreate and re-run the stream processor, allowing it to successfully process all files this time
        time.sleep(1.0)
        dfsp = DataFileStreamProcessorForTesting(TEST_CONST.TEST_CONFIG_FILE_PATH,
                                                 TOPIC_NAME,
                                                 output_dir=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART,
                                                 n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                                                 consumer_group_ID=CONSUMER_GROUP_ID,
                                                 logger=LOGGER,
                                                )
        stream_thread = ExceptionTrackingThread(target=dfsp.process_files_as_read)
        stream_thread.start()
        try :
            current_messages_read = -1
            time_waited = 0
            LOGGER.set_stream_level(logging.INFO)
            msg = f'Waiting to read test files from "{TOPIC_NAME}" in test_data_file_stream_processor_restart '
            msg+= f'(will timeout after {TIMEOUT_SECS} seconds)...'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            while ( ( (TEST_CONST.TEST_DATA_FILE_NAME not in [t[0] for t in dfsp.completed_filenames_bytestrings]) or
                    (third_filepath.name not in [t[0] for t in dfsp.completed_filenames_bytestrings]) ) and 
                    time_waited<TIMEOUT_SECS ) :
                current_messages_read = dfsp.n_msgs_read
                LOGGER.set_stream_level(logging.INFO)
                LOGGER.info(f'\t{current_messages_read} messages read after waiting {time_waited} seconds....')
                LOGGER.set_stream_level(logging.ERROR)
                time.sleep(5)
                time_waited+=5
            LOGGER.set_stream_level(logging.INFO)
            msg = 'Quitting download thread in test_data_file_stream_processor_restart after processing '
            msg+= f'{dfsp.n_msgs_read} messages; will timeout after {JOIN_TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            dfsp.control_command_queue.put('q')
            dfsp.close()
            stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if stream_thread.is_alive() :
                errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart timed out after '
                errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            #make sure the content of the previously failed file is now correct
            ref_bytestring = None
            with open(TEST_CONST.TEST_DATA_FILE_PATH,'rb') as fp :
                ref_bytestring = fp.read()
            self.assertTrue((TEST_CONST.TEST_DATA_FILE_NAME,ref_bytestring) in dfsp.completed_filenames_bytestrings)
            #make sure the previously-successful file wasn't read again
            self.assertFalse(TEST_CONST.TEST_DATA_FILE_2_NAME in [t[0] for t in dfsp.completed_filenames_bytestrings])
            #make sure the contents of the third file are also accurate
            ref_bytestring = None
            with open(third_filepath,'rb') as fp :
                ref_bytestring = fp.read()
            self.assertTrue((third_filepath.name,ref_bytestring) in dfsp.completed_filenames_bytestrings)
        except Exception as e :
            raise e
        finally :
            if stream_thread.is_alive() :
                try :
                    dfsp.control_command_queue.put('q')
                    stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    if stream_thread.is_alive() :
                        errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart timed out after '
                        errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
        #read the .csv table to make sure it registers three successful files
        dfsp.file_registry.table.dump_to_file()
        time.sleep(1.0)
        spr = StreamProcessorRegistry(dirpath=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART,
                                      topic_name=TOPIC_NAME,
                                      logger=LOGGER)
        self.assertEqual(len(spr.filepaths_to_rerun),0)
        existing_entries = spr.table.obj_addresses_by_key_attr('status')
        self.assertEqual(len(existing_entries[spr.SUCCESS]),3)
        #get the attributes of the originally succeeded file to make sure the entry hasn't changed
        existing_entries = spr.table.obj_addresses_by_key_attr('filename')
        self.assertEqual(len(existing_entries[TEST_CONST.TEST_DATA_FILE_2_NAME]),1)
        testing_entry_attrs = spr.table.get_entry_attrs(existing_entries[TEST_CONST.TEST_DATA_FILE_2_NAME][0])
        self.assertEqual(succeeded_entry_attrs,testing_entry_attrs)
        dfsp.file_registry.table.dump_to_file()
        del dfsp
        time.sleep(1.0)
        shutil.rmtree(TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART)
