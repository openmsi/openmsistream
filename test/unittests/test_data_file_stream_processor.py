#imports
import unittest, time, pathlib, logging, shutil
from openmsistream.utilities.logging import Logger
from openmsistream.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream.data_file_io.config import RUN_OPT_CONST
from openmsistream.data_file_io.actor.file_registry.stream_handler_registries import StreamProcessorRegistry
from openmsistream import UploadDataFile, DataFileStreamProcessor
from config import TEST_CONST

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.INFO)
UPDATE_SECS = 5
TIMEOUT_SECS = 180
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
        upload_datafile.upload_whole_file(TEST_CONST.TEST_CFG_FILE_PATH,TOPIC_NAME,
                                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                                          chunk_size=TEST_CONST.TEST_CHUNK_SIZE)
        #Use a stream processor to read its data back into memory
        dfsp = DataFileStreamProcessorForTesting(TEST_CONST.TEST_CFG_FILE_PATH,
                                                 TOPIC_NAME,
                                                 output_dir=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR,
                                                 n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                                                 consumer_group_id='test_data_file_stream_processor',
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
            msg = f'Waiting to read other test file from the "{TOPIC_NAME}" topic in test_data_file_stream_processor '
            msg+= f'(will timeout after {TIMEOUT_SECS} seconds)...'
            LOGGER.info(msg)
            while ( (TEST_CONST.TEST_DATA_FILE_2_NAME not in [t[0] for t in dfsp.completed_filenames_bytestrings]) and 
                    time_waited<TIMEOUT_SECS ) :
                current_messages_read = dfsp.n_msgs_read
                LOGGER.info(f'\t{current_messages_read} messages read after waiting {time_waited} seconds....')
                time.sleep(5)
                time_waited+=5
            msg = 'Quitting download thread in test_data_file_stream_processor after processing '
            msg+= f'{dfsp.n_msgs_read} messages; will timeout after {JOIN_TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            dfsp.control_command_queue.put('q')
            stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if stream_thread.is_alive() :
                errmsg = 'ERROR: download thread in test_data_file_stream_processor timed out after '
                errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            else :
                dfsp.close()
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
                    dfsp.close()
                    if stream_thread.is_alive() :
                        errmsg = 'ERROR: download thread in test_data_file_stream_processor timed out after '
                        errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
        if TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR.is_dir() :
            shutil.rmtree(TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR)

    def test_data_file_stream_processor_restart_kafka(self) :
        """
        Test restarting a DataFileStreamProcessor from the beginning of the topic after failing to process a file
        """
        if TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART.is_dir() :
            shutil.rmtree(TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART)
        TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES['test_data_file_stream_processor_restart_kafka']
        CONSUMER_GROUP_ID = 'test_data_file_stream_processor_restart'
        #upload the data files
        upload_datafile = UploadDataFile(TEST_CONST.TEST_DATA_FILE_PATH,
                                         rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,logger=LOGGER)
        upload_datafile.upload_whole_file(TEST_CONST.TEST_CFG_FILE_PATH,TOPIC_NAME,
                                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                                          chunk_size=TEST_CONST.TEST_CHUNK_SIZE)
        upload_datafile = UploadDataFile(TEST_CONST.TEST_DATA_FILE_2_PATH,
                                         rootdir=TEST_CONST.TEST_DATA_DIR_PATH,logger=LOGGER)
        upload_datafile.upload_whole_file(TEST_CONST.TEST_CFG_FILE_PATH,TOPIC_NAME,
                                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                                          chunk_size=TEST_CONST.TEST_CHUNK_SIZE)
        #Use a stream processor to read their data back into memory one time, deliberately failing the first file
        time.sleep(1.0)
        dfsp = DataFileStreamProcessorForTesting(TEST_CONST.TEST_CFG_FILE_PATH,
                                                 TOPIC_NAME,
                                                 output_dir=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART,
                                                 n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                                                 consumer_group_id=CONSUMER_GROUP_ID,
                                                 logger=LOGGER,
                                                )
        dfsp.filenames_to_fail = [TEST_CONST.TEST_DATA_FILE_NAME]
        stream_thread = ExceptionTrackingThread(target=dfsp.process_files_as_read)
        stream_thread.start()
        try :
            current_messages_read = -1
            time_waited = 0
            msg = f'Waiting to read test files from "{TOPIC_NAME}" in test_data_file_stream_processor_restart '
            msg+= f'(will timeout after {TIMEOUT_SECS} seconds)...'
            LOGGER.info(msg)
            while ( ( (TEST_CONST.TEST_DATA_FILE_NAME not in [t[0] for t in dfsp.completed_filenames_bytestrings]) or
                    (TEST_CONST.TEST_DATA_FILE_2_NAME not in [t[0] for t in dfsp.completed_filenames_bytestrings]) ) and 
                    time_waited<TIMEOUT_SECS ) :
                current_messages_read = dfsp.n_msgs_read
                LOGGER.info(f'\t{current_messages_read} messages read after waiting {time_waited} seconds....')
                time.sleep(5)
                time_waited+=5
            time.sleep(3)
            msg = 'Quitting download thread in test_data_file_stream_processor after processing '
            msg+= f'{dfsp.n_msgs_read} messages; will timeout after {JOIN_TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            dfsp.control_command_queue.put('q')
            stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if stream_thread.is_alive() :
                errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart timed out after '
                errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            else :
                dfsp.close()
            #make sure the content of the failed file has been added as "None"
            self.assertTrue((TEST_CONST.TEST_DATA_FILE_NAME,None) in dfsp.completed_filenames_bytestrings)
            #make sure the contents of the successful file in memory are the same as the original
            ref_bytestring = None
            with open(TEST_CONST.TEST_DATA_FILE_2_PATH,'rb') as fp :
                ref_bytestring = fp.read()
            self.assertTrue((TEST_CONST.TEST_DATA_FILE_2_NAME,ref_bytestring) in dfsp.completed_filenames_bytestrings)
             #read the .csv table to make sure it registers one file each succeeded and failed
            time.sleep(1.0)
            dfsp.file_registry.in_progress_table.dump_to_file()
            dfsp.file_registry.succeeded_table.dump_to_file()
            spr = StreamProcessorRegistry(
                dirpath=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART/DataFileStreamProcessor.LOG_SUBDIR_NAME,
                topic_name=TOPIC_NAME,
                consumer_group_id=CONSUMER_GROUP_ID,
                logger=LOGGER
            )
            self.assertEqual(len(spr.filepaths_to_rerun),1)
            in_prog_table = spr.in_progress_table
            in_prog_entries = in_prog_table.obj_addresses_by_key_attr('status')
            succeeded_table = spr.succeeded_table
            succeeded_entries = succeeded_table.obj_addresses
            self.assertEqual(len(succeeded_entries),1)
            self.assertEqual(len(in_prog_entries[spr.FAILED]),1)
        except Exception as e :
            raise e
        finally :
            if stream_thread.is_alive() :
                try :
                    dfsp.control_command_queue.put('q')
                    stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    dfsp.close()
                    if stream_thread.is_alive() :
                        errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart timed out after '
                        errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
        #upload a third file (fake config file)
        third_filepath = TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH
        upload_datafile = UploadDataFile(third_filepath,
                                         rootdir=TEST_CONST.TEST_DATA_DIR_PATH,logger=LOGGER)
        upload_datafile.upload_whole_file(TEST_CONST.TEST_CFG_FILE_PATH,TOPIC_NAME,
                                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                                          chunk_size=TEST_CONST.TEST_CHUNK_SIZE)
        #recreate and re-run the stream processor, allowing it to successfully process all files this time
        time.sleep(1.0)
        dfsp = DataFileStreamProcessorForTesting(TEST_CONST.TEST_CFG_FILE_PATH,
                                                 TOPIC_NAME,
                                                 output_dir=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART,
                                                 n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                                                 consumer_group_id=CONSUMER_GROUP_ID,
                                                 logger=LOGGER,
                                                )
        stream_thread = ExceptionTrackingThread(target=dfsp.process_files_as_read)
        stream_thread.start()
        try :
            current_messages_read = -1
            time_waited = 0
            msg = f'Waiting to read test files from "{TOPIC_NAME}" in test_data_file_stream_processor_restart '
            msg+= f'(will timeout after {TIMEOUT_SECS} seconds)...'
            LOGGER.info(msg)
            while ( ( (TEST_CONST.TEST_DATA_FILE_NAME not in [t[0] for t in dfsp.completed_filenames_bytestrings]) or
                    (third_filepath.name not in [t[0] for t in dfsp.completed_filenames_bytestrings]) ) and 
                    time_waited<TIMEOUT_SECS ) :
                current_messages_read = dfsp.n_msgs_read
                LOGGER.info(f'\t{current_messages_read} messages read after waiting {time_waited} seconds....')
                time.sleep(5)
                time_waited+=5
            time.sleep(3)
            msg = 'Quitting download thread in test_data_file_stream_processor_restart after processing '
            msg+= f'{dfsp.n_msgs_read} messages; will timeout after {JOIN_TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            dfsp.control_command_queue.put('q')
            stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if stream_thread.is_alive() :
                errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart timed out after '
                errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            else :
                dfsp.close()
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
            time.sleep(1.0)
            #read the .csv table to make sure it registers three successful files
            dfsp.file_registry.in_progress_table.dump_to_file()
            dfsp.file_registry.succeeded_table.dump_to_file()
            spr = StreamProcessorRegistry(
                dirpath=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART/DataFileStreamProcessor.LOG_SUBDIR_NAME,
                topic_name=TOPIC_NAME,
                consumer_group_id=CONSUMER_GROUP_ID,
                logger=LOGGER
            )
            succeeded_table = spr.succeeded_table
            succeeded_entries = succeeded_table.obj_addresses
            self.assertTrue(len(succeeded_entries)>=3) #>3 if the topic has files from previous runs in it
            succeeded_entries = succeeded_table.obj_addresses_by_key_attr('filename')
            self.assertTrue(len(succeeded_entries[TEST_CONST.TEST_DATA_FILE_2_NAME])>=1)
        except Exception as e :
            raise e
        finally :
            if stream_thread.is_alive() :
                try :
                    dfsp.control_command_queue.put('q')
                    stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    dfsp.close()
                    if stream_thread.is_alive() :
                        errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart timed out after '
                        errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
        shutil.rmtree(TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART)
