#imports
import unittest, time, pathlib, logging, shutil
from openmsistream.utilities.logging import Logger
from openmsistream.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream.data_file_io.config import RUN_OPT_CONST
from openmsistream.data_file_io.actor.file_registry.stream_handler_registries import StreamProcessorRegistry
from openmsistream import DataFileUploadDirectory
from config import TEST_CONST
from test_data_file_stream_processor import DataFileStreamProcessorForTesting

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.INFO)
UPDATE_SECS = 5
TIMEOUT_SECS = 300
JOIN_TIMEOUT_SECS = 30

class TestDataFileStreamProcessorEncyrpted(unittest.TestCase) :
    """
    Class for testing behavior of an encrypted DataFileStreamProcessor
    """

    def test_data_file_stream_processor_restart_encrypted_kafka(self) :
        """
        Test restarting an encrypted DataFileStreamProcessor after failing to process a file
        """
        to_remove = [
            TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART_ENCRYPTED,
            TEST_CONST.TEST_STREAM_PROC_WATCHED_DIR_PATH_ENCRYPTED,
            ]
        for remove_dir in to_remove :
            if remove_dir.is_dir() :
                shutil.rmtree(remove_dir)
        TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES['test_data_file_stream_processor_restart_encrypted_kafka']
        CONSUMER_GROUP_ID = 'test_data_file_stream_processor_restart_encrypted'
        #make the directory to watch
        watched_subdir = TEST_CONST.TEST_STREAM_PROC_WATCHED_DIR_PATH_ENCRYPTED/TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME
        watched_subdir.mkdir(parents=True)
        #start up the DataFileUploadDirectory
        dfud = DataFileUploadDirectory(TEST_CONST.TEST_STREAM_PROC_WATCHED_DIR_PATH_ENCRYPTED,
                                       TEST_CONST.TEST_CFG_FILE_PATH_ENC,
                                       update_secs=UPDATE_SECS,logger=LOGGER)
        #start upload_files_as_added in a separate thread so we can time it out
        upload_thread = ExceptionTrackingThread(
            target=dfud.upload_files_as_added,
            args=(TOPIC_NAME,),
            kwargs={
                'n_threads':RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                'chunk_size':16*TEST_CONST.TEST_CHUNK_SIZE,
                'max_queue_size':RUN_OPT_CONST.DEFAULT_MAX_UPLOAD_QUEUE_MEGABYTES,
                'upload_existing':True,
                }
            )
        upload_thread.start()
        #wait a bit, copy the test files into the watched directory, and wait again
        time.sleep(5)
        fp = watched_subdir/TEST_CONST.TEST_DATA_FILE_NAME
        fp.write_bytes(TEST_CONST.TEST_DATA_FILE_PATH.read_bytes())
        fp = watched_subdir.parent/TEST_CONST.TEST_DATA_FILE_2_NAME
        fp.write_bytes(TEST_CONST.TEST_DATA_FILE_2_PATH.read_bytes())
        time.sleep(5)
        #Use a stream processor to read their data back into memory one time, deliberately failing the first file
        dfsp = DataFileStreamProcessorForTesting(TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
                                                 TOPIC_NAME,
                                                 output_dir=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART_ENCRYPTED,
                                                 n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                                                 consumer_group_id=CONSUMER_GROUP_ID,
                                                 logger=LOGGER,
                                                )
        dfsp.filenames_to_fail = [TEST_CONST.TEST_DATA_FILE_NAME]
        stream_thread = ExceptionTrackingThread(target=dfsp.process_files_as_read)
        stream_thread.start()
        time.sleep(10)
        try :
            current_messages_read = -1
            time_waited = 0
            msg = f'Waiting to read test files from "{TOPIC_NAME}" in test_data_file_stream_processor_restart_encrypted'
            msg+= f' (will timeout after {TIMEOUT_SECS} seconds)...'
            LOGGER.info(msg)
            while ( ( (TEST_CONST.TEST_DATA_FILE_NAME not in [t[0] for t in dfsp.completed_filenames_bytestrings]) or
                    (TEST_CONST.TEST_DATA_FILE_2_NAME not in [t[0] for t in dfsp.completed_filenames_bytestrings]) ) and 
                    time_waited<TIMEOUT_SECS ) :
                current_messages_read = dfsp.n_msgs_read
                LOGGER.info(f'\t{current_messages_read} messages read after waiting {time_waited} seconds....')
                time.sleep(5)
                time_waited+=5
            time.sleep(3)
            msg = 'Quitting download thread in test_data_file_stream_processor_restart_encrypted after processing '
            msg+= f'{dfsp.n_msgs_read} messages; will timeout after {JOIN_TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            dfsp.control_command_queue.put('q')
            stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if stream_thread.is_alive() :
                errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart_encrypted timed out after '
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
            time.sleep(5)
            dfsp.file_registry.in_progress_table.dump_to_file()
            dfsp.file_registry.succeeded_table.dump_to_file()
            spr = StreamProcessorRegistry(
                dirpath=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART_ENCRYPTED/DataFileStreamProcessorForTesting.LOG_SUBDIR_NAME,
                topic_name=TOPIC_NAME,
                consumer_group_id=CONSUMER_GROUP_ID,
                logger=LOGGER
            )
            self.assertEqual(len(spr.filepaths_to_rerun),1)
            in_prog_table = spr.in_progress_table
            in_prog_entries = in_prog_table.obj_addresses_by_key_attr('status')
            succeeded_table = spr.succeeded_table
            succeeded_entries = succeeded_table.obj_addresses
            self.assertTrue(len(succeeded_entries)>=1) #allow greater than in case of a previously-failed test
            self.assertEqual(len(in_prog_entries[spr.FAILED]),1)
        except Exception as e :
            try :
                if upload_thread.is_alive() :
                    dfud.control_command_queue.put('q')
                    upload_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    dfud.close()
                    if upload_thread.is_alive() :
                        errmsg = 'ERROR: upload thread in test_data_file_stream_processor_restart_encrypted '
                        errmsg+= f'timed out after {JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
            except Exception :
                pass
            raise e
        finally :
            try :
                if stream_thread.is_alive() :
                    dfsp.control_command_queue.put('q')
                    stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    dfsp.close()
                    if stream_thread.is_alive() :
                        errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart_encrypted '
                        errmsg+= f'timed out after {JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
            except Exception as e :
                raise e
        #upload a third file (fake config file)
        third_filepath = TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH
        time.sleep(1)
        fp = watched_subdir.parent/third_filepath.name
        fp.write_bytes(third_filepath.read_bytes())
        time.sleep(1)
        #recreate and re-run the stream processor, allowing it to successfully process all files this time
        time.sleep(1.0)
        dfsp = DataFileStreamProcessorForTesting(TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
                                                 TOPIC_NAME,
                                                 output_dir=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART_ENCRYPTED,
                                                 n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                                                 consumer_group_id=CONSUMER_GROUP_ID,
                                                 logger=LOGGER,
                                                )
        stream_thread = ExceptionTrackingThread(target=dfsp.process_files_as_read)
        stream_thread.start()
        try :
            current_messages_read = -1
            time_waited = 0
            msg = f'Waiting to read test files from "{TOPIC_NAME}" in test_data_file_stream_processor_restart_encrypted '
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
            msg = 'Quitting download thread in test_data_file_stream_processor_restart_encrypted after processing '
            msg+= f'{dfsp.n_msgs_read} messages; will timeout after {JOIN_TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            dfsp.control_command_queue.put('q')
            stream_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if stream_thread.is_alive() :
                errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart_encrypted timed out after '
                errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            else :
                dfsp.close()
            #wait for the uploading thread to complete
            dfud.control_command_queue.put('q')
            upload_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if upload_thread.is_alive() :
                errmsg = 'ERROR: upload thread in test_data_file_stream_processor_restart_encrypted '
                errmsg+= f'timed out after {JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            else :
                dfud.close()
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
                dirpath=TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART_ENCRYPTED/DataFileStreamProcessorForTesting.LOG_SUBDIR_NAME,
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
                        errmsg = 'ERROR: download thread in test_data_file_stream_processor_restart_encrypted '
                        errmsg+= f'timed out after {JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
            if upload_thread.is_alive() :
                try :
                    dfud.control_command_queue.put('q')
                    upload_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    dfud.close()
                    if upload_thread.is_alive() :
                        errmsg = 'ERROR: upload thread in test_data_file_stream_processor_restart_encrypted '
                        errmsg+= f'timed out after {JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
        shutil.rmtree(TEST_CONST.TEST_STREAM_PROC_WATCHED_DIR_PATH_ENCRYPTED)
        shutil.rmtree(TEST_CONST.TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART_ENCRYPTED)
