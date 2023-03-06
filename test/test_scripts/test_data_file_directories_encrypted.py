#imports
import unittest, pathlib, time, logging, shutil, filecmp
from openmsistream.utilities.logging import Logger
from openmsistream.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream.utilities.dataclass_table import DataclassTableReadOnly
from openmsistream.data_file_io.config import RUN_OPT_CONST
from openmsistream.data_file_io.actor.file_registry.producer_file_registry import RegistryLineInProgress, RegistryLineCompleted
from openmsistream.data_file_io.actor.data_file_upload_directory import DataFileUploadDirectory
from openmsistream.data_file_io.actor.data_file_download_directory import DataFileDownloadDirectory
from config import TEST_CONST

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.INFO)
UPDATE_SECS = 5
TIMEOUT_SECS = 300
JOIN_TIMEOUT_SECS = 60
TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[:-len('.py')]]

class TestDataFileDirectories(unittest.TestCase) :
    """
    Class for testing DataFileUploadDirectory and DataFileDownloadDirectory functions
    """

    #below we test sending and receiving encrypted messages
    def test_encrypted_upload_and_download_kafka(self) :
        #make the directory to watch
        (TEST_CONST.TEST_WATCHED_DIR_PATH_ENCRYPTED/TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME).mkdir(parents=True)
        #make the directory to reconstruct files into
        TEST_CONST.TEST_RECO_DIR_PATH_ENCRYPTED.mkdir()
        #start up the DataFileUploadDirectory
        dfud = DataFileUploadDirectory(TEST_CONST.TEST_WATCHED_DIR_PATH_ENCRYPTED,
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
        #wait a bit, copy the test file into the watched directory, and wait again
        time.sleep(5)
        fp = TEST_CONST.TEST_WATCHED_DIR_PATH_ENCRYPTED/TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME
        fp = fp/TEST_CONST.TEST_DATA_FILE_NAME
        fp.write_bytes(TEST_CONST.TEST_DATA_FILE_PATH.read_bytes())
        time.sleep(5)
        #start up the DataFileDownloadDirectory
        dfdd = DataFileDownloadDirectory(TEST_CONST.TEST_RECO_DIR_PATH_ENCRYPTED,
                                         TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
                                         TOPIC_NAME,
                                         n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                                         update_secs=UPDATE_SECS,
                                         consumer_group_id='test_encrypted_data_file_directories',
                                         logger=LOGGER,
                                         )
        #start reconstruct in a separate thread so we can time it out
        download_thread = ExceptionTrackingThread(target=dfdd.reconstruct)
        download_thread.start()
        time.sleep(10)
        try :
            #put the "check" command into the input queues a couple times to test them
            dfud.control_command_queue.put('c')
            dfdd.control_command_queue.put('c')
            dfud.control_command_queue.put('check')
            dfdd.control_command_queue.put('check')
            #wait for the timeout for the test file to be completely reconstructed 
            current_messages_read = -1
            time_waited = 0
            msg = f'Waiting to reconstruct test file from the "{TOPIC_NAME}" topic in '
            msg+= f'test_encrypted_upload_and_download (will timeout after {TIMEOUT_SECS} seconds)...'
            LOGGER.info(msg)
            reco_fp = TEST_CONST.TEST_RECO_DIR_PATH_ENCRYPTED/TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME
            reco_fp = reco_fp/TEST_CONST.TEST_DATA_FILE_NAME
            reco_rel_fp = reco_fp.relative_to(TEST_CONST.TEST_RECO_DIR_PATH_ENCRYPTED)
            while (reco_rel_fp not in dfdd.completely_processed_filepaths) and time_waited<TIMEOUT_SECS :
                current_messages_read = dfdd.n_msgs_read
                LOGGER.info(f'\t{current_messages_read} messages read after waiting {time_waited} seconds....')
                time.sleep(5)
                time_waited+=5
            #After timing out, stalling, or completely reconstructing the test file, 
            #put the "quit" command into the input queue, which SHOULD stop the method running
            msg = f'Quitting download thread in test_encrypted_upload_and_download after reading {dfdd.n_msgs_read} '
            msg+= f'messages; will timeout after {JOIN_TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            dfdd.control_command_queue.put('q')
            #wait for the download thread to finish
            download_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if download_thread.is_alive() :
                errmsg = 'ERROR: download thread in test_encrypted_upload_and_download timed out after '
                errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            dfdd.close()
            #put the quit command in the upload directory's command queue to stop the process running
            msg = '\nQuitting upload thread in test_encrypted_upload_and_download; '
            msg+= f'will timeout after {TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            dfud.control_command_queue.put('q')
            #wait for the uploading thread to complete
            upload_thread.join(timeout=TIMEOUT_SECS)
            if upload_thread.is_alive() :
                errmsg = 'ERROR: upload thread in test_encrypted_upload_and_download '
                errmsg+= f'timed out after {TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            #make sure the reconstructed file exists with the same name and content as the original
            self.assertTrue(reco_fp.is_file())
            if not filecmp.cmp(TEST_CONST.TEST_DATA_FILE_PATH,reco_fp,shallow=False) :
                errmsg = 'ERROR: files are not the same after reconstruction! '
                errmsg+= f'(This may also be due to the timeout at {TIMEOUT_SECS} seconds)'
                raise RuntimeError(errmsg)
            #make sure that the ProducerFileRegistry files were created and list the file as completely uploaded
            log_subdir = TEST_CONST.TEST_WATCHED_DIR_PATH_ENCRYPTED/DataFileUploadDirectory.LOG_SUBDIR_NAME
            in_prog_filepath = log_subdir / f'upload_to_{TOPIC_NAME}_in_progress.csv'
            completed_filepath = log_subdir / f'uploaded_to_{TOPIC_NAME}.csv'
            self.assertTrue(in_prog_filepath.is_file())
            in_prog_table = DataclassTableReadOnly(RegistryLineInProgress,filepath=in_prog_filepath,logger=LOGGER)
            self.assertTrue(in_prog_table.obj_addresses_by_key_attr('filename')=={})
            self.assertTrue(completed_filepath.is_file())
            completed_table = DataclassTableReadOnly(RegistryLineCompleted,filepath=completed_filepath,logger=LOGGER)
            addrs_by_fp = completed_table.obj_addresses_by_key_attr('rel_filepath')
            self.assertTrue(fp.relative_to(TEST_CONST.TEST_WATCHED_DIR_PATH_ENCRYPTED) in addrs_by_fp.keys())
        except Exception as e :
            raise e
        finally :
            if download_thread.is_alive() :
                try :
                    dfdd.control_command_queue.put('q')
                    download_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    if download_thread.is_alive() :
                        errmsg = 'ERROR: download thread in test_encrypted_upload_and_download timed out after '
                        errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
                finally :
                    shutil.rmtree(TEST_CONST.TEST_RECO_DIR_PATH_ENCRYPTED)
            if TEST_CONST.TEST_RECO_DIR_PATH_ENCRYPTED.is_dir() :
                shutil.rmtree(TEST_CONST.TEST_RECO_DIR_PATH_ENCRYPTED)
            if upload_thread.is_alive() :
                try :
                    dfud.shutdown()
                    upload_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    if upload_thread.is_alive() :
                        errmsg = 'ERROR: upload thread in test_encrypted_upload_and_download timed out after '
                        errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
                finally :
                    shutil.rmtree(TEST_CONST.TEST_WATCHED_DIR_PATH_ENCRYPTED)
            if TEST_CONST.TEST_WATCHED_DIR_PATH_ENCRYPTED.is_dir() :
                shutil.rmtree(TEST_CONST.TEST_WATCHED_DIR_PATH_ENCRYPTED)
