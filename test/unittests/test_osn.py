# imports
import os
import unittest, pathlib, time, logging, shutil, hashlib
from openmsistream.shared.my_thread import MyThread
from openmsistream.osn.osn_stream_processor import OSNStreamProcessor
from openmsistream.osn.s3_data_transfer import S3DataTransfer
from openmsistream.shared.logging import Logger
from openmsistream.data_file_io.config import RUN_OPT_CONST
from openmsistream.data_file_io.data_file_upload_directory import DataFileUploadDirectory
from config import TEST_CONST

# constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0], logging.ERROR)
UPDATE_SECS = 5
TIMEOUT_SECS = 90
JOIN_TIMEOUT_SECS = 60
TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[:-len('.py')]]

class TestOSN(unittest.TestCase):
    """
    Class for testing DataFileUploadDirectory and DataFileDownloadDirectory functions
    """

    # called by the test method below
    def run_data_file_upload_directory(self):
        # make the directory to watch
        dfud = DataFileUploadDirectory(TEST_CONST.TEST_WATCHED_DIR_PATH_OSN,TEST_CONST.TEST_CONFIG_FILE_PATH_OSN,
                                       update_secs=UPDATE_SECS,logger=LOGGER)
        # print(TEST_CONST.TEST_WATCHED_DIR_PATH_OSN)
        # osn_path = TEST_CONST.TEST_WATCHED_DIR_PATH_OSN.replace('\\', '/')
        # print(osn_path)
        # start upload_files_as_added in a separate thread so we can time it out
        upload_thread = MyThread(target=dfud.upload_files_as_added,
                                 args=(TOPIC_NAME,),
                                 kwargs={'n_threads': RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                                         'chunk_size': RUN_OPT_CONST.DEFAULT_CHUNK_SIZE,
                                         'max_queue_size': RUN_OPT_CONST.DEFAULT_MAX_UPLOAD_QUEUE_SIZE,
                                         'upload_existing': True})
        upload_thread.start()
        try:
            # wait a second, copy the test file into the watched directory, and wait another second
            time.sleep(1)
            fp = TEST_CONST.TEST_WATCHED_DIR_PATH_OSN / TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME 
            fp = fp / TEST_CONST.TEST_DATA_FILE_NAME
            if not fp.parent.is_dir():
                fp.parent.mkdir(parents=True)
            fp.write_bytes(TEST_CONST.TEST_DATA_FILE_PATH.read_bytes())
            time.sleep(1)
            # put the "check" command into the input queue a couple times to test it
            dfud.control_command_queue.put('c')
            dfud.control_command_queue.put('check')
            # put the quit command in the command queue to stop the process running
            LOGGER.set_stream_level(logging.INFO)
            msg = '\nQuitting upload thread in run_data_file_upload_directory; '
            msg += f'will timeout after {TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            dfud.control_command_queue.put('q')
            # wait for the uploading thread to complete
            upload_thread.join(timeout=TIMEOUT_SECS)
            if upload_thread.is_alive():
                errmsg = 'ERROR: upload thread in run_data_file_upload_directory '
                errmsg += f'timed out after {TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
        except Exception as e:
            raise e
        finally:
            if upload_thread.is_alive():
                try:
                    dfud.shutdown()
                    upload_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    if upload_thread.is_alive():
                        errmsg = 'ERROR: upload thread in run_data_file_upload_directory timed out after '
                        errmsg += f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e:
                    raise e

    # called by the test method below
    def run_osn_tranfer_data(self):
        osp = OSNStreamProcessor(
            TEST_CONST.TEST_BUCKET_NAME,
            TEST_CONST.TEST_CONFIG_FILE_PATH_OSN,
            TOPIC_NAME,
            n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
            update_secs=UPDATE_SECS,
            consumer_group_ID='test_osn',
            logger=LOGGER,
        )
        osp_thread = MyThread(target=osp.make_stream)
        osp_thread.start()
        try:
            # wait a second, copy the test file into the watched directory, and wait another second
            time.sleep(40)
            # put the "check" command into the input queue a couple of times to test it
            osp.control_command_queue.put('c')
            osp.control_command_queue.put('check')
            # put the quit command in the command queue to stop the process running
            LOGGER.set_stream_level(logging.INFO)
            msg = '\nQuitting OSNStreamProcessor; '
            msg += f'will timeout after {TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            osp.control_command_queue.put('q')
            # wait for the uploading thread to complete
            osp_thread.join(timeout=TIMEOUT_SECS)
            if osp_thread.is_alive():
                errmsg = 'ERROR: transfer osn thread in OSNStreamProcessor '
                errmsg += f'timed out after {TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
        except Exception as e:
            raise e
        finally:
            osp.close_session_client()
            if osp_thread.is_alive():
                try:
                    osp.shutdown()
                    osp_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    if osp_thread.is_alive():
                        errmsg = 'ERROR: transfer osn thread in OSNStreamProcessor timed out after '
                        errmsg += f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e:
                    raise e
                finally:
                    LOGGER.info('wait until validate with producer...')
                    self.validate_osn_with_producer()

    def validate_osn_producer_data_transfer(self):
        osp = OSNStreamProcessor(
            TEST_CONST.TEST_BUCKET_NAME,
            TEST_CONST.TEST_CONFIG_FILE_PATH_OSN,
            TOPIC_NAME,
            n_threads=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,
            update_secs=UPDATE_SECS,
            consumer_group_ID='test_osn',
            logger=LOGGER,
        )
        validate_thread = MyThread(target=self.validate_osn_with_producer)
        validate_thread.start()
        try:
            # wait a second, copy the test file into the watched directory, and wait another second
            time.sleep(10)
            # put the "check" command into the input queue a couple of times to test it
            osp.control_command_queue.put('c')
            osp.control_command_queue.put('check')
            # put the quit command in the command queue to stop the process running
            LOGGER.set_stream_level(logging.INFO)
            msg = '\nQuitting validate_osn_producer_data_transfer; '
            msg += f'will timeout after {TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            osp.control_command_queue.put('q')
            # wait for the uploading thread to complete
            validate_thread.join(timeout=TIMEOUT_SECS)
            if validate_thread.is_alive():
                errmsg = 'ERROR: transfer osn thread in validate_osn_producer_data_transfer '
                errmsg += f'timed out after {TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
        except Exception as e:
            raise e
        finally:
            if validate_thread.is_alive():
                try:
                    osp.shutdown()
                    validate_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    if validate_thread.is_alive():
                        errmsg = 'ERROR: transfer osn thread in validate_osn_producer_data_transfer timed out after '
                        errmsg += f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e:
                    raise e

    def hash_file(self, my_file):
        md5 = hashlib.md5()
        BUF_SIZE = 65536
        try:
            with open(my_file, 'rb') as f:
                while True:
                    data = f.read(BUF_SIZE)
                    if not data:
                        break
                    md5.update(data)
        except IOError:
            LOGGER.info('Error While Opening the file!')
            return None
        return format(md5.hexdigest())

    def validate_osn_with_producer(self):
        LOGGER.info('validating osn with producer')
        endpoint_url = TEST_CONST.TEST_ENDPOINT_URL
        if not endpoint_url.startswith('https://'):
            endpoint_url = 'https://' + endpoint_url
        aws_access_key_id = TEST_CONST.TEST_ASSCESS_KEY_ID
        aws_secret_access_key = TEST_CONST.TEST_SECRET_KEY_ID
        region_name = TEST_CONST.TEST_REGION
        bucket_name = TEST_CONST.TEST_BUCKET_NAME
        osn_config = {'endpoint_url': endpoint_url, 'access_key_id': aws_access_key_id,
                      'secret_key_id': aws_secret_access_key,
                      'region': region_name, 'bucket_name': bucket_name}
        s3d = S3DataTransfer(osn_config,logger=LOGGER)
        for subdir, dirs, files in os.walk(TEST_CONST.TEST_WATCHED_DIR_PATH_OSN):
            for file in files:
                local_path = str(os.path.join(subdir, file))
                if ( local_path.__contains__(f'files_fully_uploaded_to_{TOPIC_NAME}') or
                     local_path.__contains__(f'files_to_upload_to_{TOPIC_NAME}') or 
                     local_path.__contains__('LOGS') ) :
                    continue

                hashed_datafile_stream = self.hash_file(local_path)
                if hashed_datafile_stream == None:
                    raise Exception('datafile_stream producer is null!')
                local_path = str(os.path.join(subdir, file)).replace('\\', '/')
                object_key = TOPIC_NAME + '/' + local_path[len(str(TEST_CONST.TEST_WATCHED_DIR_PATH_OSN)) + 1:]

                if not (s3d.compare_producer_datafile_with_osn_object_stream(TEST_CONST.TEST_BUCKET_NAME, object_key,
                                                                         hashed_datafile_stream)):
                    LOGGER.info('did not match for producer')
                    raise Exception('Failed to match osn object with the original producer data')
                s3d.delete_object_from_osn(bucket_name, object_key)
        shutil.rmtree(TEST_CONST.TEST_WATCHED_DIR_PATH_OSN)
        if TEST_CONST.TEST_WATCHED_DIR_PATH_OSN.is_dir() :
            shutil.rmtree(TEST_CONST.TEST_WATCHED_DIR_PATH_OSN)
        s3d.close_session()
        LOGGER.info('All test cases passed')

    def test_upload_and_transfer_into_osn_kafka(self):
        self.run_data_file_upload_directory()
        self.run_osn_tranfer_data()
        self.validate_osn_producer_data_transfer()
