#imports
import os, pathlib
from openmsistream.running.config import RUN_CONST
from openmsistream.data_file_io.config import RUN_OPT_CONST

class TestRoutineConstants :
    """
    constants used in running tests
    """
    @property
    def TEST_TOPIC_NAMES(self) :
        return {
            'test_data_file_directories':'test_data_file_directories',
            'test_data_file_directories_encrypted':'test_oms_encrypted',
            'test_data_file_stream_processor_kafka':'test_data_file_stream_processor',
            'test_data_file_stream_processor_restart_kafka':'test_data_file_stream_processor_2',
            'test_data_file_stream_processor_restart_encrypted_kafka':'test_data_file_stream_processor_encrypted',
            'test_s3_transfer_stream_processor':'test_s3_transfer_stream_processor',
            'test_serialization':'test_oms_encrypted',
            'test_metadata_reproducer':'test_metadata_extractor',
        }
    @property
    def TEST_ENDPOINT_URL(self) :  # the endpoint_url for s3 bucket connection
        return os.environ['ENDPOINT_URL']
    @property
    def TEST_BUCKET_NAME(self) :  # the bucket name to upload data to (S3Client)
        return os.environ['BUCKET_NAME']
    @property
    def TEST_ASSCESS_KEY_ID(self) :  # the access_key_id for s3 Authentication
        return os.environ['ACCESS_SECRET_KEY_ID']
    @property
    def TEST_SECRET_KEY_ID(self) :  # the secret_key_id for s3 Authentication
        return os.environ['SECRET_KEY_ID']
    @property
    def TEST_REGION(self) :  # the region for the testing s3 bucket
        return os.environ['REGION']
    @property
    def TEST_CONFIG_FILE_PATH(self) : # The path to the Kafka config file to use
        return (RUN_CONST.CONFIG_FILE_DIR / f'{RUN_OPT_CONST.DEFAULT_CONFIG_FILE}{RUN_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def TEST_CONFIG_FILE_PATH_ENCRYPTED(self) : # Same as above except it includes a node_id to test encryption
        return (RUN_CONST.CONFIG_FILE_DIR / f'test_encrypted{RUN_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def TEST_CONFIG_FILE_PATH_ENCRYPTED_2(self) : # Same as above except it includes a node_id to test encryption
        return (RUN_CONST.CONFIG_FILE_DIR / f'test_encrypted_2{RUN_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def TEST_CONFIG_FILE_PATH_S3_TRANSFER(self) : # Same as above except it includes S3 transfer configs
        return (RUN_CONST.CONFIG_FILE_DIR / f'test_s3_transfer{RUN_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def PROD_CONFIG_FILE_PATH(self) : # The path to the "prod" Kafka config file to use in making sure that the prod environment variables are not set
        return (RUN_CONST.CONFIG_FILE_DIR / f'prod{RUN_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def TEST_DIR_PATH(self) :
        return pathlib.Path(__file__).parent.parent
    @property
    def PACKAGE_ROOT_DIR(self) :
        return self.TEST_DIR_PATH.parent / 'openmsistream'
    @property
    def TEST_DATA_DIR_PATH(self) : #path to the test data directory
        return self.TEST_DIR_PATH / 'data'
    @property
    def FAKE_PROD_CONFIG_FILE_PATH(self) : # The path to the "prod" Kafka config file to use in making sure that the prod environment variables are not set
        return (self.TEST_DATA_DIR_PATH / f'fake_prod{RUN_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def TEST_DATA_FILE_ROOT_DIR_NAME(self) : #path to the test data directory
        return 'test_file_root_dir'
    @property
    def TEST_DATA_FILE_SUB_DIR_NAME(self) : #path to the test data directory
        return 'test_file_sub_dir'
    @property 
    def TEST_DATA_FILE_NAME(self) : #name of the test data file
        return '1a0ceb89-b5f0-45dc-9c12-63d3020e2217.dat'
    @property 
    def TEST_DATA_FILE_2_NAME(self) : #name of a second test data file
        return '4ceee043-0b99-4f49-8527-595d93ddc487.dat'
    @property
    def TEST_DATA_FILE_ROOT_DIR_PATH(self) : # Path to the test data file
        return self.TEST_DATA_DIR_PATH / self.TEST_DATA_FILE_ROOT_DIR_NAME
    @property
    def TEST_DATA_FILE_PATH(self) : # Path to the test data file
        return self.TEST_DATA_DIR_PATH / self.TEST_DATA_FILE_ROOT_DIR_NAME / self.TEST_DATA_FILE_SUB_DIR_NAME/ self.TEST_DATA_FILE_NAME
    @property
    def TEST_DATA_FILE_2_PATH(self) : # Path to the second test data file
        return self.TEST_DATA_DIR_PATH / self.TEST_DATA_FILE_2_NAME
    @property
    def TEST_METADATA_REPRODUCER_CONSUMER_CONFIG_FILE_PATH(self) : 
        """
        Path to the config file to use for the final consumer in the metadata reproducer test
        """
        return (RUN_CONST.CONFIG_FILE_DIR / 'test_metadata_reproducer_consumer.config').resolve()
    @property
    def TEST_WATCHED_DIR_PATH(self) : #path to the "watched" directory to use in testing DataFileDirectory etc.
        return self.TEST_DIR_PATH / 'test_watched_dir'
    @property
    def TEST_WATCHED_DIR_PATH_ENCRYPTED(self) : #same as above except for the encrypted tests
        return self.TEST_DIR_PATH / 'test_watched_dir_encrypted'
    @property
    def TEST_STREAM_PROC_WATCHED_DIR_PATH_ENCRYPTED(self) :
        return self.TEST_DIR_PATH / 'test_watched_dir_stream_processor_encrypted'
    @property
    def TEST_WATCHED_DIR_PATH_S3_TRANSFER(self) : #same as above except for the tests that interact with the object store
        return self.TEST_DIR_PATH / 'test_watched_dir_s3_transfer'
    @property
    def TEST_DIR_SERVICES_TEST(self) : # scrap directory to use for services tests
        return self.TEST_DIR_PATH / 'test_dir_services'
    @property
    def TEST_DIR_CUSTOM_RUNNABLE_SERVICE_TEST(self) : # scrap directory to use for the custom Runnable service tests
        return self.TEST_DIR_PATH / 'test_dir_custom_runnable_service'
    @property
    def TEST_DIR_CUSTOM_SCRIPT_SERVICE_TEST(self) : # scrap directory to use for the custom script service tests
        return self.TEST_DIR_PATH / 'test_dir_custom_script_service'
    @property
    def TEST_RECO_DIR_PATH(self) : # Path to the directory where consumed files should be reconstructed
        return self.TEST_DIR_PATH / 'test_reco'
    @property
    def TEST_RECO_DIR_PATH_ENCRYPTED(self) : # same as above except for encrypted tests
        return self.TEST_DIR_PATH / 'test_reco_encrypted'
    @property
    def TEST_STREAM_PROCESSOR_OUTPUT_DIR(self) :
        return self.TEST_DIR_PATH / 'test_stream_processor_output_dir'
    @property
    def TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART(self) :
        return self.TEST_DIR_PATH / 'test_stream_processor_output_dir_restart'
    @property
    def TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART_ENCRYPTED(self) :
        return self.TEST_DIR_PATH / 'test_stream_processor_output_dir_restart_encrypted'
    @property
    def TEST_S3_TRANSFER_STREAM_PROCESSOR_OUTPUT_DIR(self) :
        return self.TEST_DIR_PATH / 'test_s3_transfer_stream_processor_output_dir'
    @property
    def TEST_METADATA_REPRODUCER_OUTPUT_DIR(self) :
        return self.TEST_DIR_PATH / 'test_metadata_reproducer_output_dir'
    @property
    def TEST_METADATA_DICT_PICKLE_FILE(self) :
        return self.TEST_DATA_DIR_PATH / 'test_metadata_dict.pickle'
    
TEST_CONST=TestRoutineConstants()