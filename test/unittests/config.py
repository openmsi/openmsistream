#imports
import os, pathlib
from openmsistream.shared.config import UTIL_CONST
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
            'test_data_file_stream_processor':'test_data_file_stream_processor',
            'test_osn':'osn_test',
            'test_serialization':'test_oms_encrypted',
        }
    @property
    def TEST_ENDPOINT_URL(self) :  # the endpoint_url for osn connection
        return os.environ['ENDPOINT_URL']
    @property
    def TEST_BUCKET_NAME(self) :  # the bucket name to upload data to OSN (S3Client)
        return os.environ['BUCKET_NAME']
    @property
    def TEST_ASSCESS_KEY_ID(self) :  # the access_key_id for OSN Authentication
        return os.environ['ACCESS_SECRET_KEY_ID']
    @property
    def TEST_SECRET_KEY_ID(self) :  # the secret_key_id for OSN Authentication
        return os.environ['SECRET_KEY_ID']
    @property
    def TEST_REGION(self) :  # the region for osn
        return os.environ['REGION']
    @property
    def TEST_CONFIG_FILE_PATH(self) : # The path to the Kafka config file to use
        return (UTIL_CONST.CONFIG_FILE_DIR / f'{RUN_OPT_CONST.DEFAULT_CONFIG_FILE}{UTIL_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def TEST_CONFIG_FILE_PATH_ENCRYPTED(self) : # Same as above except it includes a node_id to test encryption
        return (UTIL_CONST.CONFIG_FILE_DIR / f'test_encrypted{UTIL_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def TEST_CONFIG_FILE_PATH_ENCRYPTED_2(self) : # Same as above except it includes a node_id to test encryption
        return (UTIL_CONST.CONFIG_FILE_DIR / f'test_encrypted_2{UTIL_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def TEST_CONFIG_FILE_PATH_OSN(self) : # Same as above except it includes OSN configs
        return (UTIL_CONST.CONFIG_FILE_DIR / f'test_osn{UTIL_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def PROD_CONFIG_FILE_PATH(self) : # The path to the "prod" Kafka config file to use in making sure that the prod environment variables are not set
        return (UTIL_CONST.CONFIG_FILE_DIR / f'prod{UTIL_CONST.CONFIG_FILE_EXT}').resolve()
    @property
    def TEST_DIR_PATH(self) :
        return pathlib.Path(__file__).parent.parent
    @property
    def TEST_DATA_DIR_PATH(self) : #path to the test data directory
        return self.TEST_DIR_PATH / 'data'
    @property
    def FAKE_PROD_CONFIG_FILE_PATH(self) : # The path to the "prod" Kafka config file to use in making sure that the prod environment variables are not set
        return (self.TEST_DATA_DIR_PATH / f'fake_prod{UTIL_CONST.CONFIG_FILE_EXT}').resolve()
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
    def TEST_WATCHED_DIR_PATH(self) : #path to the "watched" directory to use in testing DataFileDirectory etc.
        return self.TEST_DIR_PATH / 'test_watched_dir'
    @property
    def TEST_WATCHED_DIR_PATH_ENCRYPTED(self) : #same as above except for the encrypted tests
        return self.TEST_DIR_PATH / 'test_watched_dir_encrypted'
    @property
    def TEST_WATCHED_DIR_PATH_OSN(self) : #same as above except for the tests that interact with the object store
        return self.TEST_DIR_PATH / 'test_watched_dir_osn'
    @property
    def TEST_DIR_SERVICES_TEST(self) : # scrap directory to use for services tests
        return self.TEST_DIR_PATH / 'test_dir_services'
    @property
    def TEST_RECO_DIR_PATH(self) : # Path to the directory where consumed files should be reconstructed
        return self.TEST_DIR_PATH / 'test_reco'
    @property
    def TEST_RECO_DIR_PATH_ENCRYPTED(self) : # same as above except for encrypted tests
        return self.TEST_DIR_PATH / 'test_reco_encrypted'
    
TEST_CONST=TestRoutineConstants()