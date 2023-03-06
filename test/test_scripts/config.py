#imports
import os, pathlib
from openmsistream.utilities.config import RUN_CONST
from openmsistream.data_file_io.config import RUN_OPT_CONST

class TestRoutineConstants :
    """
    constants used in running tests
    """
    
    #names of topics on the test cluster to use for each test
    TEST_TOPIC_NAMES = {
            'test_data_file_directories':'test_data_file_directories',
            'test_data_file_directories_encrypted':'test_oms_encrypted',
            'test_data_file_stream_processor_kafka':'test_data_file_stream_processor',
            'test_data_file_stream_processor_restart_kafka':'test_data_file_stream_processor_2',
            'test_data_file_stream_processor_restart_encrypted_kafka':'test_data_file_stream_processor_encrypted',
            'test_plots_for_tutorial':'test_plots_for_tutorial',
            'test_s3_transfer_stream_processor':'test_s3_transfer_stream_processor',
            'test_serialization':'test_oms_encrypted',
            'test_metadata_reproducer':'test_metadata_extractor',
        }

    #Paths to locations inside the code base
    TEST_DIR_PATH = (pathlib.Path(__file__).parent.parent).resolve()
    EXAMPLES_DIR_PATH = TEST_DIR_PATH.parent / 'examples'
    TEST_DATA_DIR_PATH = TEST_DIR_PATH / 'data'
    
    #S3 connection information
    # the endpoint_url for s3 bucket connection
    TEST_ENDPOINT_URL = os.environ['ENDPOINT_URL'] if 'ENDPOINT_URL' in os.environ else 'ENDPOINT_URL'
    # the bucket name to upload data to (S3Client)
    TEST_BUCKET_NAME = os.environ['BUCKET_NAME'] if 'BUCKET_NAME' in os.environ else 'BUCKET_NAME'
    # the access_key_id for s3 Authentication
    TEST_ACCESS_KEY_ID = os.environ['ACCESS_KEY_ID'] if 'ACCESS_KEY_ID' in os.environ else 'ACCESS_KEY_ID'
    # the secret_key_id for s3 Authentication
    TEST_SECRET_KEY_ID = os.environ['SECRET_KEY_ID'] if 'SECRET_KEY_ID' in os.environ else 'SECRET_KEY_ID'
    # the region for the testing s3 bucket
    TEST_REGION = os.environ['REGION'] if 'REGION' in os.environ else 'REGION'

    #different config files used in tests
    TEST_CFG_FILE_PATH = RUN_CONST.CONFIG_FILE_DIR/f'{RUN_OPT_CONST.DEFAULT_CONFIG_FILE}{RUN_CONST.CONFIG_FILE_EXT}'
    # Same as above except it includes a node_id to test encryption
    TEST_CFG_FILE_PATH_ENC = TEST_CFG_FILE_PATH.with_name(f'test_encrypted{TEST_CFG_FILE_PATH.suffix}')
    TEST_CFG_FILE_PATH_ENC_2 = TEST_CFG_FILE_PATH.with_name(f'test_encrypted_2{TEST_CFG_FILE_PATH.suffix}')
    # Same as above except it includes S3 transfer configs
    TEST_CFG_FILE_PATH_S3 = TEST_CFG_FILE_PATH.with_name(f'test_s3_transfer{TEST_CFG_FILE_PATH.suffix}')
    #the config file to use for the final consumer in the metadata reproducer test
    TEST_CFG_FILE_PATH_MDC = TEST_CFG_FILE_PATH.with_name(f'test_metadata_rep_consumer{TEST_CFG_FILE_PATH.suffix}')
    # If an environment variable indicates that a local broker is being used, 
    # prepend "local_broker_" to the names of the above config files used in tests
    if os.environ.get('LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS') and os.environ.get('USE_LOCAL_KAFKA_BROKER_IN_TESTS') :
        TEST_CFG_FILE_PATH = TEST_CFG_FILE_PATH.with_name(f'local_broker_{TEST_CFG_FILE_PATH.name}')
        TEST_CFG_FILE_PATH_ENC = TEST_CFG_FILE_PATH_ENC.with_name(f'local_broker_{TEST_CFG_FILE_PATH_ENC.name}')
        TEST_CFG_FILE_PATH_ENC_2 = TEST_CFG_FILE_PATH_ENC_2.with_name(f'local_broker_{TEST_CFG_FILE_PATH_ENC_2.name}')
        TEST_CFG_FILE_PATH_S3 = TEST_CFG_FILE_PATH_S3.with_name(f'local_broker_{TEST_CFG_FILE_PATH_S3.name}')
        TEST_CFG_FILE_PATH_MDC = TEST_CFG_FILE_PATH_MDC.with_name(f'local_broker_{TEST_CFG_FILE_PATH_MDC.name}')
    # The path to the "prod" Kafka config file to use in making sure that the prod environment variables are not set
    PROD_CONFIG_FILE_PATH = TEST_CFG_FILE_PATH.with_name(f'prod{TEST_CFG_FILE_PATH.suffix}')
    FAKE_PROD_CONFIG_FILE_PATH = TEST_DATA_DIR_PATH/f'fake_prod{RUN_CONST.CONFIG_FILE_EXT}'

    #Names of and paths to directories and files used in testing
    TEST_DATA_FILE_ROOT_DIR_NAME = 'test_file_root_dir'
    TEST_DATA_FILE_SUB_DIR_NAME = 'test_file_sub_dir'
    TEST_DATA_FILE_NAME = '1a0ceb89-b5f0-45dc-9c12-63d3020e2217.dat'
    TEST_DATA_FILE_2_NAME = '4ceee043-0b99-4f49-8527-595d93ddc487.dat'
    TEST_DATA_FILE_ROOT_DIR_PATH = TEST_DATA_DIR_PATH / TEST_DATA_FILE_ROOT_DIR_NAME
    TEST_DATA_FILE_PATH = ( TEST_DATA_DIR_PATH / TEST_DATA_FILE_ROOT_DIR_NAME / 
                            TEST_DATA_FILE_SUB_DIR_NAME / TEST_DATA_FILE_NAME )
    TEST_DATA_FILE_2_PATH = TEST_DATA_DIR_PATH / TEST_DATA_FILE_2_NAME
    TEST_WATCHED_DIR_PATH = TEST_DIR_PATH / 'test_watched_dir'
    TEST_WATCHED_DIR_PATH_ENCRYPTED = TEST_DIR_PATH / 'test_watched_dir_encrypted'
    TEST_STREAM_PROC_WATCHED_DIR_PATH_ENCRYPTED = TEST_DIR_PATH / 'test_watched_dir_stream_processor_encrypted'
    TEST_WATCHED_DIR_PATH_S3_TRANSFER = TEST_DIR_PATH / 'test_watched_dir_s3_transfer'
    TEST_DIR_SERVICES_TEST = TEST_DIR_PATH / 'test_dir_services'
    TEST_DIR_CUSTOM_RUNNABLE_SERVICE_TEST = TEST_DIR_PATH / 'test_dir_custom_runnable_service'
    TEST_DIR_CUSTOM_SCRIPT_SERVICE_TEST = TEST_DIR_PATH / 'test_dir_custom_script_service'
    TEST_RECO_DIR_PATH = TEST_DIR_PATH / 'test_reco'
    TEST_RECO_DIR_PATH_ENCRYPTED = TEST_DIR_PATH / 'test_reco_encrypted'
    TEST_STREAM_PROCESSOR_OUTPUT_DIR = TEST_DIR_PATH / 'test_stream_processor'
    TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART = TEST_DIR_PATH / 'test_stream_processor_restart'
    TEST_STREAM_PROCESSOR_OUTPUT_DIR_RESTART_ENCRYPTED = TEST_DIR_PATH / 'test_stream_processor_restart_encrypted'
    TEST_S3_TRANSFER_STREAM_PROCESSOR_OUTPUT_DIR = TEST_DIR_PATH / 'test_s3_transfer_stream_processor_output_dir'
    TEST_METADATA_REPRODUCER_OUTPUT_DIR = TEST_DIR_PATH / 'test_metadata_reproducer_output_dir'
    TEST_METADATA_DICT_PICKLE_FILE = TEST_DATA_DIR_PATH / 'test_metadata_dict.pickle'
    TEST_TUTORIAL_PLOTS_OUTPUT_DIR = TEST_DIR_PATH / 'test_plots_for_tutorial'
    
    # size (in bytes) of chunks to use in tests
    TEST_CHUNK_SIZE = 16384 

    # URL to download the example metadata extraction/tutorial plots file from
    TUTORIAL_TEST_FILE_URL = 'https://data.paradim.org/194/XRD/SC001/SC001%20XRR.csv'
    
TEST_CONST=TestRoutineConstants()