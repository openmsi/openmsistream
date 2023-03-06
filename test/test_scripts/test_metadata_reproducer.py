#imports
import unittest, pathlib, logging, time, datetime, json, pickle, shutil, urllib.request, os
import importlib.machinery, importlib.util
from openmsistream import UploadDataFile
from openmsistream.utilities import Logger
from openmsistream.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream.kafka_wrapper import ConsumerGroup
from openmsistream.data_file_io.actor.file_registry.stream_handler_registries import StreamReproducerRegistry
from config import TEST_CONST

#import the XRDCSVMetadataReproducer from the examples directory
class_path = TEST_CONST.EXAMPLES_DIR_PATH / 'extracting_metadata' / 'xrd_csv_metadata_reproducer.py'
module_name = class_path.name[:-len('.py')]
loader = importlib.machinery.SourceFileLoader(module_name, str(class_path))
module = loader.load_module()

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.DEBUG)#ERROR)
TIMEOUT_SECS = 90
JOIN_TIMEOUT_SECS = 60
REP_CONFIG_PATH = TEST_CONST.EXAMPLES_DIR_PATH / 'extracting_metadata' / 'test_xrd_csv_metadata_reproducer.config'
if os.environ.get('LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS') and os.environ.get('USE_LOCAL_KAFKA_BROKER_IN_TESTS') :
    REP_CONFIG_PATH = REP_CONFIG_PATH.with_name(f'local_broker_{REP_CONFIG_PATH.name}')
UPLOAD_FILE = TEST_CONST.EXAMPLES_DIR_PATH / 'extracting_metadata' / 'SC001_XRR.csv'
SOURCE_TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[:-len('.py')]]+'_source'
DEST_TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[:-len('.py')]]+'_dest'
CONSUMER_GROUP_ID = 'test_metadata_reproducer'

class TestMetadataReproducer(unittest.TestCase) :
    """
    Class for testing that an uploaded file can be read back from the topic and have its metadata 
    successfully extracted and produced to another topic as a string of JSON
    """

    def setUp(self) :
        """
        Download the test data file from its URL on the PARADIM website
        """
        urllib.request.urlretrieve(TEST_CONST.TUTORIAL_TEST_FILE_URL,
                                   UPLOAD_FILE)

    def tearDown(self) :
        """
        Remove the test data file that was downloaded
        """
        if UPLOAD_FILE.is_file() :
            UPLOAD_FILE.unlink()

    def test_metadata_reproducer_kafka(self) :
        #remove any old output
        if TEST_CONST.TEST_METADATA_REPRODUCER_OUTPUT_DIR.is_dir() :
            shutil.rmtree(TEST_CONST.TEST_METADATA_REPRODUCER_OUTPUT_DIR)
        #make note of the start time
        start_time = datetime.datetime.now()
        #start up the reproducer
        metadata_reproducer = module.XRDCSVMetadataReproducer(
                                REP_CONFIG_PATH,
                                SOURCE_TOPIC_NAME,
                                DEST_TOPIC_NAME,
                                consumer_group_id=CONSUMER_GROUP_ID,
                                output_dir=TEST_CONST.TEST_METADATA_REPRODUCER_OUTPUT_DIR,
                                logger=LOGGER
                            )
        rep_thread = ExceptionTrackingThread(target=metadata_reproducer.produce_processing_results_for_files_as_read)
        rep_thread.start()
        try :
            #upload the test data file
            LOGGER.set_stream_level(logging.INFO)
            msg = 'Uploading test metadata file in test_metadata_reproducer'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            upload_file = UploadDataFile(UPLOAD_FILE,
                                        logger=LOGGER)
            upload_file.upload_whole_file(TEST_CONST.TEST_CFG_FILE_PATH,SOURCE_TOPIC_NAME)
            #wait for the file to be processed
            current_messages_read = -1
            time_waited = 0
            LOGGER.set_stream_level(logging.INFO)
            msg = f'Waiting to extract and reproduce test file metadata from the "{SOURCE_TOPIC_NAME}" topic in '
            msg+= f'test_metadata_reproducer (will timeout after {TIMEOUT_SECS} seconds)...'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            recofp = pathlib.Path(UPLOAD_FILE.name)
            while (recofp not in metadata_reproducer.results_produced_filepaths) and time_waited<TIMEOUT_SECS :
                current_messages_read = metadata_reproducer.n_msgs_read
                LOGGER.set_stream_level(logging.INFO)
                LOGGER.info(f'\t{current_messages_read} messages read after waiting {time_waited} seconds....')
                LOGGER.set_stream_level(logging.ERROR)
                time.sleep(5)
                time_waited+=5
            #put the "quit" command into the input queue, which should stop the thread and clean it up also
            LOGGER.set_stream_level(logging.INFO)
            msg = f'Quitting reproducer thread in test_metadata_reproducer after reading {metadata_reproducer.n_msgs_read} '
            msg+= f'messages; will timeout after {JOIN_TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            metadata_reproducer.control_command_queue.put('q')
            #wait for the reproducer thread to finish
            rep_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if rep_thread.is_alive() :
                errmsg = 'ERROR: download thread in test_metadata_reproducer timed out after '
                errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            #make sure the file is listed in the 'results_produced' file
            time.sleep(1.0)
            metadata_reproducer.file_registry.in_progress_table.dump_to_file()
            metadata_reproducer.file_registry.succeeded_table.dump_to_file()
            srpr = StreamReproducerRegistry(dirpath=TEST_CONST.TEST_METADATA_REPRODUCER_OUTPUT_DIR,
                                            consumer_topic_name=SOURCE_TOPIC_NAME,
                                            consumer_group_id=CONSUMER_GROUP_ID,
                                            producer_topic_name=DEST_TOPIC_NAME,
                                            logger=LOGGER)
            self.assertEqual(len(srpr.filepaths_to_rerun),0)
            in_prog_table = srpr.in_progress_table
            in_prog_entries = in_prog_table.obj_addresses_by_key_attr('status')
            succeeded_table = srpr.succeeded_table
            succeeded_entries = succeeded_table.obj_addresses
            self.assertTrue(len(succeeded_entries)>=1)
            self.assertTrue(srpr.PRODUCING_MESSAGE_FAILED not in in_prog_entries.keys())
            self.assertTrue(srpr.COMPUTING_RESULT_FAILED not in in_prog_entries.keys())
            #get the attributes of the succeeded file to make sure its the one that was produced
            succeeded_entry_attrs = succeeded_table.get_entry_attrs(succeeded_entries[0])
            self.assertTrue(succeeded_entry_attrs['filename']==UPLOAD_FILE.name)
            #consume messages from the destination topic and make sure the metadata from the test file is there
            consumer_group = ConsumerGroup(TEST_CONST.TEST_CFG_FILE_PATH_MDC,
                                        DEST_TOPIC_NAME,
                                        consumer_group_id=CONSUMER_GROUP_ID)
            consumer = consumer_group.get_new_subscribed_consumer()
            LOGGER.set_stream_level(logging.INFO)
            msg =  'Consuming metadata message in test_metadata_reproducer; '
            msg+= f'will timeout after {TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            success = False; consume_start_time = datetime.datetime.now()
            while (not success) and (datetime.datetime.now()-consume_start_time).total_seconds()<TIMEOUT_SECS :
                msg = None
                while msg is None and (datetime.datetime.now()-consume_start_time).total_seconds()<TIMEOUT_SECS :
                    msg = consumer.get_next_message()
                if msg is None :
                    continue
                msg_value = msg.value()
                msg_dict = json.loads(msg_value)
                created_at_time = datetime.datetime.strptime(msg_dict['metadata_message_generated_at'],'%m/%d/%Y, %H:%M:%S')
                if (created_at_time-start_time).total_seconds()>0 :
                    with open(TEST_CONST.TEST_METADATA_DICT_PICKLE_FILE,'rb') as fp :
                        ref_dict =  pickle.load(fp)
                    n_matches = 0
                    for k,v in ref_dict.items() :
                        if k in msg_dict.keys() and msg_dict[k]==v :
                            n_matches+=1
                    if n_matches==len(ref_dict) :
                        success=True
            if msg is None :
                errmsg = f'ERROR: could not consume metadata message from {DEST_TOPIC_NAME} within {TIMEOUT_SECS} seconds.'
                raise RuntimeError(errmsg)
            if not success :
                errmsg = 'ERROR: message read from destination topic does not match the reference metadata dictionary!'
                raise RuntimeError(errmsg)
        except Exception as e :
            raise e
        finally :
            if rep_thread.is_alive() :
                try :
                    metadata_reproducer.control_command_queue.put('q')
                    rep_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    if rep_thread.is_alive() :
                        errmsg = 'ERROR: reproducer thread in test_metadata_reproducer timed out after '
                        errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
                finally :
                    shutil.rmtree(TEST_CONST.TEST_METADATA_REPRODUCER_OUTPUT_DIR)
            if TEST_CONST.TEST_METADATA_REPRODUCER_OUTPUT_DIR.is_dir() :
                shutil.rmtree(TEST_CONST.TEST_METADATA_REPRODUCER_OUTPUT_DIR)
