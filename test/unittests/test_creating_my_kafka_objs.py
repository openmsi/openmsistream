#imports
import unittest, pathlib, logging
from openmsistream.shared.logging import Logger
from openmsistream.data_file_io.config import RUN_OPT_CONST
from openmsistream.my_kafka.my_producer import MyProducer
from openmsistream.my_kafka.my_consumer import MyConsumer
from openmsistream.my_kafka.consumer_group import ConsumerGroup
from config import TEST_CONST

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.ERROR)

class TestCreateMyKafkaObjects(unittest.TestCase) :
    """
    Class for testing that objects in openmsistream.my_kafka can 
    be instantiated using default configs
    """

    def test_create_my_producer(self) :
        myproducer = MyProducer.from_file(TEST_CONST.TEST_CONFIG_FILE_PATH,logger=LOGGER)
        self.assertTrue(myproducer is not None)
        myproducer.close()

    def test_create_my_producer_encrypted(self) :
        myproducer = MyProducer.from_file(TEST_CONST.TEST_CONFIG_FILE_PATH_ENCRYPTED,logger=LOGGER)
        self.assertTrue(myproducer is not None)
        myproducer.close()

    def test_create_my_consumer(self) :
        myconsumer = MyConsumer.from_file(TEST_CONST.TEST_CONFIG_FILE_PATH,logger=LOGGER)
        self.assertTrue(myconsumer is not None)
        myconsumer.close()
    
    def test_create_my_consumer_encrypted(self) :
        myconsumer = MyConsumer.from_file(TEST_CONST.TEST_CONFIG_FILE_PATH_ENCRYPTED_2,logger=LOGGER)
        self.assertTrue(myconsumer is not None)
        myconsumer.close()

    def test_create_consumer_group(self) :
        cg = ConsumerGroup(TEST_CONST.TEST_CONFIG_FILE_PATH,RUN_OPT_CONST.DEFAULT_TOPIC_NAME,
                           consumer_group_ID='test_create_consumer_group',
                           n_consumers=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS)
        self.assertTrue(cg is not None)
        cg.close()
    
    def test_create_consumer_group_encrypted(self) :
        cg = ConsumerGroup(TEST_CONST.TEST_CONFIG_FILE_PATH_ENCRYPTED_2,RUN_OPT_CONST.DEFAULT_TOPIC_NAME,
                           consumer_group_ID='test_create_consumer_group_encrypted',
                           n_consumers=RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS)
        self.assertTrue(cg is not None)
        cg.close()
