#imports
import pathlib, logging
from openmsistream.utilities.logging import Logger
from openmsistream.data_file_io.config import RUN_OPT_CONST
from openmsistream.kafka_wrapper.openmsistream_producer import OpenMSIStreamProducer
from openmsistream.kafka_wrapper.openmsistream_consumer import OpenMSIStreamConsumer
from openmsistream.kafka_wrapper.producer_group import ProducerGroup
from openmsistream.kafka_wrapper.consumer_group import ConsumerGroup
from config import TEST_CONST
from placeholder_env_vars import TestWithEnvVars

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.ERROR)

class TestCreateOpenMSIStreamKafkaObjects(TestWithEnvVars) :
    """
    Class for testing that objects in openmsistream.kafka_wrapper can 
    be instantiated using default configs
    """

    def test_create_openmsistream_producer(self) :
        producer = OpenMSIStreamProducer.from_file(TEST_CONST.TEST_CFG_FILE_PATH,logger=LOGGER)
        self.assertTrue(producer is not None)
        producer.close()

    def test_create_openmsistream_producer_encrypted(self) :
        producer = OpenMSIStreamProducer.from_file(TEST_CONST.TEST_CFG_FILE_PATH_ENC,logger=LOGGER)
        self.assertTrue(producer is not None)
        producer.close()

    def test_create_openmsistream_consumer(self) :
        consumer = OpenMSIStreamConsumer.from_file(TEST_CONST.TEST_CFG_FILE_PATH,logger=LOGGER)
        self.assertTrue(consumer is not None)
        consumer.close()
    
    def test_create_openmsistream_consumer_encrypted(self) :
        consumer = OpenMSIStreamConsumer.from_file(TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,logger=LOGGER)
        self.assertTrue(consumer is not None)
        consumer.close()

    def test_create_producer_group(self) :
        pg = ProducerGroup(TEST_CONST.TEST_CFG_FILE_PATH,logger=LOGGER)
        self.assertTrue(pg is not None)
        pg.close()
    
    def test_create_producer_group_encrypted(self) :
        pg = ProducerGroup(TEST_CONST.TEST_CFG_FILE_PATH_ENC,logger=LOGGER)
        self.assertTrue(pg is not None)
        pg.close()
    
    def test_create_consumer_group_kafka(self) :
        cg = ConsumerGroup(TEST_CONST.TEST_CFG_FILE_PATH,RUN_OPT_CONST.DEFAULT_TOPIC_NAME,
                           consumer_group_id='test_create_consumer_group',
                           logger=LOGGER)
        self.assertTrue(cg is not None)
        cg.close()
    
    def test_create_consumer_group_encrypted_kafka(self) :
        cg = ConsumerGroup(TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,RUN_OPT_CONST.DEFAULT_TOPIC_NAME,
                           consumer_group_id='test_create_consumer_group_encrypted',
                           logger=LOGGER)
        self.assertTrue(cg is not None)
        cg.close()
