# imports
from openmsistream.utilities.config import RUN_CONST
from openmsistream.kafka_wrapper.openmsistream_producer import OpenMSIStreamProducer
from openmsistream.kafka_wrapper.openmsistream_consumer import OpenMSIStreamConsumer
from openmsistream.kafka_wrapper.consumer_and_producer_group import (
    ConsumerAndProducerGroup,
)

try:
    from .config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from .base_classes import (
        TestWithKafkaTopics,
        TestWithLogger,
        TestWithEnvVars,
    )
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from base_classes import (
        TestWithKafkaTopics,
        TestWithLogger,
        TestWithEnvVars,
    )


class TestCreateOpenMSIStreamKafkaObjects(
    TestWithKafkaTopics, TestWithLogger, TestWithEnvVars
):
    """
    Class for testing that objects in openmsistream.kafka_wrapper can
    be instantiated using default configs
    """

    TOPICS = {RUN_CONST.DEFAULT_TOPIC_NAME: {}}

    def test_create_openmsistream_producer(self):
        """
        Create a producer from a config file
        """
        producer = OpenMSIStreamProducer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH, logger=self.logger
        )
        self.assertTrue(producer is not None)
        producer.close()

    def test_create_openmsistream_producer_encrypted(self):
        """
        Create an encrypted producer from a config file
        """
        producer = OpenMSIStreamProducer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH_ENC, logger=self.logger
        )
        self.assertTrue(producer is not None)
        producer.close()

    def test_create_openmsistream_consumer(self):
        """
        Create a consumer from a config file
        """
        consumer = OpenMSIStreamConsumer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH, logger=self.logger
        )
        self.assertTrue(consumer is not None)
        consumer.close()

    def test_create_openmsistream_consumer_encrypted(self):
        """
        Create a encrypted consumer from a config file
        """
        consumer = OpenMSIStreamConsumer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH_ENC_2, logger=self.logger
        )
        self.assertTrue(consumer is not None)
        consumer.close()

    def test_create_producer_group(self):
        """
        Create a producer group
        """
        prod_group = ConsumerAndProducerGroup(
            TEST_CONST.TEST_CFG_FILE_PATH, logger=self.logger
        )
        self.assertTrue(prod_group is not None)
        prod_group.close()

    def test_create_producer_group_encrypted(self):
        """
        Create an encrypted producer group
        """
        prod_group = ConsumerAndProducerGroup(
            TEST_CONST.TEST_CFG_FILE_PATH_ENC, logger=self.logger
        )
        self.assertTrue(prod_group is not None)
        prod_group.close()

    def test_create_consumer_group_kafka(self):
        """
        Create a consumer group
        """
        con_group = ConsumerAndProducerGroup(
            TEST_CONST.TEST_CFG_FILE_PATH,
            consumer_topic_name=RUN_CONST.DEFAULT_TOPIC_NAME,
            consumer_group_id=f"test_create_consumer_group_{TEST_CONST.PY_VERSION}",
            logger=self.logger,
        )
        self.assertTrue(con_group is not None)
        con_group.close()

    def test_create_consumer_group_encrypted_kafka(self):
        """
        Create an encrypted consumer group
        """
        con_group = ConsumerAndProducerGroup(
            TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
            consumer_topic_name=RUN_CONST.DEFAULT_TOPIC_NAME,
            consumer_group_id=f"test_create_consumer_group_encrypted_{TEST_CONST.PY_VERSION}",
            logger=self.logger,
        )
        self.assertTrue(con_group is not None)
        con_group.close()
