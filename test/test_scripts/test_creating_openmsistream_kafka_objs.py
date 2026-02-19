import pytest

from openmsistream.utilities.config import RUN_CONST
from openmsistream.kafka_wrapper.openmsistream_producer import OpenMSIStreamProducer
from openmsistream.kafka_wrapper.openmsistream_consumer import OpenMSIStreamConsumer
from openmsistream.kafka_wrapper.consumer_and_producer_group import (
    ConsumerAndProducerGroup,
)

from .config import TEST_CONST


@pytest.mark.parametrize(
    "kafka_topics",
    [{RUN_CONST.DEFAULT_TOPIC_NAME: {}}],  # same pattern as your working test
    indirect=True,
)
@pytest.mark.usefixtures("logger", "kafka_topics", "apply_kafka_env")
class TestCreateOpenMSIStreamKafkaObjects:
    """
    Tests that objects in openmsistream.kafka_wrapper can be instantiated
    using default Kafka configs with the ephemeral test cluster.
    """

    def test_create_openmsistream_producer(self, logger):
        producer = OpenMSIStreamProducer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH, logger=logger
        )
        assert producer is not None
        producer.close()

    def test_create_openmsistream_producer_encrypted(self, logger):
        producer = OpenMSIStreamProducer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH_ENC, logger=logger
        )
        assert producer is not None
        producer.close()

    def test_create_openmsistream_consumer(self, logger):
        consumer = OpenMSIStreamConsumer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH, logger=logger
        )
        assert consumer is not None
        consumer.close()

    def test_create_openmsistream_consumer_encrypted(self, logger):
        consumer = OpenMSIStreamConsumer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH_ENC_2, logger=logger
        )
        assert consumer is not None
        consumer.close()

    def test_create_producer_group(self, logger):
        group = ConsumerAndProducerGroup(TEST_CONST.TEST_CFG_FILE_PATH, logger=logger)
        assert group is not None
        group.close()

    def test_create_producer_group_encrypted(self, logger):
        group = ConsumerAndProducerGroup(TEST_CONST.TEST_CFG_FILE_PATH_ENC, logger=logger)
        assert group is not None
        group.close()

    def test_create_consumer_group_kafka(self, logger):
        group = ConsumerAndProducerGroup(
            TEST_CONST.TEST_CFG_FILE_PATH,
            consumer_topic_name=RUN_CONST.DEFAULT_TOPIC_NAME,
            consumer_group_id=f"test_create_consumer_group_{TEST_CONST.PY_VERSION}",
            logger=logger,
        )
        assert group is not None
        group.close()

    def test_create_consumer_group_encrypted_kafka(self, logger):
        group = ConsumerAndProducerGroup(
            TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
            consumer_topic_name=RUN_CONST.DEFAULT_TOPIC_NAME,
            consumer_group_id=f"test_create_consumer_group_encrypted_{TEST_CONST.PY_VERSION}",
            logger=logger,
        )
        assert group is not None
        group.close()
