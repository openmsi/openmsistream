import sys

import pytest

from openmsistream.kafka_wrapper.consumer_and_producer_group import (
    ConsumerAndProducerGroup,
)
from openmsistream.kafka_wrapper.openmsistream_consumer import OpenMSIStreamConsumer
from openmsistream.kafka_wrapper.openmsistream_producer import OpenMSIStreamProducer
from openmsistream.utilities.config import RUN_CONST

from .config import TEST_CONST


@pytest.mark.kafka
@pytest.mark.parametrize(
    "kafka_topics",
    [{RUN_CONST.DEFAULT_TOPIC_NAME: {}}],
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

    def test_create_openmsistream_consumer(self, logger):
        consumer = OpenMSIStreamConsumer.from_file(
            TEST_CONST.TEST_CFG_FILE_PATH, logger=logger
        )
        assert consumer is not None
        consumer.close()

    def test_create_producer_group(self, logger):
        group = ConsumerAndProducerGroup(TEST_CONST.TEST_CFG_FILE_PATH, logger=logger)
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


TOPIC_NAME = "test_objects_encrypted"
TOPICS = {
    TOPIC_NAME: {},
    f"{TOPIC_NAME}.keys": {"--partitions": 1},
    f"{TOPIC_NAME}.reqs": {"--partitions": 1},
    f"{TOPIC_NAME}.subs": {"--partitions": 1},
}


@pytest.mark.kafka
@pytest.mark.parametrize(
    "kafka_topics",
    [TOPICS],
    indirect=True,
)
@pytest.mark.usefixtures("logger", "kafka_topics")
class TestCreateOpenMSIStreamKafkaObjectsEncrypted:
    """
    Tests that objects in openmsistream.kafka_wrapper can be instantiated
    using default Kafka configs with the ephemeral test cluster using KafkaCrypto
    """

    @staticmethod
    def _node_config(node_type):
        return (
            TOPIC_NAME,
            "test-rotation-password",
            "test-password",
            1,
            node_type,
        )

    def test_create_openmsistream_consumer_encrypted(
        self, logger, kafka_config_file, encrypted_kafka_node_config
    ):
        node_id = "consumer_node"
        config_path = kafka_config_file(node_id=node_id)
        encrypted_kafka_node_config(config_path, node_id, *self._node_config("consumer"))
        consumer = OpenMSIStreamConsumer.from_file(config_path, logger=logger)
        assert consumer is not None
        consumer.close()

    def test_create_openmsistream_producer_encrypted(
        self, logger, kafka_config_file, encrypted_kafka_node_config
    ):
        node_id = "consumer_node"
        config_path = kafka_config_file(node_id=node_id)
        encrypted_kafka_node_config(config_path, node_id, *self._node_config("producer"))
        producer = OpenMSIStreamProducer.from_file(config_path, logger=logger)
        assert producer is not None
        producer.close()

    def test_create_producer_group_encrypted(
        self, logger, kafka_config_file, encrypted_kafka_node_config
    ):
        node_id = "prodcon_node"
        config_path = kafka_config_file(node_id=node_id)
        encrypted_kafka_node_config(config_path, node_id, *self._node_config("prodcon"))
        group = ConsumerAndProducerGroup(config_path, logger=logger)
        assert group is not None
        group.close()

    def test_create_consumer_group_encrypted_kafka(
        self, logger, kafka_config_file, encrypted_kafka_node_config
    ):
        node_id = "prodcon_node2"
        config_path = kafka_config_file(node_id=node_id)
        encrypted_kafka_node_config(config_path, node_id, *self._node_config("prodcon"))
        py_ver = f"python_{sys.version.split()[0].replace('.', '_')}"
        group = ConsumerAndProducerGroup(
            config_path,
            consumer_topic_name=TOPIC_NAME,
            consumer_group_id=f"test_create_consumer_group_encrypted_{py_ver}",
            logger=logger,
        )
        assert group is not None
        group.close()
