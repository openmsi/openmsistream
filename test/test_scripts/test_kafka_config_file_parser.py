import pytest
from confluent_kafka.serialization import (
    DoubleSerializer,
    IntegerSerializer,
    StringSerializer,
    DoubleDeserializer,
    IntegerDeserializer,
    StringDeserializer,
)

from openmsistream.kafka_wrapper.serialization import (
    DataFileChunkSerializer,
    DataFileChunkDeserializer,
)
from openmsistream.kafka_wrapper.config_file_parser import KafkaConfigFileParser


@pytest.fixture
def test_config_dict():
    return {
        "par_1": "DoubleSerializer",
        "par_2": "IntegerSerializer",
        "par_3": "StringSerializer",
        "par_4": "DataFileChunkSerializer",
        "par_5": "DoubleDeserializer",
        "par_6": "IntegerDeserializer",
        "par_7": "StringDeserializer",
        "par_8": "DataFileChunkDeserializer",
    }


ref_classes_dict = {
    "par_1": DoubleSerializer,
    "par_2": IntegerSerializer,
    "par_3": StringSerializer,
    "par_4": DataFileChunkSerializer,
    "par_5": DoubleDeserializer,
    "par_6": IntegerDeserializer,
    "par_7": StringDeserializer,
    "par_8": DataFileChunkDeserializer,
}


def test_get_replaced_configs_serialization(test_config_dict):
    out = KafkaConfigFileParser.get_replaced_configs(
        test_config_dict.copy(), "serialization"
    )

    for k, v in out.items():
        if k in ["par_1", "par_2", "par_3", "par_4"]:
            assert isinstance(v, ref_classes_dict[k])
        else:
            assert v == test_config_dict[k]


def test_get_replaced_configs_deserialization(test_config_dict):
    out = KafkaConfigFileParser.get_replaced_configs(
        test_config_dict.copy(), "deserialization"
    )

    for k, v in out.items():
        if k in ["par_5", "par_6", "par_7", "par_8"]:
            assert isinstance(v, ref_classes_dict[k])
        else:
            assert v == test_config_dict[k]


def test_invalid_inputs():
    with pytest.raises(ValueError):
        KafkaConfigFileParser.get_replaced_configs(None, None)

    with pytest.raises(ValueError):
        KafkaConfigFileParser.get_replaced_configs(
            {"par_1": "DoubleSerializer"},
            "never make this a recognized parameter group name",
        )
