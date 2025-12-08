# imports
import unittest
from confluent_kafka.serialization import (
    DoubleSerializer,
    IntegerSerializer,
    StringSerializer,
)
from confluent_kafka.serialization import (
    DoubleDeserializer,
    IntegerDeserializer,
    StringDeserializer,
)
from openmsistream.kafka_wrapper.serialization import (
    DataFileChunkSerializer,
    DataFileChunkDeserializer,
)
from openmsistream.kafka_wrapper.config_file_parser import KafkaConfigFileParser


class TestKafkaConfigFileParser(unittest.TestCase):
    """
    Test the custom config file parser
    """

    def test_get_replaced_configs(self):
        """
        Make sure strings are converted correctly to classes
        """
        test_config_dict = {
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
        test_config_dict_1 = test_config_dict.copy()
        test_config_dict_1 = KafkaConfigFileParser.get_replaced_configs(
            test_config_dict_1, "serialization"
        )
        for k, v in test_config_dict_1.items():
            if k in ["par_1", "par_2", "par_3", "par_4"]:
                self.assertTrue(isinstance(v, ref_classes_dict[k]))
            else:
                self.assertEqual(v, test_config_dict[k])
        test_config_dict_2 = test_config_dict.copy()
        test_config_dict_2 = KafkaConfigFileParser.get_replaced_configs(
            test_config_dict_2, "deserialization"
        )
        for k, v in test_config_dict_2.items():
            if k in ["par_5", "par_6", "par_7", "par_8"]:
                self.assertTrue(isinstance(v, ref_classes_dict[k]))
            else:
                self.assertEqual(v, test_config_dict[k])
        with self.assertRaises(ValueError):
            _ = KafkaConfigFileParser.get_replaced_configs(None, None)
        with self.assertRaises(ValueError):
            _ = KafkaConfigFileParser.get_replaced_configs(
                test_config_dict, "never make this a recognized parameter group name"
            )
