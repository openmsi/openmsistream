#imports
import unittest
from confluent_kafka.serialization import DoubleSerializer, IntegerSerializer, StringSerializer
from confluent_kafka.serialization import DoubleDeserializer, IntegerDeserializer, StringDeserializer
from openmsistream.my_kafka.serialization import DataFileChunkSerializer, DataFileChunkDeserializer
from openmsistream.my_kafka.config_file_parser import MyKafkaConfigFileParser

class TestMyKafkaConfigFileParser(unittest.TestCase) :
    """
    Class for testing functions in openmsistream.my_kafka.utilities
    """

    def test_get_replaced_configs(self) :
        test_config_dict = {'par_1': 'DoubleSerializer',
                            'par_2': 'IntegerSerializer',
                            'par_3': 'StringSerializer',
                            'par_4': 'DataFileChunkSerializer',
                            'par_5': 'DoubleDeserializer',
                            'par_6': 'IntegerDeserializer',
                            'par_7': 'StringDeserializer',
                            'par_8': 'DataFileChunkDeserializer',
                        }
        ref_classes_dict = {'par_1': DoubleSerializer, 
                            'par_2': IntegerSerializer, 
                            'par_3': StringSerializer, 
                            'par_4': DataFileChunkSerializer, 
                            'par_5': DoubleDeserializer, 
                            'par_6': IntegerDeserializer, 
                            'par_7': StringDeserializer, 
                            'par_8': DataFileChunkDeserializer, 
                        }
        test_config_dict_1 = test_config_dict.copy()
        test_config_dict_1 = MyKafkaConfigFileParser.get_replaced_configs(test_config_dict_1,'serialization')
        for k in test_config_dict_1 :
            if k in ['par_1','par_2','par_3','par_4'] :
                self.assertTrue(isinstance(test_config_dict_1[k],ref_classes_dict[k]))
            else :
                self.assertEqual(test_config_dict_1[k],test_config_dict[k])
        test_config_dict_2 = test_config_dict.copy()
        test_config_dict_2 = MyKafkaConfigFileParser.get_replaced_configs(test_config_dict_2,'deserialization')
        for k in test_config_dict_2 :
            if k in ['par_5','par_6','par_7','par_8'] :
                self.assertTrue(isinstance(test_config_dict_2[k],ref_classes_dict[k]))
            else :
                self.assertEqual(test_config_dict_2[k],test_config_dict[k])
        with self.assertRaises(AttributeError) :
            _ = MyKafkaConfigFileParser.get_transformed_configs(None,None)
        with self.assertRaises(AttributeError) :
            _ = MyKafkaConfigFileParser.get_transformed_configs(test_config_dict,'never make this a recognized parameter group name')
