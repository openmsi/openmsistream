"""Wrapper around the KafkaCrypto producer/consumer pair communicating with the key passing topics"""

#imports
import pathlib, uuid, warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from kafkacrypto import KafkaProducer, KafkaConsumer, KafkaCrypto
from ..utilities.misc import change_dir

class OpenMSIStreamKafkaCrypto :
    """
    A wrapper class to own and work with the Producers, Consumers, and other objects needed
    by KafkaCrypto to handle encrypting/decrypting messages and exchanging keys

    :param broker_configs: the producer/consumer-agnostic configurations for connecting to the broker
    :type broker_configs: dict
    :param config_file: the path to the KafkaCrypto config file for the node being used
    :type config_file: :class:`pathlib.Path`
    """

    @property
    def config_file_path(self) :
        """
        The path to the KafkaCrypto config file used
        """
        return self.__config_file
    @property
    def key_serializer(self) :
        """
        The crypto key serializer
        """
        return self._kc.getKeySerializer()
    @property
    def key_deserializer(self) :
        """
        The crypto key deserializer
        """
        return self._kc.getKeyDeserializer()
    @property
    def value_serializer(self) :
        """
        The crypto value serializer
        """
        return self._kc.getValueSerializer()
    @property
    def value_deserializer(self) :
        """
        The crypto value deserializer
        """
        return self._kc.getValueDeserializer()

    def __init__(self,broker_configs,config_file) :
        """
        Constructor method
        """
        producer_configs = broker_configs.copy()
        consumer_configs = broker_configs.copy()
        #figure out a consumer group ID to use (KafkaCrypto Consumers need one)
        if 'group.id' not in consumer_configs.keys() :
            consumer_configs['group.id'] = str(uuid.uuid1())
        with change_dir(pathlib.Path(config_file).parent) :
            #start up the producer and consumer
            self._kcp = KafkaProducer(**producer_configs)
            self._kcc = KafkaConsumer(**consumer_configs)
            #initialize the KafkaCrypto object
            self._kc = KafkaCrypto(None,self._kcp,self._kcc,config_file)
        self.__config_file = config_file

    def close(self) :
        """
        Close the :class:`kafkacrypto.KafkaCrypto` object and un-set its producer/consumer
        """
        try :
            self._kc.close()
        except Exception :
            pass
        finally :
            self._kc = None
            self._kcp = None
            self._kcc = None
