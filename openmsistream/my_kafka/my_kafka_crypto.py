#imports
import pathlib, uuid
from kafkacrypto import KafkaProducer, KafkaConsumer, KafkaCrypto
from ..utilities.misc import cd

class MyKafkaCrypto :
    """
    A class to own and work with the Producers, Consumers, and other objects needed
    by KafkaCrypto to handle encrypting/decrypting messages and exchanging keys
    """

    @property
    def key_serializer(self) :
        return self._kc.getKeySerializer()
    @property
    def key_deserializer(self) :
        return self._kc.getKeyDeserializer()
    @property
    def value_serializer(self) :
        return self._kc.getValueSerializer()
    @property
    def value_deserializer(self) :
        return self._kc.getValueDeserializer()

    def __init__(self,server_configs,config_file) :
        """
        server_configs = the producer/consumer-agnostic configurations for connecting to the server in question
        config_file    = the path to the config file that should be used to instantiate KafkaCrypto 
        """
        producer_configs = server_configs.copy()
        consumer_configs = server_configs.copy()
        #figure out a consumer group ID to use
        if 'group.id' not in consumer_configs.keys() :
            consumer_configs['group.id'] = str(uuid.uuid1())
        with cd(pathlib.Path(config_file).parent) :
            #start up the producer and consumer
            self._kcp = KafkaProducer(**producer_configs)
            self._kcc = KafkaConsumer(**consumer_configs)
            #initialize the KafkaCrypto object 
            self._kc = KafkaCrypto(None,self._kcp,self._kcc,config_file)

    def close(self) :
        try :
            self._kc.close()
        except :
            pass
        finally :
            self._kc = None
            self._kcp = None
            self._kcc = None
