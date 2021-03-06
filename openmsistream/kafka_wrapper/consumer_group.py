#imports
import kafka
from confluent_kafka.admin import AdminClient
from confluent_kafka import TopicPartition
from ..utilities import LogOwner
from .utilities import reset_to_beginning_on_assign
from .config_file_parser import KafkaConfigFileParser
from .openmsistream_consumer import OpenMSIStreamConsumer

class ConsumerGroup(LogOwner) :
    """
    Class for working with a group of consumers sharing a single :class:`kafkacrypto.KafkaCrypto` instance

    :param config_path: Path to the config file that should be used to define Consumers in the group
    :type config_path: :class:`pathlib.Path`
    :param topic_name: The name of the topic to which the Consumers should be subscribed
    :type topic_name: str
    :param consumer_group_ID: The ID string that should be used for each Consumer in the group. 
        "create_new" will create a new UID to use.
    :type consumer_group_ID: str
    :param kwargs: Other keyword arguments will be added to the underlying Consumer's configurations, 
        with underscores in their names replaced with dots.
    :type kwargs: dict
    """

    @property
    def topic_name(self) :
        return self.__topic_name
    @property
    def consumer_group_ID(self) :
        return self.__consumer_group_ID

    def __init__(self,config_path,topic_name,*,consumer_group_ID='create_new',**kwargs) :
        """
        Constructor method
        """
        super().__init__(**kwargs)
        self.__group_starting_offsets = self.__get_group_starting_offsets(config_path,topic_name,consumer_group_ID)
        self.__topic_name = topic_name
        self.__c_args, self.__c_kwargs = OpenMSIStreamConsumer.get_consumer_args_kwargs(config_path,
                                                                                        group_id=consumer_group_ID,
                                                                                        logger=self.logger)
        if len(self.__c_args)>1 and 'group.id' in self.__c_args[1].keys() :
            self.__consumer_group_ID = self.__c_args[1]['group.id'] 
        else :
            self.__consumer_group_ID = consumer_group_ID

    def get_new_subscribed_consumer(self,*,restart_at_beginning=False,**kwargs) :
        """
        Return a new Consumer, subscribed to the topic and with the shared group ID.
        Call this function from a child thread to get thread-independent Consumers.

        Note: This function just creates and subscribes the Consumer. Polling it, closing 
        it, and everything else must be handled by whatever calls this function.

        :param restart_at_beginning: if True, the new Consumer will start reading partitions from the earliest 
            messages available, regardless of Consumer group ID and auto.offset.reset values. 
            Useful when re-reading messages.
        :type restart_at_beginning: bool, optional
        :param kwargs: other keyword arguments are passed to the :class:`~OpenMSIStreamConsumer` constructor method
            (for example, parameters to set a key regex or how to filter messages)
        :type kwargs: dict

        :return: a Consumer created using the configs set in the constructor/from `kwargs`, subscribed to the topic
        :rtype: :class:`~OpenMSIStreamConsumer`
        """
        consumer = OpenMSIStreamConsumer(*self.__c_args,
                                         **kwargs,
                                         starting_offsets=self.__group_starting_offsets,**self.__c_kwargs)
        if restart_at_beginning :
            consumer.subscribe([self.__topic_name],on_assign=reset_to_beginning_on_assign)
        else :
            consumer.subscribe([self.__topic_name])
        return consumer

    def close(self) :
        """
        Wrapper around :func:`kafkacrypto.KafkaCrypto.close`. 
        """
        try :
            self.__c_kwargs['kafkacrypto'].close()
        except :
            pass
        finally :
            self.__c_kwargs['kafkacrypto'] = None

    def __get_group_starting_offsets(self,config_path,topic_name,consumer_group_ID) :
        """
        Return a list of TopicPartitions listing the starting offsets for each partition 
        in the topic for the given consumer group ID

        Re-raises any errors encountered in getting the necessary metadata, 
        returning None if that happens
        """
        cfp = KafkaConfigFileParser(config_path)
        starting_offsets = []
        try :
            admin_client = AdminClient(cfp.broker_configs)
            cluster_metadata = admin_client.list_topics(topic=topic_name)
            n_partitions = len(cluster_metadata.topics[topic_name].partitions)
            if n_partitions<=0 :
                raise RuntimeError(f'ERROR: number of partitions for topic {topic_name} is {n_partitions}')
            kac_kwargs = {}
            for k,v in cfp.broker_configs.items() :
                if k in ('sasl.username','sasl.password') :
                    key = k.replace('.','_plain_')
                else :
                    key = k.replace('.','_')
                kac_kwargs[key]=v
            kac = kafka.KafkaAdminClient(**kac_kwargs)
            parts = [kafka.TopicPartition(topic_name,pi) for pi in range(n_partitions)]
            tp_offsets=kac.list_consumer_group_offsets(group_id=consumer_group_ID,partitions=parts)
            if len(tp_offsets)!=n_partitions :
                errmsg = f'Found {n_partitions} partitions for topic {topic_name} but got {len(tp_offsets)} '
                errmsg+= 'TopicPartitions listing current consumer group offsets'
                raise RuntimeError(errmsg)
            for tp,om in tp_offsets.items() :
                starting_offsets.append(TopicPartition(tp.topic,tp.partition,om.offset))
            return starting_offsets
        except Exception as e :
            errmsg = f'ERROR: encountered an exception when gathering initial "{topic_name}" topic offsets for '
            errmsg+= f'consumer group ID "{consumer_group_ID}". The error will be logged below and re-raised.'
            self.logger.error(errmsg,exc_obj=e)
            return None
