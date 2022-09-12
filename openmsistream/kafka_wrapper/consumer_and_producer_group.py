"""A set of Consumers and Producers that share share the same KafkaCrypto key-passing instance"""

#imports
from ..utilities.logging import LogOwner
from .consumer_group import ConsumerGroup
from .producer_group import ProducerGroup

class ConsumerAndProducerGroup(LogOwner) :
    """
    Class for working with a group of Consumers and Producers sharing a single :class:`kafkacrypto.KafkaCrypto` instance

    :param config_path: Path to the config file that should be used to define Consumers/Producers in the group
    :type config_path: :class:`pathlib.Path`
    :param consumer_topic_name: The name of the topic to which the Consumers should be subscribed
    :type consumer_topic_name: str
    :param consumer_group_id: The ID string that should be used for each Consumer in the group.
        "create_new" (the defaults) will create a new UID to use.
    :type consumer_group_id: str, optional
    """

    @property
    def consumer_topic_name(self) :
        """
        Name of the topic to which the consumers are subscribed
        """
        return self.__consumer_group.topic_name
    @property
    def consumer_group_id(self) :
        """
        Group ID of all consumers in the group
        """
        return self.__consumer_group.consumer_group_id

    def __init__(self,config_path,consumer_topic_name,*,consumer_group_id='create_new',**kwargs) :
        """
        Constructor method
        """
        super().__init__(**kwargs)
        self.__consumer_group = ConsumerGroup(config_path,consumer_topic_name,
                                              consumer_group_id=consumer_group_id,logger=self.logger)
        self.__producer_group = ProducerGroup(config_path,kafkacrypto=self.__consumer_group.kafkacrypto,
                                              logger=self.logger)

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
        return self.__consumer_group.get_new_subscribed_consumer(restart_at_beginning=restart_at_beginning,**kwargs)

    def get_new_producer(self) :
        """
        Return a new :class:`~OpenMSIStreamProducer` object.
        Call this function from a child thread to get thread-independent Producers.
        Note: this function just creates the Producer; closing it etc. must be handled by whatever calls this function.

        :return: a Producer created using the config set in the constructor
        :rtype: :class:`~OpenMSIStreamProducer`
        """
        return self.__producer_group.get_new_producer()

    def close(self) :
        """
        Wrapper around :func:`kafkacrypto.KafkaCrypto.close`.
        """
        self.__consumer_group.close()
        self.__producer_group.close()
