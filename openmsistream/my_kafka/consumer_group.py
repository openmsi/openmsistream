#imports
import uuid
from ..shared.logging import LogOwner
from .my_consumer import MyConsumer

class ConsumerGroup(LogOwner) :
    """
    Class for working with a group of consumers
    """

    @property
    def topic_name(self) :
        return self.__topic_name

    def __init__(self,config_path,topic_name,*args,consumer_group_ID=str(uuid.uuid1()),**kwargs) :
        """
        arguments:
        config_path = path to the config file that should be used to define the consumer group
        topic_name  = name of the topic to consume messages from

        keyword arguments:
        consumer_group_ID = ID to use for all consumers in the group (a new & unique ID is created by default)
        """
        super().__init__(*args,**kwargs)
        self.__topic_name = topic_name
        self.__c_args, self.__c_kwargs = MyConsumer.get_consumer_args_kwargs(config_path,
                                                                             group_id=consumer_group_ID,
                                                                             logger=self.logger)

    def get_new_subscribed_consumer(self) :
        """
        Return a new Consumer, subscribed to the topic and with the shared group ID
        Call this function from a child thread to get thread-independent Consumers 
        This function just creates and subscribes the Consumer. Polling it, closing 
        it, and everything else must be handled by whatever calls this function.
        """
        consumer = MyConsumer(*self.__c_args,**self.__c_kwargs)
        consumer.subscribe([self.__topic_name])
        return consumer

    def close(self) :
        try :
            self.__c_kwargs['kafkacrypto'].close()
        except :
            pass
        finally :
            self.__c_kwargs['kafkacrypto'] = None
