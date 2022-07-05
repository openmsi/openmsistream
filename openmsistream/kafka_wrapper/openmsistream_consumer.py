#imports
import uuid
from confluent_kafka import DeserializingConsumer, Message
from kafkacrypto import KafkaConsumer
from ..utilities import LogOwner
from .utilities import add_kwargs_to_configs, KCCommitOffsetDictKey, KCCommitOffset
from .config_file_parser import KafkaConfigFileParser
from .openmsistream_kafka_crypto import OpenMSIStreamKafkaCrypto
from .serialization import CompoundDeserializer

class OpenMSIStreamConsumer(LogOwner) :
    """
    Wrapper for working with a Consumer of some type

    :param consumer_type: The type of underlying Consumer that should be used
    :type consumer_type: :class:`confluent_kafka.DeserializingConsumer` or :class:`kafkacrypto.KafkaConsumer`
    :param configs: A dictionary of configuration names and parameters to use in instantiating the underlying Consumer
    :type configs: dict
    :param kafkacrypto: The :class:`~OpenMSIStreamKafkaCrypto` object that should be used to instantiate the Consumer. 
        Only needed if `consumer_type` is :class:`kafkacrypto.KafkaConsumer`.
    :type kafkacrypto: :class:`~OpenMSIStreamKafkaCrypto`, optional
    :param kwargs: Any extra keyword arguments are added to the configuration dict for the Consumer, 
        with underscores in their names replaced by dots
    :type kwargs: dict

    :raises ValueError: if `consumer_type` is not :class:`confluent_kafka.DeserializingConsumer` 
        or :class:`kafkacrypto.KafkaConsumer`
    :raises ValueError: if `consumer_type` is :class:`kafkacrypto.KafkaConsumer` and `kafkacrypto` is None
    """

    def __init__(self,consumer_type,configs,kafkacrypto=None,**kwargs) :
        """
        Constructor method
        """
        super().__init__(**kwargs)
        if consumer_type==KafkaConsumer :
            if kafkacrypto is None :
                self.logger.error('ERROR: creating a KafkaConsumer requires holding onto its KafkaCrypto objects!')
            self.__kafkacrypto = kafkacrypto
            self.__consumer = consumer_type(**configs)
            self.__messages = []
        elif consumer_type==DeserializingConsumer :
            self.__consumer = consumer_type(configs)
        else :
            errmsg=f'ERROR: Unrecognized consumer type {consumer_type} for OpenMSIStreamConsumer!'
            self.logger.error(errmsg,ValueError)
        self.configs = configs

    @staticmethod
    def get_consumer_args_kwargs(config_file_path,logger=None,**kwargs) :
        """
        Return the list of arguments and dictionary or keyword arguments that should be used to instantiate 
        :class:`~OpenMSIStreamConsumer` objects based on the given config file. 
        Used to share a single :class:`~OpenMSIStreamKafkaCrypto` instance across several Consumers.

        :param config_file_path: Path to the config file to use in defining Consumers
        :type config_file_path: :class:`pathlib.Path`
        :param logger: The :class:`openmsistream.utilities.Logger` object to use for each of the 
            :class:`~OpenMSIStreamConsumer` objects
        :type logger: :class:`openmsistream.utilities.Logger`
        :param kwargs: Any extra keyword arguments are added to the configuration dict for the Consumers, 
            with underscores in their names replaced by dots
        :type kwargs: dict

        :return: ret_args, the list of arguments to create new :class:`~OpenMSIStreamConsumer` objects 
            based on the config file and arguments to this method
        :rtype: list
        :return: ret_kwargs, the dictionary of keyword arguments to create new :class:`~OpenMSIStreamConsumer` objects 
            based on the config file and arguments to this method
        :rtype: dict
        """
        parser = KafkaConfigFileParser(config_file_path,logger=logger)
        ret_kwargs = {}
        #get the broker and consumer configurations
        all_configs = {**parser.broker_configs,**parser.consumer_configs}
        all_configs = add_kwargs_to_configs(all_configs,logger,**kwargs)
        #all_configs['debug']='broker,topic,msg'
        #if the group.id has been set as "create_new" generate a new group ID
        if 'group.id' in all_configs.keys() and all_configs['group.id'].lower()=='create_new' :
            all_configs['group.id']=str(uuid.uuid1())
        #if there are configs for KafkaCrypto, use a KafkaConsumer
        if parser.kc_config_file_str is not None :
            if logger is not None :
                logger.debug(f'Consumed messages will be decrypted using configs at {parser.kc_config_file_str}')
            kc = OpenMSIStreamKafkaCrypto(parser.broker_configs,parser.kc_config_file_str)
            if 'key.deserializer' in all_configs.keys() :
                keydes = CompoundDeserializer(kc.key_deserializer,all_configs.pop('key.deserializer'))
            else :
                keydes = kc.key_deserializer
            if 'value.deserializer' in all_configs.keys() :
                valdes = CompoundDeserializer(kc.value_deserializer,all_configs.pop('value.deserializer'))
            else :
                valdes = kc.value_deserializer
            all_configs['key_deserializer']=keydes
            all_configs['value_deserializer']=valdes
            ret_args = [KafkaConsumer,all_configs]
            ret_kwargs['kafkacrypto']=kc
        #otherwise use a DeserializingConsumer
        else :
            ret_args = [DeserializingConsumer,all_configs]
        ret_kwargs['logger'] = logger
        return ret_args, ret_kwargs

    @classmethod
    def from_file(cls,*args,**kwargs) :
        """
        Wrapper around :func:`~OpenMSIStreamConsumer.get_consumer_args_kwargs` and the :class:`~OpenMSIStreamConsumer` 
        constructor to return a single :class:`~OpenMSIStreamConsumer` from a given config file/arguments. 
        Arguments are the same as :func:`~OpenMSIStreamConsumer.get_consumer_args_kwargs`

        :returns: An :class:`~OpenMSIStreamConsumer` object based on the given config file/arguments
        :rtype: :class:`~OpenMSIStreamConsumer`
        """
        args_to_use, kwargs_to_use = OpenMSIStreamConsumer.get_consumer_args_kwargs(*args,**kwargs)
        return cls(*args_to_use,**kwargs_to_use)

    def get_next_message(self,*poll_args,**poll_kwargs) :
        """
        Call poll() for the underlying consumer and return any successfully consumed message's value.
        Log a warning if there's an error while calling poll().

        For the case of encrypted messages, this may return a :class:`kafkacrypto.Message` object with 
        :class:`kafkacrypto.KafkaCryptoMessages` for its key and/or value if the message fails to be 
        decrypted within a certain amount of time

        All arguments/keyword arguments are passed through to the underlying Consumer's poll() function.

        :return: a single consumed :class:`confluent_kafka.Message` object, an undecrypted 
            :class:`kafkacrypto.Message` object, or None
        :rtype: :class:`confluent_kafka.Message`, :class:`kafkacrypto.Message`, or None
        """ 
        #There's one version for the result of a KafkaConsumer.poll() call
        if isinstance(self.__consumer,KafkaConsumer) :
            #check if there are any messages still waiting to be processed from a recent KafkaCrypto poll call
            if isinstance(self.__consumer,KafkaConsumer) and len(self.__messages)>0 :
                consumed_msg = self.__messages.pop(0)
                return consumed_msg
            msg_dict = self.__consumer.poll(*poll_args,**poll_kwargs)
            if msg_dict=={} :
                return
            for pk in msg_dict.keys() :
                for m in msg_dict[pk] :
                    self.__messages.append(m)
            return self.get_next_message(*poll_args,**poll_kwargs)
        #And another version for a regular Consumer
        else :
            consumed_msg = None
            try :
                consumed_msg = self.__consumer.poll(*poll_args,**poll_kwargs)
            except Exception as e :
                warnmsg = 'WARNING: encountered an error in a call to consumer.poll() '
                warnmsg+= f'and this message will be skipped. Exception: {e}'
                self.logger.warning(warnmsg)
                #raise e
                return None
            if consumed_msg is not None and consumed_msg!={} :
                if consumed_msg.error() is not None or consumed_msg.value() is None :
                    warnmsg = f'WARNING: unexpected consumed message, consumed_msg = {consumed_msg}'
                    warnmsg+= f', consumed_msg.error() = {consumed_msg.error()}, '
                    warnmsg+= f'consumed_msg.value() = {consumed_msg.value()}'
                    self.logger.warning(warnmsg)
                return consumed_msg
            else :
                return None

    def subscribe(self,*args,**kwargs) :
        """
        A wrapper around the underlying Consumer's subscribe() method
        """
        return self.__consumer.subscribe(*args,**kwargs)

    def commit(self,message=None,offsets=None,asynchronous=True) :
        """
        A wrapper around the underlying Consumer's commit() method. 
        Exactly one of `message` and `offsets` must be given.

        :param message: The message whose offset should be committed
        :type message: :class:`confluent_kafka.Message` or :class:`kafkacrypto.Message`, optional
        :param offsets: The list of topic+partitions+offsets to commit
        :type offsets: list(:class:`confluent_kafka.TopicPartition`), optional
        :param asynchronous: If True, the Consumer.commit call will return immediately and run asynchronously 
            instead of blocking
        :type asynchronous: bool

        :raises ValueError: if `message` and `offsets` are both given
        """
        if (message is not None) and (not isinstance(message,Message)) :
            try :
                offset_dict = {KCCommitOffsetDictKey(message.topic,message.partition):KCCommitOffset(message.offset)}
                if asynchronous :
                    return self.__consumer.commit_async(offsets=offset_dict)
                else :
                    return self.__consumer.commit(offsets=offset_dict)
            except :
                warnmsg = 'WARNING: failed to commit an offset for an encrypted message. '
                warnmsg = 'Duplicates may result if the Consumer is restarted.'
                self.logger.warning(warnmsg)
                return None
        if message is None :
            return self.__consumer.commit(offsets=offsets,asynchronous=asynchronous)
        elif offsets is None :
            return self.__consumer.commit(message=message,asynchronous=asynchronous)
        else :
            errmsg = 'ERROR: "message" and "offset" arguments are exclusive for Consumer.commit. Nothing commited.'
            self.logger.error(errmsg,ValueError)
    
    def close(self,*args,**kwargs) :
        """
        Combined wrapper around the underlying Consumer's close() method and :func:`kafkacrypto.KafkaCrypto.close`. 
        """
        self.__consumer.close(*args,**kwargs)
        try :
            if self.__kafkacrypto :
                self.__kafkacrypto.close()
        except :
            pass
        finally :
            self.__kafkacrypto = None
