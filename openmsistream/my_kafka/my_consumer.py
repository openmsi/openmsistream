#imports
from confluent_kafka import DeserializingConsumer, Message
from kafkacrypto import KafkaConsumer
from ..shared.logging import LogOwner
from .utilities import add_kwargs_to_configs, KCCommitOffsetDictKey, KCCommitOffset
from .config_file_parser import MyKafkaConfigFileParser
from .my_kafka_crypto import MyKafkaCrypto
from .serialization import CompoundDeserializer

class MyConsumer(LogOwner) :
    """
    Convenience class for working with a Consumer of some type
    """

    def __init__(self,consumer_type,configs,kafkacrypto=None,**kwargs) :
        """
        consumer_type = the type of Consumer underlying this object
        configs = a dictionary of configurations to pass to the consumer to create it
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
            self.logger.error(f'ERROR: Unrecognized consumer type {consumer_type} for MyConsumer!',ValueError)
        self.configs = configs

    @staticmethod
    def get_consumer_args_kwargs(config_file_path,logger=None,**kwargs) :
        """
        Return the arguments and keyword arguments that should be used to create a MyConsumer based on the configs

        config_file_path = path to the config file to use in defining this consumer

        any keyword arguments will be added to the final consumer configs (with underscores replaced with dots)

        Used to quickly instantiate more than one identical MyConsumer for a ConsumerGroup
        """
        parser = MyKafkaConfigFileParser(config_file_path,logger=logger)
        ret_kwargs = {}
        #get the broker and consumer configurations
        all_configs = {**parser.broker_configs,**parser.consumer_configs}
        all_configs = add_kwargs_to_configs(all_configs,**kwargs)
        #all_configs['debug']='broker,topic,msg'
        #if there are configs for KafkaCrypto, use a KafkaConsumer
        if parser.kc_config_file_str is not None :
            if logger is not None :
                logger.debug(f'Consumed messages will be decrypted using configs at {parser.kc_config_file_str}')
            kc = MyKafkaCrypto(parser.broker_configs,parser.kc_config_file_str)
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
        args_to_use, kwargs_to_use = MyConsumer.get_consumer_args_kwargs(*args,**kwargs)
        return cls(*args_to_use,**kwargs_to_use)

    def get_next_message(self,*poll_args,**poll_kwargs) :
        """
        Call "poll" for this consumer and return any successfully consumed message's value
        otherwise log a warning if there's an error while calling poll

        For the case of encrypted messages, this may return a Message object with 
        KafkaCryptoMessages for its key and/or value if the message fails 
        to be decrypted within a certain amount of time
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
                warnmsg = 'WARNING: encountered an error in a call to consumer.poll() and this message will be skipped. '
                warnmsg+= f'Exception: {e}'
                self.logger.warning(warnmsg)
                #raise e
                return None
            if consumed_msg is not None and consumed_msg!={} :
                if consumed_msg.error() is not None or consumed_msg.value() is None :
                    warnmsg = f'WARNING: unexpected consumed message, consumed_msg = {consumed_msg}'
                    warnmsg+= f', consumed_msg.error() = {consumed_msg.error()}, consumed_msg.value() = {consumed_msg.value()}'
                    self.logger.warning(warnmsg)
                return consumed_msg
            else :
                return None

    def subscribe(self,*args,**kwargs) :
        self.__consumer.subscribe(*args,**kwargs)

    def commit(self,message=None,offsets=None,asynchronous=True) :
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
            self.logger.error(errmsg)
    
    def close(self,*args,**kwargs) :
        self.__consumer.close(*args,**kwargs)
        try :
            if self.__kafkacrypto :
                self.__kafkacrypto.close()
        except :
            pass
        finally :
            self.__kafkacrypto = None
