"""A wrapped Kafka Producer. May produce encrypted messages."""

#imports
import time, warnings
from confluent_kafka import SerializingProducer
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from kafkacrypto import KafkaProducer
from ..utilities import LogOwner
from ..utilities.misc import raise_err_with_optional_logger, debug_msg_with_optional_logger
from .utilities import add_kwargs_to_configs, default_producer_callback, make_callback
from .producible import Producible
from .config_file_parser import KafkaConfigFileParser
from .openmsistream_kafka_crypto import OpenMSIStreamKafkaCrypto
from .serialization import CompoundSerializer

class OpenMSIStreamProducer(LogOwner) :
    """
    Wrapper for working with a Producer of some type. Expects message values that are
    :class:`~.data_file_io.entity.data_file_chunk.DataFileChunk` objects by default;
    other message value types can be accommodated by setting "value.serializer" in the config file.

    :param producer_type: The type of underlying Producer that should be used
    :type producer_type: :class:`confluent_kafka.SerializingProducer` or :class:`kafkacrypto.KafkaProducer`
    :param configs: A dictionary of configuration names and parameters to use in instantiating the underlying Producer
    :type configs: dict
    :param kafkacrypto: The :class:`~.kafka_wrapper.OpenMSIStreamKafkaCrypto` object that should be used
        to instantiate the Producer. Only needed if `producer_type` is :class:`kafkacrypto.KafkaProducer`.
    :type kafkacrypto: :class:`~.kafka_wrapper.OpenMSIStreamKafkaCrypto`, optional
    :param kwargs: Any extra keyword arguments (other than "logger") are added to the configuration dict for the
        Producer, with underscores in their names replaced by dots
    :type kwargs: dict

    :raises ValueError: if `producer_type` is not :class:`confluent_kafka.SerializingProducer`
        or :class:`kafkacrypto.KafkaProducer`
    :raises ValueError: if `producer_type` is :class:`kafkacrypto.KafkaProducer` and `kafkacrypto` is None
    """

    POLL_EVERY = 5 # poll the producer at least every 5 calls to produce

    def __init__(self,producer_type,configs,kafkacrypto=None,**kwargs) :
        """
        Constructor method
        """
        super().__init__(**kwargs)
        if producer_type==KafkaProducer :
            if kafkacrypto is None :
                errmsg = 'ERROR: creating a KafkaProducer requires holding onto its KafkaCrypto objects!'
                self.logger.error(errmsg,exc_type=ValueError)
            self.__kafkacrypto = kafkacrypto
            self._producer = producer_type(**configs)
        elif producer_type==SerializingProducer :
            self._producer = producer_type(configs)
        else :
            errmsg=f'ERROR: Unrecognized producer type {producer_type} for OpenMSIStreamProducer!'
            self.logger.error(errmsg,exc_type=ValueError)
        # poll the producer at least every 5 calls to produce
        self.__poll_counter = 0
        # create an ID for the producer based on its location in memory
        self.producer_id = str(hex(id(self)))[2:]

    @staticmethod
    def get_producer_args_kwargs(config_file_path,logger=None,kafkacrypto=None,**kwargs) :
        """
        Return the list of arguments and dictionary or keyword arguments that should be used to instantiate
        :class:`~OpenMSIStreamProducer` objects based on the given config file.
        Used to share a single :class:`~.kafka_wrapper.OpenMSIStreamKafkaCrypto` instance across
        several Producers.

        :param config_file_path: Path to the config file to use in defining Producers
        :type config_file_path: :class:`pathlib.Path`
        :param logger: The :class:`~.utilities.Logger` object to use for each of the
            :class:`~OpenMSIStreamProducer` objects
        :type logger: :class:`~.utilities.Logger`
        :param kafkacrypto: The :class:`~.kafka_wrapper.OpenMSIStreamKafkaCrypto` object that should
            be used to instantiate Producers. Only needed if a single specific
            :class:`~.kafka_wrapper.OpenMSIStreamKafkaCrypto` instance should be shared.
        :type kafkacrypto: :class:`~.kafka_wrapper.OpenMSIStreamKafkaCrypto`, optional
        :param kwargs: Any extra keyword arguments are added to the configuration dict for the Producers,
            with underscores in their names replaced by dots
        :type kwargs: dict

        :return: ret_args, the list of arguments to create new :class:`~OpenMSIStreamProducer` objects
            based on the config file and arguments to this method
        :rtype: list
        :return: ret_kwargs, the dictionary of keyword arguments to create new :class:`~OpenMSIStreamProducer` objects
            based on the config file and arguments to this method
        :rtype: dict
        """
        parser = KafkaConfigFileParser(config_file_path,logger=logger)
        ret_kwargs = {'logger':logger}
        #get the broker and producer configurations
        all_configs = {**parser.broker_configs,**parser.producer_configs}
        all_configs = add_kwargs_to_configs(all_configs,logger,**kwargs)
        #all_configs['debug']='broker,topic,msg'
        #use a SerializingProducer by default
        if kafkacrypto is None and parser.kc_config_file_str is None :
            ret_args = [SerializingProducer,all_configs]
            return ret_args, ret_kwargs
        #if "kafkacrypto" is given, or there are configs for KafkaCrypto, use a KafkaProducer
        if kafkacrypto is not None :
            if not isinstance(kafkacrypto,OpenMSIStreamKafkaCrypto) :
                errmsg = f'ERROR: kafkacrypto argument type = {type(kafkacrypto)}'
                raise_err_with_optional_logger(logger,errmsg,TypeError)
            debugmsg = f'Produced messages will be encrypted using configs at {kafkacrypto.config_file_path}'
            debug_msg_with_optional_logger(logger,debugmsg)
            k_c = kafkacrypto
        else :
            debugmsg = f'Produced messages will be encrypted using configs at {parser.kc_config_file_str}'
            debug_msg_with_optional_logger(logger,debugmsg)
            k_c = OpenMSIStreamKafkaCrypto(parser.broker_configs,parser.kc_config_file_str)
        if 'key.serializer' in all_configs :
            keyser = CompoundSerializer(all_configs.pop('key.serializer'),k_c.key_serializer)
        else :
            keyser = k_c.key_serializer
        if 'value.serializer' in all_configs :
            valser = CompoundSerializer(all_configs.pop('value.serializer'),k_c.value_serializer)
        else :
            valser = k_c.value_serializer
        all_configs['key_serializer']=keyser
        all_configs['value_serializer']=valser
        ret_args = [KafkaProducer,all_configs]
        ret_kwargs['kafkacrypto']=k_c
        return ret_args, ret_kwargs

    @classmethod
    def from_file(cls,*args,**kwargs) :
        """
        Wrapper around :func:`~OpenMSIStreamProducer.get_producer_args_kwargs` and the :class:`~OpenMSIStreamProducer`
        constructor to return a single :class:`~OpenMSIStreamProducer` from a given config file/arguments.
        Arguments are the same as :func:`~OpenMSIStreamProducer.get_producer_args_kwargs`

        :returns: An :class:`~OpenMSIStreamProducer` object based on the given config file/arguments
        :rtype: :class:`~OpenMSIStreamProducer`
        """
        args_to_use, kwargs_to_use = OpenMSIStreamProducer.get_producer_args_kwargs(*args,**kwargs)
        return cls(*args_to_use,**kwargs_to_use)

    def produce_from_queue(self,queue,topic_name,**kwargs) :
        """
        Get a :class:`~.kafka_wrapper.Producible` object from a given Queue and produce it to
        the given topic. Does nothing if the queue is empty, and does not block waiting for items from the queue.

        Meant to be run in multiple threads in parallel.

        :param queue: the :class:`queue.Queue` holding objects that should be produced
        :type queue: :class:`queue.Queue`
        :param topic_name: the name of the topic to produce to
        :type topic_name: str
        :param kwargs: other keyword arguments are passed to :func:`~produce_object`
        :type kwargs: dict
        """
        if queue.empty() :
            return
        #get the next object from the Queue
        obj = queue.get_nowait()
        if isinstance(obj,Producible) :
            success = self.produce_object(obj,topic_name,**kwargs)
            if not success :
                warnmsg = f'WARNING: message with key {obj.msg_key} failed to buffer for longer than '
                warnmsg+=  'the timeout with no new callbacks served. This message will be re-enqueued.'
                self.logger.warning(warnmsg)
                queue.put(obj)
            self.__poll_counter+=1
            if self.__poll_counter%self.POLL_EVERY==0 :
                _ = self.poll(0)
                self.__poll_counter = 0
        else :
            warnmsg = f'WARNING: found an object of type {type(obj)} in a Producer queue that should only contain '
            warnmsg+= 'Producible objects. This object will be skipped!'
            self.logger.warning(warnmsg)

    def produce_from_queue_looped(self,queue,topic_name,**kwargs) :
        """
        Get :class:`~.kafka_wrapper.Producible` objects from a given Queue and produce
        them to the given topic. Blocks while waiting for items to appear in the queue.
        Runs until "None" is pulled from the queue.

        Meant to be run in multiple threads in parallel.

        :param queue: the :class:`queue.Queue` holding objects that should be produced
        :type queue: :class:`queue.Queue`
        :param topic_name: the name of the topic to produce to
        :type topic_name: str
        :param kwargs: other keyword arguments are passed to :func:`~produce_object`
        :type kwargs: dict
        """
        #get the next object from the Queue
        obj = queue.get()
        #loop until "None" is pulled from the Queue
        while obj is not None :
            if isinstance(obj,Producible) :
                success = self.produce_object(obj,topic_name,**kwargs)
                if not success :
                    warnmsg = f'WARNING: message with key {obj.msg_key} failed to buffer for longer than '
                    warnmsg+=  'the timeout with no new callbacks served. This message will be re-enqueued.'
                    self.logger.warning(warnmsg)
                    queue.put(obj)
                self.__poll_counter+=1
                if self.__poll_counter%self.POLL_EVERY==0 :
                    _ = self.poll(0)
                    self.__poll_counter = 0
            else :
                warnmsg = f'WARNING: found an object of type {type(obj)} in a Producer queue that should only contain '
                warnmsg+= 'Producible objects. This object will be skipped!'
                self.logger.warning(warnmsg)
            #get the next object in the Queue
            obj = queue.get()
        queue.task_done()

    def produce_object(self,obj,topic_name,callback=None,print_every=1000,timeout=60,retry_sleep=5) :
        """
        Produce a given :class:`~.kafka_wrapper.Producible` object to a given topic,
        with some handling for BufferErrors, calling poll() automatically, and using callbacks
        constructed on the fly.

        :param obj: the object to produce
        :type obj: :class:`~.kafka_wrapper.Producible`
        :param topic_name: the name of the topic to produce to
        :type topic_name: str
        :param callback: a function that should be called for each message upon recognition by the broker.
            Will be wrapped in a lambda for each call to produce().
            Arguments to the callback function are determined by the particular type of :class:`Producible` object used
        :type callback: producer callback function (takes at least "err" and "msg" arguments), optional
        :param print_every: print/log progress every (this many) messages
        :type print_every: int, optional
        :param timeout: max time (seconds) to wait for the message to be produced in the event of
            (repeated) BufferError(s)
        :type timeout: float, optional
        :param retry_sleep: how long (seconds) to wait between produce attempts if one fails with a BufferError
        :type retry_sleep: float, optional

        :return: True if the call to produce is successful, False otherwise.
        :rtype: bool
        """
        #log a line about this message if applicable
        logmsg = obj.get_log_msg(print_every)
        if logmsg is not None :
            self.logger.debug(logmsg)
        #get the Producible's callback arguments and set the callback to use
        if callback is None :
            callback_to_use = make_callback(default_producer_callback,self.producer_id,logger=self.logger,
                                            **obj.callback_kwargs)
        else :
            callback_to_use = make_callback(callback,self.producer_id,**obj.callback_kwargs)
        #produce the message to the topic
        success=False
        total_wait_secs=0
        while (not success) and total_wait_secs<timeout :
            try :
                self.produce(topic=topic_name,key=obj.msg_key,value=obj.msg_value,on_delivery=callback_to_use)
                success=True
            except BufferError :
                n_new_callbacks = self.poll(0)
                time.sleep(retry_sleep)
                if n_new_callbacks is None or n_new_callbacks==0 :
                    total_wait_secs+=retry_sleep
                else :
                    total_wait_secs = 0
            except Exception as exc :
                errmsg = 'ERROR: failed during call to Producer.produce! Will log and re-raise Exception.'
                self.logger.error(errmsg,exc_info=exc,reraise=True)
        self.__poll_counter+=1
        if self.__poll_counter%self.POLL_EVERY==0 :
            n_new_callbacks = self.poll(0)
            self.__poll_counter = 0
        return success

    def produce(self,topic,key,value,**kwargs) :
        """
        Produce a message to a topic.
        Other kwargs are passed through to the underlying producer's produce() function.

        :param topic: the name of the topic to produce to
        :type topic: str
        :param key: the key of the message
        :type key: depends on serialization settings
        :param value: the value of the message
        :type value: depends on the serialization settings
        """
        if isinstance(self._producer,KafkaProducer) :
            key = self._producer.ks(topic,key)
            value = self._producer.vs(topic,value)
        return self._producer.produce(topic=topic,key=key,value=value,**kwargs)

    def poll(self,*args,**kwargs) :
        """
        Wrapper around Producer.poll()
        """
        return self._producer.poll(*args,**kwargs)

    def flush(self,*args,**kwargs) :
        """
        Wrapper around Producer.flush()
        """
        return self._producer.flush(*args,**kwargs)

    def close(self) :
        """
        Wrapper around :func:`kafkacrypto.KafkaCrypto.close`.
        It's important to call this on shutdown if the Producer is producing encrypted messages.
        """
        try :
            if self.__kafkacrypto :
                self.__kafkacrypto.close()
        except Exception :
            pass
        finally :
            self.__kafkacrypto = None
