"""Miscellaneous functions/classes used elsewhere in the Kafka wrapper"""

#imports
import warnings
from collections import namedtuple
from confluent_kafka import OFFSET_BEGINNING
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from kafkacrypto import KafkaConsumer

def add_kwargs_to_configs(configs,logger,**kwargs) :
    """
    Add any kwargs with underscores replaced with dots to a given config dictionary
    """
    new_configs = configs.copy()
    for argname,arg in kwargs.items() :
        config_name = argname.replace('_','.')
        if config_name in new_configs and new_configs[config_name]!=arg :
            if logger is not None :
                msg = f'A new value for the "{config_name}" config has been supplied '
                msg+=  'by a keyword argument that will overwrite a previously-set value. '
                msg+= f'The value will be set to {arg} instead of {new_configs[config_name]}.'
                logger.info(msg)
        new_configs[config_name]=arg
    return new_configs

def default_producer_callback(err,msg,logger=None,**other_kwargs) :
    """
    Default callback function to use for testing whether a message has been successfully produced to the topic
    """
    if err is None and msg.error() is not None :
        err = msg.error()
    if err is not None: #raise an error if the message wasn't sent successfully
        if err.fatal() :
            logmsg =f'ERROR: fatally failed to deliver message with kwargs "{other_kwargs}". '
            logmsg+=f'MESSAGE DROPPED. Error reason: {err.str()}'
            if logger is not None :
                logger.error(logmsg)
            else :
                raise RuntimeError(logmsg)
        elif not err.retriable() :
            logmsg =f'ERROR: Failed to deliver message with kwargs "{other_kwargs}" and cannot retry. '
            logmsg+= f'MESSAGE DROPPED. Error reason: {err.str()}'
            if logger is not None :
                logger.error(logmsg)
            else :
                raise RuntimeError(logmsg)

def make_callback(func,*args,**kwargs) :
    """
    Callbacks are rendered at runtime and so regular functions used as producer callbacks
    need to be wrapped in this lambda
    """
    return lambda err,msg : func(err,msg,*args,**kwargs)

KCCommitOffsetDictKey = namedtuple('KCCommitOffsetDictKey',['topic','partition'])
KCCommitOffset = namedtuple('KCCommitOffset',['offset'])

def reset_to_beginning_on_assign(consumer, partitions):
    """
    Used as the "on_assign" parameter to a Consumer constructor to get each partition
    to start at its earliest message when it's assigned to a Consumer
    """
    for part in partitions:
        part.offset=OFFSET_BEGINNING
    if isinstance(consumer,KafkaConsumer) :
        consumer.assign_and_seek(partitions)
    else :
        consumer.assign(partitions)
