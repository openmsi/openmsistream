#imports
from collections import namedtuple

def add_kwargs_to_configs(configs,**kwargs) :
    """
    Add any kwargs with underscores replaced with dots to a given config dictionary
    """
    new_configs = configs.copy()
    for argname,arg in kwargs.items() :
        new_configs[argname.replace('_','.')]=arg
    return new_configs

#a very small class (and instance thereof) to hold a logger object to use in the default producer callback 
# (literally exists because I don't think I can add extra keyword or other arguments to the producer callback function)
class ProducerCallbackLogger :

    @property
    def logger(self) :
        return self._logger
    @logger.setter
    def logger(self,logger_val) :
        self._logger = logger_val
    def __init__(self) :
        self._logger = None

#a callback function to use for testing whether a message has been successfully produced to the topic
def default_producer_callback(err,msg,logger=None,**other_kwargs) :
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
    return lambda err,msg : func(err,msg,*args,**kwargs)


KCCommitOffsetDictKey = namedtuple('KCCommitOffsetDictKey',['topic','partition'])
KCCommitOffset = namedtuple('KCCommitOffset',['offset'])
