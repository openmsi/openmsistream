#imports
from ..shared.logging import LogOwner
from .my_producer import MyProducer

class ProducerGroup(LogOwner) :
    """
    Class for working with a group of producers sharing a single KafkaCrypto instance
    """

    def __init__(self,config_path,*args,**kwargs) :
        """
        config_path = path to the config file that should be used to define the producer group
        """
        super().__init__(*args,**kwargs)
        self.__p_args, self.__p_kwargs = MyProducer.get_producer_args_kwargs(config_path,logger=self.logger)

    def get_new_producer(self) :
        """
        Return a new Producer

        Call this function from a child thread to get thread-independent Producers

        This function just creates the Producer; closing it etc. must be handled by whatever calls this function.
        """
        producer = MyProducer(*self.__p_args,**self.__p_kwargs)
        return producer

    def close(self) :
        try :
            self.__p_kwargs['kafkacrypto'].close()
        except :
            pass
        finally :
            self.__p_kwargs['kafkacrypto'] = None