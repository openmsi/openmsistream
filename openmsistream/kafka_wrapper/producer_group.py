"""A set of Producers sharing an optional KafkaCrypto instance for key passing"""

#imports
from ..utilities import LogOwner
from .openmsistream_producer import OpenMSIStreamProducer

class ProducerGroup(LogOwner) :
    """
    Class for working with a group of producers sharing a single :class:`kafkacrypto.KafkaCrypto` instance

    :param config_path: Path to the config file that should be used to define Producers in the group
    :type config_path: :class:`pathlib.Path`
    :param kafkacrypto: The :class:`~OpenMSIStreamKafkaCrypto` object that should be used to instantiate Producers.
        Only needed if a single specific :class:`~OpenMSIStreamKafkaCrypto` instance should be shared.
    :type kafkacrypto: :class:`~OpenMSIStreamKafkaCrypto`, optional
    """

    @property
    def kafkacrypto(self) :
        """
        The KafkaCrypto object handling key passing and serialization for encrypted messages
        """
        return self.__p_kwargs['kafkacrypto'] if 'kafkacrypto' in self.__p_kwargs else None

    def __init__(self,config_path,kafkacrypto=None,**kwargs) :
        """
        Constructor method
        """
        super().__init__(**kwargs)
        self.__p_args, self.__p_kwargs = OpenMSIStreamProducer.get_producer_args_kwargs(config_path,
                                                                                        logger=self.logger,
                                                                                        kafkacrypto=kafkacrypto)

    def get_new_producer(self) :
        """
        Return a new :class:`~OpenMSIStreamProducer` object.
        Call this function from a child thread to get thread-independent Producers.
        Note: this function just creates the Producer; closing it etc. must be handled by whatever calls this function.

        :return: a Producer created using the config set in the constructor
        :rtype: :class:`~OpenMSIStreamProducer`
        """
        producer = OpenMSIStreamProducer(*self.__p_args,**self.__p_kwargs)
        return producer

    def close(self) :
        """
        Wrapper around :func:`kafkacrypto.KafkaCrypto.close`.
        """
        try :
            self.__p_kwargs['kafkacrypto'].close()
        except Exception :
            pass
        finally :
            self.__p_kwargs['kafkacrypto'] = None
