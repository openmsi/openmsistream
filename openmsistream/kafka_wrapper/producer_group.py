"""A set of Producers sharing an optional KafkaCrypto instance for key passing"""

# imports
from confluent_kafka import SerializingProducer
from kafkacrypto import KafkaProducer
from openmsitoolbox import LogOwner
from .openmsistream_producer import OpenMSIStreamProducer
from .serialization import CompoundSerializer


class ProducerGroup(LogOwner):
    """
    Class for working with a group of producers sharing a single
    :class:`kafkacrypto.KafkaCrypto` instance

    :param config_path: Path to the config file that should be used to define Producers in the group
    :type config_path: :class:`pathlib.Path`
    :param kafkacrypto: The :class:`~OpenMSIStreamKafkaCrypto` object that should be used
        to instantiate Producers. Only needed if a single specific
        :class:`~OpenMSIStreamKafkaCrypto` instance should be shared.
    :type kafkacrypto: :class:`~OpenMSIStreamKafkaCrypto`, optional
    """

    @property
    def kafkacrypto(self):
        """
        The KafkaCrypto object handling key passing and serialization for encrypted messages
        """
        return (
            self.__p_kwargs["kafkacrypto"] if "kafkacrypto" in self.__p_kwargs else None
        )

    def __init__(self, config_path, kafkacrypto=None, **kwargs):
        """
        Constructor method
        """
        super().__init__(**kwargs)
        self.__p_args, self.__p_kwargs = OpenMSIStreamProducer.get_producer_args_kwargs(
            config_path, logger=self.logger, kafkacrypto=kafkacrypto
        )

    def get_new_producer(
        self, key_serializer_override=None, value_serializer_override=None
    ):
        """
        Return a new :class:`~OpenMSIStreamProducer` object.
        Call this function from a child thread to get thread-independent Producers.
        Note: this function just creates the Producer; closing it etc. must be handled
        by whatever calls this function.

        :return: a Producer created using the config set in the constructor
        :rtype: :class:`~OpenMSIStreamProducer`
        """
        p_args = self.__p_args.copy()
        p_args[1] = p_args[1].copy()
        if p_args[0] == SerializingProducer:
            if key_serializer_override is not None:
                p_args[1]["key.serializer"] = key_serializer_override
            if value_serializer_override is not None:
                p_args[1]["value.serializer"] = value_serializer_override
        if p_args[0] == KafkaProducer:
            if key_serializer_override is not None:
                p_args[1]["key_serializer"] = CompoundSerializer(
                    key_serializer_override, self.kafkacrypto.key_serializer
                )
            if value_serializer_override is not None:
                p_args[1]["value_serializer"] = CompoundSerializer(
                    value_serializer_override, self.kafkacrypto.value_serializer
                )
        producer = OpenMSIStreamProducer(*p_args, **self.__p_kwargs)
        return producer

    def close(self):
        """
        Wrapper around :func:`kafkacrypto.KafkaCrypto.close`.
        """
        try:
            self.__p_kwargs["kafkacrypto"].close()
        except Exception:
            pass
        finally:
            self.__p_kwargs["kafkacrypto"] = None
