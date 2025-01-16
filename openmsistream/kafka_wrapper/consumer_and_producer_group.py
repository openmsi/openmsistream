"""A set of Consumers and Producers that share share the same KafkaCrypto key-passing instance"""

# imports
from openmsitoolbox import LogOwner
from .consumer_group import ConsumerGroup
from .producer_group import ProducerGroup


class ConsumerAndProducerGroup(LogOwner):
    """
    Class for working with a group of Consumers and Producers sharing a single
    :class:`kafkacrypto.KafkaCrypto` instance

    :param config_path: Path to the config file that should be used to define
        Consumers/Producers in the group
    :type config_path: :class:`pathlib.Path`
    :param consumer_topic_name: The name of the topic to which the Consumers should be subscribed
    :type consumer_topic_name: str
    :param consumer_group_id: The ID string that should be used for each Consumer in the group.
        "create_new" (the defaults) will create a new UID to use.
    :type consumer_group_id: str, optional
    :param kafkacrypto: The :class:`~OpenMSIStreamKafkaCrypto` object that should be used
        to instantiate Consumers. Only needed if a single specific
        :class:`~OpenMSIStreamKafkaCrypto` instance should be shared.
    :type kafkacrypto: :class:`~OpenMSIStreamKafkaCrypto`, optional
    :param treat_undecryptable_as_plaintext: If True, the KafkaCrypto Deserializers
        will immediately return any keys/values that are not possibly decryptable as
        binary data. This allows faster handling of messages that will never be
        decryptable, such as when enabling or disabling encryption across a platform,
        or when unencrypted messages are mixed in a topic with encrypted messages.
    :type treat_undecryptable_as_plaintext: boolean, optional
    :param max_wait_per_decrypt: Number of seconds a KafkaCrypto Deserializer
        waits before giving up.
    :type max_wait_per_decrypt: float, optional
    :param max_initial_wait_per_decrypt: Number of seconds a KafkaCrypto Deserializer
        waits before giving up on the first message it sees.
    :type max_initial_wait_per_decrypt: float, optional
    """

    @property
    def kafkacrypto(self):
        """
        The KafkaCrypto object handling key passing and serdes for encrypted messages
        """
        return self.__kafkacrypto

    @property
    def consumer_topic_name(self):
        """
        Name of the topic to which the consumers are subscribed
        """
        if self.__consumer_group is not None:
            return self.__consumer_group.consumer_topic_name
        return None

    @property
    def consumer_group_id(self):
        """
        Group ID of all consumers in the group
        """
        if self.__consumer_group is not None:
            return self.__consumer_group.consumer_group_id
        return None

    def __init__(
        self,
        config_path,
        *,
        consumer_topic_name=None,
        consumer_group_id="create_new",
        kafkacrypto=None,
        treat_undecryptable_as_plaintext=False,
        max_wait_per_decrypt=5.0,
        max_initial_wait_per_decrypt=60.0,
        **kwargs,
    ):
        """
        Constructor method
        """
        # must remove producer_topic_name since we define it as
        # part of this class even though we do not use it in init.
        other_kwargs = kwargs.copy()
        if "producer_topic_name" in other_kwargs:
            del other_kwargs["producer_topic_name"]
        super().__init__(**other_kwargs)
        self.__producer_group = ProducerGroup(
            config_path, kafkacrypto=kafkacrypto, logger=self.logger
        )
        self.__kafkacrypto = self.__producer_group.kafkacrypto
        self.__consumer_group = None
        if consumer_topic_name is not None:
            self.__consumer_group = ConsumerGroup(
                config_path,
                consumer_topic_name,
                consumer_group_id=consumer_group_id,
                kafkacrypto=self.__producer_group.kafkacrypto,
                treat_undecryptable_as_plaintext=treat_undecryptable_as_plaintext,
                max_wait_per_decrypt=max_wait_per_decrypt,
                max_initial_wait_per_decrypt=max_initial_wait_per_decrypt,
                logger=self.logger,
            )

    def get_new_subscribed_consumer(self, *, restart_at_beginning=False, **kwargs):
        """
        Return a new Consumer, subscribed to the topic and with the shared group ID.
        Call this function from a child thread to get thread-independent Consumers.

        Note: This function just creates and subscribes the Consumer. Polling it, closing
        it, and everything else must be handled by whatever calls this function.

        :param restart_at_beginning: if True, the new Consumer will start reading partitions
            from the earliest messages available, regardless of Consumer group ID and
            auto.offset.reset values. Useful when re-reading messages.
        :type restart_at_beginning: bool, optional
        :param kwargs: other keyword arguments are passed to the
            :class:`~OpenMSIStreamConsumer` constructor method
        :type kwargs: dict

        :return: a Consumer created using the configs set in the constructor/from `kwargs`,
            subscribed to the topic
        :rtype: :class:`~OpenMSIStreamConsumer`
        """
        if self.__consumer_group is None:
            return None
        return self.__consumer_group.get_new_subscribed_consumer(
            restart_at_beginning=restart_at_beginning, **kwargs
        )

    def get_new_producer(self, **kwargs):
        """
        Return a new :class:`~OpenMSIStreamProducer` object.
        Call this function from a child thread to get thread-independent Producers.
        Note: this function just creates the Producer; closing it etc. must be handled by
        whatever calls this function.

        :return: a Producer created using the config set in the constructor
        :rtype: :class:`~OpenMSIStreamProducer`
        """
        return self.__producer_group.get_new_producer(**kwargs)

    def close(self):
        """
        Wrapper around :func:`kafkacrypto.KafkaCrypto.close`.
        """
        if self.__consumer_group is not None:
            self.__consumer_group.close()
        self.__producer_group.close()

    @classmethod
    def get_command_line_arguments(cls):
        """
        Anything extending this class should be able to access the
        "treat_undecryptable_as_plaintext" flag and the
        "max(_initial)_wait_per_decrypt" settings
        """
        superargs, superkwargs = super().get_command_line_arguments()
        args = [
            *superargs,
            "config",
            "topic_name",
            "consumer_topic_name",
            "consumer_group_id",
            "producer_topic_name",
            "treat_undecryptable_as_plaintext",
            "max_wait_per_decrypt",
            "max_initial_wait_per_decrypt",
        ]
        return args, superkwargs

    @classmethod
    def get_init_args_kwargs(cls, parsed_args):
        superargs, superkwargs = super().get_init_args_kwargs(parsed_args)
        args = [
            *superargs,
            parsed_args.config,
        ]
        kwargs = {
            **superkwargs,
            "consumer_topic_name": parsed_args.consumer_topic_name,
            "consumer_group_id": parsed_args.consumer_group_id,
            "producer_topic_name": parsed_args.producer_topic_name,
            "treat_undecryptable_as_plaintext": parsed_args.treat_undecryptable_as_plaintext,
            "max_wait_per_decrypt": parsed_args.max_wait_per_decrypt,
            "max_initial_wait_per_decrypt": parsed_args.max_initial_wait_per_decrypt,
        }
        return args, kwargs
