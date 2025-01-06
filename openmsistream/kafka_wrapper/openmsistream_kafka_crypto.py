"""
Wrapper around the KafkaCrypto producer/consumer pair communicating with the key passing topics
"""

# imports
import pathlib, configparser

from kafkacrypto import KafkaProducer, KafkaConsumer, KafkaCrypto, KafkaCryptoStore
from openmsitoolbox.utilities.misc import change_dir


class OpenMSIStreamKafkaCrypto:
    """
    A wrapper class to own and work with the Producers, Consumers, and other objects needed
    by KafkaCrypto to handle encrypting/decrypting messages and exchanging keys

    :param broker_configs: the producer/consumer-agnostic configurations for connecting
        to the broker
    :type broker_configs: dict
    :param config_file: the path to the KafkaCrypto config file for the node being used
    :type config_file: str
    """

    @property
    def config_file_path(self):
        """
        The path to the KafkaCrypto config file used
        """
        return self.__config_file

    @property
    def key_serializer(self):
        """
        The crypto key serializer
        """
        return self._kc.getKeySerializer()

    @property
    def key_deserializer(self):
        """
        The crypto key deserializer
        """
        return self._kc.getKeyDeserializer()

    @property
    def value_serializer(self):
        """
        The crypto value serializer
        """
        return self._kc.getValueSerializer()

    @property
    def value_deserializer(self):
        """
        The crypto value deserializer
        """
        return self._kc.getValueDeserializer()

    def __init__(self, broker_configs, config_file, log_level):
        """
        Constructor method
        """
        # get kafka crypto configs, and set logging level for kafkacrypto loggers
        kcp_cfgs, kcc_cfgs = self.__get_configs_from_file(
            broker_configs, pathlib.Path(config_file), log_level
        )
        with change_dir(pathlib.Path(config_file).parent):
            # start up the producer and consumer
            self._kcp = KafkaProducer(**kcp_cfgs)
            self._kcc = KafkaConsumer(**kcc_cfgs)
            # initialize the KafkaCrypto object
            self._kc = KafkaCrypto(None, self._kcp, self._kcc, config_file)
        self.__config_file = config_file

    def close(self):
        """
        Close the :class:`kafkacrypto.KafkaCrypto` object and un-set its producer/consumer
        """
        try:
            self._kc.close()
        except Exception:
            pass
        finally:
            self._kc = None
            self._kcp = None
            self._kcc = None

    def __get_configs_from_file(self, broker_configs, config_file, default_log_level):
        """Return the dictionaries of crypto producer and consumer configs determined
        from the KafkaCrypto config file and overwritten with the given broker configs
        from the OpenMSIStream config file. KafkaCryptoStore must be used when parsing
        the crypto config file to ensure options (and clearing of options) is properly
        handled.
        """
        # Make updates to kafkaconfig file according to what we are passed
        config = configparser.ConfigParser(delimiters=":")
        config.read(config_file)
        # Unilaterally pdate default log_level (can be overridden in -crypto subsection by user)
        section_name = f"{config_file.stem}"
        config.set(section_name, "log_level", str(default_log_level))
        # If ssl.ca.location is set in the broker configs, make sure it's written to the
        # KafkaCrypto config file as well in the right place
        if "ssl.ca.location" in broker_configs:
            section_name = f"{config_file.stem}-kafka"
            option_name = "ssl_cafile"
            if config.has_option(section_name, option_name):
                config.set(section_name, option_name, broker_configs["ssl.ca.location"])
        # Save changes to config_file
        with open(config_file, "w") as cfg_fp:
            config.write(cfg_fp)
        # Parse the config file and get consumer and producer configs
        cfg_parser = KafkaCryptoStore(
            str(config_file), conf_global_logger=False
        )  # this sets logging levels for kafkacrypto loggers
        kcc_cfgs = cfg_parser.get_kafka_config("consumer", extra="crypto")
        kcp_cfgs = cfg_parser.get_kafka_config("producer", extra="crypto")
        cfg_parser.close()
        # Overwrite with OpenMSIStream broker configs
        kcc_cfgs.update(broker_configs.copy())
        kcp_cfgs.update(broker_configs.copy())
        # KafkaCryptoStore automatically handles settings a group id if not already set
        # and makes sure unnecessary defaults (such as node_id) are not included.
        return kcp_cfgs, kcc_cfgs
