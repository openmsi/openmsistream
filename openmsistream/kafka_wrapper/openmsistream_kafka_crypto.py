"""
Wrapper around the KafkaCrypto producer/consumer pair communicating with the key passing topics
"""

# imports
import pathlib, uuid, warnings, logging

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from kafkacrypto import KafkaProducer, KafkaConsumer, KafkaCrypto
from openmsitoolbox.utilities.misc import change_dir
from ..utilities.config_file_parser import ConfigFileParser


class OpenMSIStreamKafkaCrypto:
    """
    A wrapper class to own and work with the Producers, Consumers, and other objects needed
    by KafkaCrypto to handle encrypting/decrypting messages and exchanging keys

    :param broker_configs: the producer/consumer-agnostic configurations for connecting
        to the broker
    :type broker_configs: dict
    :param config_file: the path to the KafkaCrypto config file for the node being used
    :type config_file: :class:`pathlib.Path`
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

    def __init__(self, broker_configs, config_file):
        """
        Constructor method
        """
        kcp_cfgs, kcc_cfgs = self.__get_configs_from_file(broker_configs, config_file)
        with change_dir(pathlib.Path(config_file).parent):
            kc_logger = logging.getLogger("kafkacrypto")
            kc_logger.setLevel(logging.ERROR)
            # start up the producer and consumer
            self._kcp = KafkaProducer(**kcp_cfgs)
            self._kcc = KafkaConsumer(**kcc_cfgs)
            # initialize the KafkaCrypto object
            self._kc = KafkaCrypto(None, self._kcp, self._kcc, config_file)
            kc_logger.setLevel(logging.WARNING)
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

    def __get_configs_from_file(self, broker_configs, config_file):
        """Return the dictionaries of crypto producer and consumer configs determined
        from the KafkaCrypto config file and overwritten with the given broker configs
        from the OpenMSIStream config file
        """
        cfg_parser = ConfigFileParser(config_file)
        node_id = cfg_parser.get_config_dict_for_groups("DEFAULT")["node_id"]
        kcp_cfgs = {}
        kcc_cfgs = {}
        for section_name_stem in (f"{node_id}-kafka", f"{node_id}-kafka-crypto"):
            p_cs = cfg_parser.get_config_dict_for_groups(f"{section_name_stem}-producer")
            c_cs = cfg_parser.get_config_dict_for_groups(f"{section_name_stem}-consumer")
            kcp_cfgs.update(p_cs)
            kcc_cfgs.update(c_cs)
        kcp_cfgs.update(broker_configs.copy())
        kcc_cfgs.update(broker_configs.copy())
        # figure out a consumer group ID to use (KafkaCrypto Consumers need one)
        if "group.id" not in kcc_cfgs:
            kcc_cfgs["group.id"] = str(uuid.uuid1())
        # pop the node_ID
        if "node_id" in kcp_cfgs:
            kcp_cfgs.pop("node_id")
        if "node_id" in kcc_cfgs:
            kcc_cfgs.pop("node_id")
        return kcp_cfgs, kcc_cfgs
