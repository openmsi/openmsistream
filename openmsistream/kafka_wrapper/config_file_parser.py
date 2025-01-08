"""
A wrapper around ConfigFileParser to handle OpenMSIStream config files that have sections
for connecting to brokers and running Producers and Consumers
"""

# imports
import pathlib
from confluent_kafka.serialization import (
    DoubleSerializer,
    IntegerSerializer,
    StringSerializer,
)
from confluent_kafka.serialization import (
    DoubleDeserializer,
    IntegerDeserializer,
    StringDeserializer,
)
from ..utilities.config_file_parser import ConfigFileParser
from ..utilities.config import RUN_CONST
from .serialization import DataFileChunkSerializer, DataFileChunkDeserializer


class KafkaConfigFileParser(ConfigFileParser):
    """
    A ConfigFileParser to parse Kafka configurations from files
    """

    #################### CONSTANTS ####################

    SERIALIZERS = [
        StringSerializer,
        IntegerSerializer,
        DoubleSerializer,
        DataFileChunkSerializer,
    ]

    DESERIALIZERS = [
        StringDeserializer,
        IntegerDeserializer,
        DoubleDeserializer,
        DataFileChunkDeserializer,
    ]

    #################### PROPERTIES ####################

    @property
    def broker_configs(self):
        """
        Configs for connecting to the broker
        """
        if self.__broker_configs is None:
            self.__broker_configs = self._get_config_dict("broker")
        return self.__convert_floats(self.__broker_configs)

    @property
    def heartbeat_configs(self):
        """
        Configs for the producer to the heartbeat topic
        """
        if self.__heartbeat_configs is None:
            self.__heartbeat_configs = self._get_config_dict("heartbeat")
        return self.__convert_floats(self.__heartbeat_configs)

    @property
    def log_configs(self):
        """
        Configs for the producer to the log topic
        """
        if self.__log_configs is None:
            self.__log_configs = self._get_config_dict("log")
        return self.__convert_floats(self.__log_configs)

    @property
    def producer_configs(self):
        """
        Configs for setting up Producers
        """
        if self.__producer_configs is None:
            pcs = self._get_config_dict("producer")
            self.__producer_configs = self.get_replaced_configs(pcs, "serialization")
        return self.__convert_floats(self.__producer_configs)

    @property
    def consumer_configs(self):
        """
        Configs for setting up Consumers
        """
        if self.__consumer_configs is None:
            ccs = self._get_config_dict("consumer")
            # if the auto.offset.reset was given as "none" then remove it from the ccs
            if "auto.offset.reset" in ccs and ccs["auto.offset.reset"] == "none":
                del ccs["auto.offset.reset"]
            self.__consumer_configs = self.get_replaced_configs(ccs, "deserialization")
        return self.__convert_floats(self.__consumer_configs)

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__broker_configs = None
        self.__heartbeat_configs = None
        self.__log_configs = None
        self.__producer_configs = None
        self.__consumer_configs = None
        self.kc_config_file_str = self.__get_kc_config_file_str()

    @staticmethod
    def get_replaced_configs(configs, replacement_type):
        """
        Returns a configuration dictionary with (de)serialization parameters replaced
        by instances of corresponding classes

        configs = the configurations dictionary to alter and return
        replacement_type = a string indicating the type of replacement that should be performed
        """
        if replacement_type == "serialization":
            classes = KafkaConfigFileParser.SERIALIZERS
        elif replacement_type == "deserialization":
            classes = KafkaConfigFileParser.DESERIALIZERS
        else:
            raise ValueError(f'ERROR: unrecognized replacement_type "{replacement_type}"')
        for cfg_name, cfg_value in configs.items():
            for serdes_class in classes:
                if cfg_value == serdes_class.__name__:
                    configs[cfg_name] = serdes_class()
                    break
        return configs

    #################### PRIVATE HELPER FUNCTIONS ####################

    def __get_kc_config_file_str(self):
        """
        Returns the path to the config file that KafkaCrypto needs based on the current configs.
        Return value is a string as expected by KafkaCrypto.
        If no config file is found according to the conventions listed below, this function
        returns None and it will be assumed that no configuration for KafkaCrypto exists

        Options are:
        1) The regular config file has a "kafkacrypto" section with a "config_file" parameter
        that is the path to the KafkaCrypo config file
        2) The regular config file has a "kafkacrypto" section with a "node_id" parameter
        corresponding to a named subdirectory that was created when the node was provisioned.
        The subdirectory must be located in:
        a) openmsistream/kafka_wrapper/config_files,
        b) the same directory as the config file, or
        c) the current directory
        """
        if "kafkacrypto" in self.available_group_names:
            kc_configs = self.get_config_dict_for_groups("kafkacrypto")
            # option 1 above
            if "config_file" in kc_configs:
                path_as_str = (kc_configs["config_file"]).lstrip("file#")
                if not pathlib.Path(path_as_str).is_file():
                    errmsg = (
                        f"ERROR: KafkaCrypto config file {path_as_str} (from config file "
                        f"{self.filepath}) not found!"
                    )
                    self.logger.error(errmsg, exc_type=FileNotFoundError)
                return path_as_str
            # option 2 above
            if "node_id" in kc_configs:
                node_id = kc_configs["node_id"]
                dirpaths = [
                    RUN_CONST.CONFIG_FILE_DIR / node_id,
                    self.filepath.parent / node_id,
                    pathlib.Path(".").resolve() / node_id,
                ]
                filepaths = [dpath / f"{node_id}.config" for dpath in dirpaths]
                for filepath in filepaths:
                    if filepath.is_file():
                        return str(filepath)
                errmsg = (
                    f"ERROR: no KafkaCrypto config file found for node ID = {node_id}"
                    f"(expected one of {filepaths})"
                )
                self.logger.error(errmsg, exc_type=FileNotFoundError)
        # no config file found
        return None

    def __convert_floats(self, given_dict):
        """
        Return a version of the dictionary "given_dict" where any values that can be
        converted to floats are converted to floats
        (Some KafkaCrypto code assumes configs will be floats)
        """
        return_dict = {}
        for k, v in given_dict.items():
            try:
                v_as_float = float(v)
                return_dict[k] = v_as_float
            except Exception:
                return_dict[k] = v
        return return_dict
