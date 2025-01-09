"""ControlledProcess classes that periodically also produce messages to heartbeat topics
and/or self-produces own logs
"""

# imports
import datetime, inspect
from abc import ABC
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer
from openmsitoolbox.argument_parsing.has_arguments import HasArguments
from openmsitoolbox.controlled_process.controlled_process import ControlledProcess
from openmsitoolbox import ControlledProcessSingleThread, ControlledProcessMultiThreaded
from ..kafka_wrapper.config_file_parser import KafkaConfigFileParser
from ..kafka_wrapper import OpenMSIStreamProducer
from .heartbeat_producibles import HeartbeatProducible
from .log_producibles import LogProducible
from .log_handler import LoggingHandler


class ControlledProcessHeartbeatsLogs(ControlledProcess, HasArguments, ABC):
    """A long-running process that occasionally produces messages
    to a heartbeat topic and/or self-produces own logs
    """

    def __init__(
        self,
        config_path,
        *args,
        heartbeat_topic_name=None,
        heartbeat_program_id=None,
        heartbeat_interval_secs=None,
        log_topic_name=None,
        log_program_id=None,
        log_interval_secs=None,
        **kwargs,
    ):
        try:
            super().__init__(config_path, *args, **kwargs)
        except TypeError:
            super().__init__(*args, **kwargs)
        self.__heartbeat_topic_name = heartbeat_topic_name
        self.__default_heartbeat_program_id = heartbeat_program_id
        self._heartbeat_program_id = None
        self.__heartbeat_interval_secs = heartbeat_interval_secs
        self.__heartbeat_producer = None
        self.__heartbeat_producer_owned = False
        self.__last_heartbeat = datetime.datetime.fromtimestamp(0)
        self.__log_topic_name = log_topic_name
        self.__default_log_program_id = log_program_id
        self._log_program_id = None
        self.__log_interval_secs = log_interval_secs
        self.__log_producer = None
        self.__log_producer_owned = False
        self.__last_log = datetime.datetime.fromtimestamp(0)
        # Read config file for later use
        self.__config_path = config_path
        cfp = KafkaConfigFileParser(config_path, logger=self.logger)
        if "heartbeat" in cfp.available_group_names:
            self.__heartbeat_config_dict = cfp.heartbeat_configs
        else:
            self.__heartbeat_config_dict = {}
        if "log" in cfp.available_group_names:
            self.__log_config_dict = cfp.log_configs
        else:
            self.__log_config_dict = {}
        self.__broker_configs = cfp.broker_configs

    def set_heartbeat_producer(self, producer, close_it=False):
        "Set producer instance for generating heartbeats"
        if self.__heartbeat_topic_name is None:
            # Nothing to do
            producer = None
        # Get configuration preference
        prefer_separate_producer = False
        if (
            "prefer.separate.producer" in self.__heartbeat_config_dict
            and self.__heartbeat_config_dict["prefer.separate.producer"]
        ):
            prefer_separate_producer = True
        if producer == "Separate":
            prefer_separate_producer = True
        # Order of priority:
        # 1. If producer is None, turn off heartbeats.
        # 2. If prefer_separate_producer is False, and producer is a valid
        #    producer object, use that.
        # 3. Otherwise, use our own producer.
        if producer is None:
            self.__set_heartbeat_producer_internal(None, False)
        elif (
            not prefer_separate_producer
            and hasattr(producer, "produce_object")
            and inspect.isroutine(producer.produce_object)
            and hasattr(producer, "producer_id")
        ):
            self.__set_heartbeat_producer_internal(producer, close_it)
        else:
            own_producer = self.__get_standalone_heartbeat_producer()
            self.__set_heartbeat_producer_internal(own_producer, True)

    def __get_standalone_heartbeat_producer(self):
        "Get a standalone producer for heartbeats"
        if len(self.__heartbeat_config_dict) < 1:
            self.logger.warning(
                (
                    f"WARNING: config file at {self.__config_path} has no 'heartbeat' section but "
                    "a heartbeat topic name was given and a standalone producer is in use."
                ),
            )
        heartbeat_config_dict = self.__heartbeat_config_dict.copy()
        # Remove non-broker parameters
        if "prefer.separate.producer" in heartbeat_config_dict:
            del heartbeat_config_dict["prefer.separate.producer"]
        # Add broker parameters
        if "key.serializer" not in heartbeat_config_dict:
            heartbeat_config_dict["key.serializer"] = StringSerializer()
        if "value.serializer" not in heartbeat_config_dict:
            heartbeat_config_dict["value.serializer"] = StringSerializer()
        all_heartbeat_producer_configs = {}
        if "bootstrap.servers" not in heartbeat_config_dict:
            all_heartbeat_producer_configs.update(self.__broker_configs)
        all_heartbeat_producer_configs.update(heartbeat_config_dict)
        return OpenMSIStreamProducer(
            SerializingProducer, all_heartbeat_producer_configs, logger=self.logger
        )

    def __set_heartbeat_producer_internal(self, producer, owned):
        "Set/update current heartbeat producer, cleaning up old as required"
        if (
            self.__heartbeat_producer == producer
            and self.__heartbeat_producer_owned == owned
        ):
            # no change
            return
        # Cleanup old one if present
        if self.__heartbeat_producer is not None:
            # send a final heartbeat
            self.__heartbeat_producer.produce_object(
                self.get_heartbeat_message(), self.__heartbeat_topic_name
            )
            self.logger.info("Cleaning up heartbeat producer")
            if (
                self.__heartbeat_producer == self.__log_producer
                and self.__heartbeat_producer_owned
            ):
                self.__log_producer_owned = True  # Hand off ownership
                self.__heartbeat_producer_owned = False
            if self.__heartbeat_producer_owned:
                self.__heartbeat_producer.flush()
                self.__heartbeat_producer.close()
            self.__heartbeat_producer = None
            self.__heartbeat_producer_owned = None
        # Set new one
        self.__heartbeat_producer = producer
        self.__heartbeat_producer_owned = owned
        if self.__heartbeat_producer is not None:
            self.logger.info(
                (
                    "Heartbeat messages will be produced to the "
                    f"'{self.__heartbeat_topic_name}' topic every "
                    f"{self.__heartbeat_interval_secs} seconds"
                )
            )
            self._heartbeat_program_id = self.__default_heartbeat_program_id
            if self._heartbeat_program_id is None:
                self._heartbeat_program_id = self.__heartbeat_producer.producer_id

    def set_log_producer(self, producer, close_it=True):
        "Set producer instance for generating logs"
        if self.__log_topic_name is None:
            # Nothing to do
            producer = None
        # Get configuration preference
        prefer_separate_producer = False
        if (
            "prefer.separate.producer" in self.__log_config_dict
            and self.__log_config_dict["prefer.separate.producer"]
        ):
            prefer_separate_producer = True
        if producer == "Separate":
            prefer_separate_producer = True
        # Order of priority:
        # 1. If producer is None, turn off logs.
        # 2. If prefer_separate_producer is False, and producer is a valid
        #    producer object, use that.
        # 3. Otherwise, use our own producer.
        if producer is None:
            self.__set_log_producer_internal(None, False)
        elif (
            not prefer_separate_producer
            and hasattr(producer, "produce_object")
            and inspect.isroutine(producer.produce_object)
            and hasattr(producer, "producer_id")
        ):
            self.__set_log_producer_internal(producer, close_it)
        else:
            own_producer = self.__get_standalone_log_producer()
            self.__set_log_producer_internal(own_producer, True)

    def __get_standalone_log_producer(self):
        "Get a standalone producer for logs"
        if len(self.__log_config_dict) < 1:
            self.logger.warning(
                (
                    f"WARNING: config file at {self.__config_path} has no 'log' section but "
                    "a log topic name was given and a standalone producer is in use."
                ),
            )
        log_config_dict = self.__log_config_dict.copy()
        # Remove non-broker parameters
        if "prefer.separate.producer" in log_config_dict:
            del log_config_dict["prefer.separate.producer"]
        if "max.logs.per.message" in log_config_dict:
            del log_config_dict["max.logs.per.message"]
        if "log.level" in log_config_dict:
            del log_config_dict["log.level"]
        # Add broker parameters
        if "key.serializer" not in log_config_dict:
            log_config_dict["key.serializer"] = StringSerializer()
        if "value.serializer" not in log_config_dict:
            log_config_dict["value.serializer"] = StringSerializer()
        all_log_producer_configs = {}
        if "bootstrap.servers" not in log_config_dict:
            all_log_producer_configs.update(self.__broker_configs)
        all_log_producer_configs.update(log_config_dict)
        return OpenMSIStreamProducer(
            SerializingProducer, all_log_producer_configs, logger=self.logger
        )

    def __set_log_producer_internal(self, producer, owned):
        "Set/update current log producer, cleaning up old as required"
        if self.__log_producer == producer and self.__log_producer_owned == owned:
            # no change
            return
        # Cleanup old one if present
        if self.__log_producer is not None:
            # send a final log
            self.__log_producer.produce_object(
                self.get_log_message(), self.__log_topic_name
            )
            self.logger.info("Cleaning up log producer")
            if (
                self.__heartbeat_producer == self.__log_producer
                and self.__log_producer_owned
            ):
                self.__heartbeat_producer_owned = True  # Hand off ownership
                self.__log_producer_owned = False
            if self.__log_producer_owned:
                self.__log_producer.flush()
                self.__log_producer.close()
            self.__log_producer = None
            self.__log_producer_owned = None
        # Set new one
        self.__log_producer = producer
        self.__log_producer_owned = owned
        if self.__log_producer is not None:
            self.logger.info(
                (
                    "log messages will be produced to the "
                    f"'{self.__log_topic_name}' topic every "
                    f"{self.__log_interval_secs} seconds"
                )
            )
            self._log_program_id = self.__default_log_program_id
            if self._log_program_id is None:
                self._log_program_id = self.__log_producer.producer_id
            max_messages = 65536
            if "max.logs.per.message" in self.__log_config_dict:
                max_messages = int(self.__log_config_dict["max.logs.per.message"])
            LoggingHandler.set_max_messages(max_messages)
            if "log.level" in self.__log_config_dict:
                LoggingHandler.setLevel(self.__log_config_dict["log.level"])

    def get_heartbeat_message(self):
        """Return the HeartbeatProducible-type object that should be produced to the
        heartbeat topic
        """
        return HeartbeatProducible(self._heartbeat_program_id)

    def get_log_message(self):
        """Return the LogProducible-type object that should be produced to the
        log topic
        """
        return LogProducible(self._log_program_id)

    def _print_still_alive(self):
        """Print the "still alive" character to the console like a regular
        ControlledProcess, but also produce messages to the heartbeat and/or
        log topics if applicable
        """
        super()._print_still_alive()
        if (
            self.__heartbeat_producer is not None
            and (datetime.datetime.now() - self.__last_heartbeat).total_seconds()
            > self.__heartbeat_interval_secs
        ):
            self.__heartbeat_producer.produce_object(
                self.get_heartbeat_message(), self.__heartbeat_topic_name
            )
            self.__last_heartbeat = datetime.datetime.now()
        if (
            self.__log_producer is not None
            and (datetime.datetime.now() - self.__last_log).total_seconds()
            > self.__log_interval_secs
        ):
            self.__log_producer.produce_object(
                self.get_log_message(), self.__log_topic_name
            )
            self.__last_log = datetime.datetime.now()

    def _on_shutdown(self):
        try:
            super()._on_shutdown()
        except NotImplementedError:
            pass
        # Cleanup
        self.__set_heartbeat_producer_internal(None, False)
        self.__set_log_producer_internal(None, False)

    @classmethod
    def get_command_line_arguments(cls):
        superargs, superkwargs = super().get_command_line_arguments()
        args = [
            *superargs,
            "heartbeat_topic_name",
            "heartbeat_program_id",
            "heartbeat_interval_secs",
            "log_topic_name",
            "log_program_id",
            "log_interval_secs",
        ]
        return args, superkwargs

    @classmethod
    def get_init_args_kwargs(cls, parsed_args):
        superargs, superkwargs = super().get_init_args_kwargs(parsed_args)
        kwargs = {
            **superkwargs,
            "heartbeat_topic_name": parsed_args.heartbeat_topic_name,
            "heartbeat_program_id": parsed_args.heartbeat_program_id,
            "heartbeat_interval_secs": parsed_args.heartbeat_interval_secs,
            "log_topic_name": parsed_args.log_topic_name,
            "log_program_id": parsed_args.log_program_id,
            "log_interval_secs": parsed_args.log_interval_secs,
        }
        return superargs, kwargs


class ControlledProcessSingleThreadHeartbeatsLogs(
    ControlledProcessHeartbeatsLogs, ControlledProcessSingleThread
):
    """A long-running process with a single worker thread that occasionally produces
    messages to a heartbeat and/or log topic
    """


class ControlledProcessMultiThreadedHeartbeatsLogs(
    ControlledProcessHeartbeatsLogs, ControlledProcessMultiThreaded
):
    """A long-running process with multiple worker threads that occasionally produces
    messages to a heartbeat and/or log topic
    """
