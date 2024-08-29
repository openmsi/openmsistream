" ControlledProcess classes that periodically also produce messages to heartbeat topics "

# imports
import datetime
from abc import ABC
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer
from openmsitoolbox.argument_parsing.has_arguments import HasArguments
from openmsitoolbox.controlled_process.controlled_process import ControlledProcess
from openmsitoolbox import ControlledProcessSingleThread, ControlledProcessMultiThreaded
from ..kafka_wrapper.config_file_parser import KafkaConfigFileParser
from ..kafka_wrapper import OpenMSIStreamProducer
from .heartbeat_producibles import HeartbeatProducible


class ControlledProcessHeartbeats(ControlledProcess, HasArguments, ABC):
    "A long-running process that occasionally produces messages to a heartbeat topic"

    def __init__(
        self,
        config_path,
        *args,
        heartbeat_topic_name=None,
        heartbeat_program_id=None,
        heartbeat_interval_secs=None,
        **kwargs,
    ):
        try:
            super().__init__(config_path, *args, **kwargs)
        except TypeError:
            super().__init__(*args, **kwargs)
        self.__heartbeat_topic_name = heartbeat_topic_name
        self._heartbeat_program_id = heartbeat_program_id
        self.__heartbeat_interval_secs = heartbeat_interval_secs
        self.__heartbeat_producer = None
        self.__last_heartbeat = datetime.datetime.now()
        # just return if heartbeats won't be produced
        if self.__heartbeat_topic_name is None:
            return
        cfp = KafkaConfigFileParser(config_path, logger=self.logger)
        if "heartbeat" in cfp.available_group_names:
            heartbeat_config_dict = cfp.heartbeat_configs
        else:
            self.logger.warning(
                (
                    f"WARNING: config file at {config_path} has no 'heartbeat' section but "
                    "a heartbeat topic name was given."
                ),
            )
            heartbeat_config_dict = {}
        if "key.serializer" not in heartbeat_config_dict:
            heartbeat_config_dict["key.serializer"] = StringSerializer()
        if "value.serializer" not in heartbeat_config_dict:
            heartbeat_config_dict["value.serializer"] = StringSerializer()
        all_heartbeat_producer_configs = {}
        if "bootstrap.servers" not in heartbeat_config_dict:
            all_heartbeat_producer_configs.update(cfp.broker_configs)
        all_heartbeat_producer_configs.update(heartbeat_config_dict)
        self.__heartbeat_producer = OpenMSIStreamProducer(
            SerializingProducer, all_heartbeat_producer_configs, logger=self.logger
        )
        self.logger.info(
            (
                "Heartbeat messages will be produced to the "
                f"'{self.__heartbeat_topic_name}' topic every "
                f"{self.__heartbeat_interval_secs} seconds"
            )
        )
        if self._heartbeat_program_id is None:
            self._heartbeat_program_id = self.__heartbeat_producer.producer_id

    def get_heartbeat_message(self):
        """Return the HeartbeatProducible-type object that should be produced to the
        heartbeat topic
        """
        return HeartbeatProducible(self._heartbeat_program_id)

    def _print_still_alive(self):
        """Print the "still alive" character to the console like a regular
        ControlledProcess, but also produce messages to the heartbeat topic if applicable
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

    def _on_shutdown(self):
        try:
            super()._on_shutdown()
        except NotImplementedError:
            pass
        if self.__heartbeat_producer is not None:
            # send a final heartbeat
            self.__heartbeat_producer.produce_object(
                self.get_heartbeat_message(), self.__heartbeat_topic_name
            )
            self.logger.info("Flushing heartbeat producer")
            self.__heartbeat_producer.flush()
            self.__heartbeat_producer.close()

    @classmethod
    def get_command_line_arguments(cls):
        superargs, superkwargs = super().get_command_line_arguments()
        args = [
            *superargs,
            "heartbeat_topic_name",
            "heartbeat_program_id",
            "heartbeat_interval_secs",
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
        }
        return superargs, kwargs


class ControlledProcessSingleThreadHeartbeats(
    ControlledProcessHeartbeats, ControlledProcessSingleThread
):
    """A long-running process with a single worker thread that occasionally produces
    messages to a heartbeat topic
    """


class ControlledProcessMultiThreadedHeartbeats(
    ControlledProcessHeartbeats, ControlledProcessMultiThreaded
):
    """A long-running process with multiple worker threads that occasionally produces
    messages to a heartbeat topic
    """
