" ControlledProcess classes that periodically also produce messages to heartbeat topics "

# imports
from abc import ABC
import datetime
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer
from openmsitoolbox.controlled_process.controlled_process import ControlledProcess
from openmsitoolbox import ControlledProcessSingleThread, ControlledProcessMultiThreaded
from ..kafka_wrapper.config_file_parser import KafkaConfigFileParser
from ..kafka_wrapper import OpenMSIStreamProducer, Producible


class DefaultHeartbeatProducible(Producible):

    def __init__(self, producer_id):
        self.__producer_id = producer_id

    @property
    def msg_key(self):
        return f"{self.__producer_id}_heartbeat"

    @property
    def msg_value(self):
        value_dict = {"timestamp": str(datetime.datetime.now())}
        return f"{value_dict}"


class ControlledProcessHeartbeats(ControlledProcess, ABC):
    "A long-running process that occasionally produces messages to a heartbeat topic"

    def __init__(self, config_path, *args, **kwargs):
        super().__init__(config_path, *args, **kwargs)
        self.__heartbeat_topic_name = None
        self.__heartbeat_interval_secs = None
        self.__heartbeat_producer = None
        self.__last_heartbeat = datetime.datetime.now()
        cfp = KafkaConfigFileParser(config_path, logger=self.logger)
        if "heartbeat" not in cfp.available_group_names:
            self.logger.info(
                (
                    f"config file at {config_path} has no 'heartbeat' section. "
                    "no heartbeat messages will be produced."
                )
            )
            return
        heartbeat_config_dict = cfp.heartbeat_configs
        if "topic.name" not in heartbeat_config_dict:
            self.logger.error(
                (
                    f"ERROR: config file at {config_path} has a 'heartbeat' section "
                    "but no 'topic.name' parameter!"
                ),
                exc_type=RuntimeError,
                reraise=True,
            )
        self.__heartbeat_topic_name = heartbeat_config_dict.pop("topic.name")
        self.__heartbeat_interval_secs = (
            heartbeat_config_dict.pop("interval.seconds")
            if "interval.seconds" in heartbeat_config_dict
            else self._ControlledProcess__update_secs # pylint: disable=no-member
        )
        if "key.serializer" not in heartbeat_config_dict:
            heartbeat_config_dict["key.serializer"] = StringSerializer()
        if "value.serializer" not in heartbeat_config_dict:
            heartbeat_config_dict["value.serializer"] = StringSerializer()
        all_configs = {}
        if "bootstrap.servers" not in heartbeat_config_dict:
            all_configs.update(cfp.broker_configs)
        all_configs.update(heartbeat_config_dict)
        self.__heartbeat_producer = OpenMSIStreamProducer(
            SerializingProducer, all_configs, logger=self.logger
        )
        self.logger.info(
            (
                "Heartbeat messages will be produced to the "
                f"'{self.__heartbeat_topic_name}' topic every "
                f"{self.__heartbeat_interval_secs} seconds"
            )
        )

    def get_heartbeat_message(self):
        return DefaultHeartbeatProducible(self.__heartbeat_producer.producer_id)

    def _print_still_alive(self):
        """Print the "still alive" character to the console like a regular
        ControlledProcess, but also produce messages to the heartbeat topic if applicable
        """
        super()._print_still_alive()
        if (
            self.__heartbeat_interval_secs is not None
            and (datetime.datetime.now() - self.__last_heartbeat).total_seconds()
            > self.__heartbeat_interval_secs
        ):
            self.__heartbeat_producer.produce_object(
                self.get_heartbeat_message(), self.__heartbeat_topic_name
            )
            self.__last_heartbeat = datetime.datetime.now()

    def _on_shutdown(self):
        super()._on_shutdown()
        self.logger.info("Flushing heartbeat producer")
        self.__heartbeat_producer.flush()
        self.__heartbeat_producer.close()

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
