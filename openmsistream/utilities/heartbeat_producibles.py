" Various Producibles for main program/base class heartbeat messages "

# imports
import datetime, json
from ..kafka_wrapper import Producible


class HeartbeatProducible(Producible):
    """A Producible for heartbeat messages. The key is a program ID and the value
    is a timestamp
    """

    def __init__(self, program_id):
        self.__program_id = program_id

    @property
    def msg_key(self):
        return f"{self.__program_id}_heartbeat"

    @property
    def msg_value(self):
        value_dict = {"timestamp": str(datetime.datetime.now())}
        return json.dumps(value_dict)


class UploadDirectoryHeartbeatProducible(HeartbeatProducible):
    "The heartbeat Producible sent by DataFileUploadDirectory programs."

    def __init__(self, program_id, n_msgs, n_bytes):
        super().__init__(program_id)
        self.__n_msgs = n_msgs
        self.__n_bytes = n_bytes

    @property
    def msg_value(self):
        value_dict = json.loads(super().msg_value)
        value_dict["n_messages_produced"] = self.__n_msgs
        value_dict["n_bytes_produced"] = self.__n_bytes
        return json.dumps(value_dict)


class MessageProcessorHeartbeatProducible(HeartbeatProducible):
    """The heartbeat Producible sent by any programs that extend the
    ControlledMessageProcessor class
    """

    def __init__(
        self, program_id, n_msgs_read, n_msgs_processed, n_bytes_read, n_bytes_processed
    ):
        super().__init__(program_id)
        self.__n_msgs_read = n_msgs_read
        self.__n_msgs_processed = n_msgs_processed
        self.__n_bytes_read = n_bytes_read
        self.__n_bytes_processed = n_bytes_processed

    @property
    def msg_value(self):
        value_dict = json.loads(super().msg_value)
        value_dict["n_messages_read"] = self.__n_msgs_read
        value_dict["n_messages_processed"] = self.__n_msgs_processed
        value_dict["n_bytes_read"] = self.__n_bytes_read
        value_dict["n_bytes_processed"] = self.__n_bytes_processed
        return json.dumps(value_dict)


class MessageReproducerHeartbeatProducible(HeartbeatProducible):
    """The heartbeat Producible sent by any programs that extend the
    ControlledMessageReproducer class
    """

    def __init__(
        self,
        program_id,
        n_msgs_read,
        n_msgs_processed,
        n_msgs_produced,
        n_bytes_read,
        n_bytes_processed,
        n_bytes_produced,
    ):
        super().__init__(program_id)
        self.__n_msgs_read = n_msgs_read
        self.__n_msgs_processed = n_msgs_processed
        self.__n_msgs_produced = n_msgs_produced
        self.__n_bytes_read = n_bytes_read
        self.__n_bytes_processed = n_bytes_processed
        self.__n_bytes_produced = n_bytes_produced

    @property
    def msg_value(self):
        value_dict = json.loads(super().msg_value)
        value_dict["n_messages_read"] = self.__n_msgs_read
        value_dict["n_messages_processed"] = self.__n_msgs_processed
        value_dict["n_messages_produced"] = self.__n_msgs_produced
        value_dict["n_bytes_read"] = self.__n_bytes_read
        value_dict["n_bytes_processed"] = self.__n_bytes_processed
        value_dict["n_bytes_produced"] = self.__n_bytes_produced
        return json.dumps(value_dict)
