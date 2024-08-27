" Various Producibles for main program/base class heartbeat messages "

# imports
import json
from ...utilities.heartbeat_producible import HeartbeatProducible

class UploadDirectoryHeartbeatProducible(HeartbeatProducible):

    def __init__(self, program_id, n_msgs, n_bytes):
        super().__init__(program_id)
        self.__n_msgs = n_msgs
        self.__n_bytes = n_bytes

    @property
    def msg_value(self):
        value_dict = json.loads(super().msg_value)
        value_dict["n_messages"] = self.__n_msgs
        value_dict["n_bytes"] = self.__n_bytes
        return f"{value_dict}"