" The basic/default Producible used for heartbeat messages "

# imports
import datetime, json
from ..kafka_wrapper import Producible


class HeartbeatProducible(Producible):
    """ A Producible for heartbeat messages. The key is a program ID and the value
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
