"""Small utility class to simplify producing messages with standardized callbacks"""

#imports
from abc import ABC, abstractmethod

class Producible(ABC) :
    """
    Small utility class for anything that can be Produced as a message to a topic
    """

    @property
    @abstractmethod
    def msg_key(self) :
        """
        The key of the object when it's represented as a message.
        This can be something that needs to be serialized still

        Not implemented in base class
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def msg_value(self) :
        """
        The value of the object when it's represented as a message.
        This can be something that needs to be serialized still

        Not implemented in base class
        """
        raise NotImplementedError

    @property
    def callback_kwargs(self) :
        """
        A dictionary of keyword arguments that should be sent to the callback function
        for Producers producing messages of this Producible

        Empty for the base class
        """
        return {}

    @abstractmethod
    def get_log_msg(self,print_every=None) :
        """
        Given some (optional) "print_every" variable, return the string that should be logged for this message.
        If "None" is returned (the default) nothing will be logged.
        """
        return None
