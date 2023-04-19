# imports
from queue import Queue
from watchdog.events import RegexMatchingEventHandler
from ...utilities import LogOwner

class UploadDirectoryEventHandler(LogOwner,RegexMatchingEventHandler) :
    """
    A watchdog EventHandler subclass to collect watchdog events and pass them back
    to a :class:`~DataFileUploadDirectory` for taking actions
    """

    def __init__(self,*args,**kwargs) :
        """
        Constructor method (same arguments as watchdog base class)
        """
        super().__init__(*args,**kwargs)
        # a queue to hold actions for the DataFileUploadDirectory to take
        self.__action_queue = Queue()