"""
A process that will run while waiting for user input to check progress/status or shut it down
"""

#imports
import datetime
from queue import Queue, Empty
from threading import Thread
from abc import ABC, abstractmethod
from ..utilities.misc import add_user_input
from ..utilities.logging import LogOwner
from .config import RUN_CONST

class ControlledProcess(LogOwner,ABC) :
    """
    A class to use when running processes that should remain active until they are explicitly shut down

    :param update_secs: number of seconds to wait between printing a progress character to the console
        to indicate the program is alive
    :type update_secs: int, optional
    """

    #################### PROPERTIES ####################

    @property
    def alive(self) :
        """
        Read-only boolean indicating if the process is running
        """
        return self.__alive

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,*args,update_secs=RUN_CONST.DEFAULT_UPDATE_SECONDS,**other_kwargs) :
        self.__update_secs = update_secs
        #start up a Queue that will hold the control commands
        self.control_command_queue = Queue()
        #use a daemon thread to allow a user to input control commands from the command line
        #while the process is running
        user_input_thread = Thread(target=add_user_input,args=(self.control_command_queue,))
        user_input_thread.daemon=True
        user_input_thread.start()
        #a variable to indicate if the process has been shut down yet
        self.__alive = False
        #the last time the "still alive" character was printed
        self.__last_update = datetime.datetime.now()
        super().__init__(*args,**other_kwargs)

    def shutdown(self) :
        """
        Stop the process running.
        """
        self.control_command_queue.task_done()
        self.__alive = False
        self._on_shutdown()

    #################### PRIVATE HELPER FUNCTIONS ####################

    def _print_still_alive(self) :
        #print the "still alive" character
        if self.__update_secs!=-1 and (datetime.datetime.now()-self.__last_update).total_seconds()>self.__update_secs:
            self.logger.debug('.')
            self.__last_update = datetime.datetime.now()

    def _check_control_command_queue(self) :
        #if anything exists in the control command queue
        try :
            cmd = self.control_command_queue.get(block=True,timeout=0.5)
        except Empty :
            cmd = None
        if cmd is not None :
            if cmd.lower() in ('q','quit') : # shut down the process
                self.shutdown()
            elif cmd.lower() in ('c','check') : # run the on_check function
                self._on_check()
            else : # otherwise just skip this unrecognized command
                self._check_control_command_queue()

    #################### ABSTRACT METHODS ####################

    @abstractmethod
    def run(self) :
        """
        Classes extending this base class should include the logic of actually
        running the controlled process in this function, and should call super().run()
        before anything else to set some internal variables
        """
        self.__alive = True
        self.__last_update = datetime.datetime.now()

    @abstractmethod
    def _on_check(self) :
        """
        This function is run when the "check" command is found in the control queue.

        Not implemented in the base class
        """
        raise NotImplementedError

    @abstractmethod
    def _on_shutdown(self) :
        """
        This function is run when the process is stopped; it's called from :func:`~shutdown`.

        Not implemented in the base class
        """
        raise NotImplementedError
