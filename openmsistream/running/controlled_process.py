#imports
import time
from queue import Queue, Empty
from threading import Thread
from abc import ABC, abstractmethod
from ..utilities.misc import add_user_input
from ..utilities.logging import LogOwner
from .config import RUN_CONST

class ControlledProcess(LogOwner,ABC) :
    """
    A class to use when running processes that should remain active until they are explicitly shut down
    """

    #################### PROPERTIES ####################

    @property
    def alive(self) :
        return self.__alive
    @property
    def control_command_queue(self) :
        return self.__control_command_queue

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,*args,update_secs=RUN_CONST.DEFAULT_UPDATE_SECONDS,**other_kwargs) :
        """
        update_secs = number of seconds to wait between printing a progress character to the console 
                      to indicate the program is alive
        """
        self.__update_secs = update_secs
        #start up a Queue that will hold the control commands
        self.__control_command_queue = Queue()
        #use a daemon thread to allow a user to input control commands from the command line 
        #while the process is running
        user_input_thread = Thread(target=add_user_input,args=(self.__control_command_queue,))
        user_input_thread.daemon=True
        user_input_thread.start()
        #a variable to indicate if the process has been shut down yet
        self.__alive = False
        super().__init__(*args,**other_kwargs)

    def shutdown(self) :
        """
        Stop the process running
        """
        self.__control_command_queue.task_done()
        self.__alive = False
        self._on_shutdown()

    #################### PRIVATE HELPER FUNCTIONS ####################

    def _print_still_alive(self) :
        #print the "still alive" character
        if self.__update_secs!=-1 and time.time()-self.__last_update>self.__update_secs:
            self.logger.debug('.')
            self.__last_update = time.time()

    def _check_control_command_queue(self) :
        #if anything exists in the control command queue
        try :
            cmd = self.__control_command_queue.get(block=True,timeout=0.5)
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
        before anything else to set these variables
        """
        self.__alive = True
        self.__last_update = time.time()

    @abstractmethod
    def _on_check(self) :
        """
        This function is run when the "check" command is found in the control queue
        Not implemented in the base class
        """
        pass

    @abstractmethod
    def _on_shutdown(self) :
        """
        This function is run when the process is stopped; it's called from shutdown
        Not implemented in the base class
        """
        pass
