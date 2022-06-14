#imports
import time
from queue import Queue, Empty
from threading import Thread, Lock
from abc import ABC, abstractmethod
from ..utilities.misc import add_user_input
from .config import UTIL_CONST
from .logging import LogOwner
from .my_thread import MyThread

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

    def __init__(self,*args,update_secs=UTIL_CONST.DEFAULT_UPDATE_SECONDS,**other_kwargs) :
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

class ControlledProcessSingleThread(ControlledProcess,ABC) :
    """
    A class for running a process in a single thread in a loop until it's explicitly shut down
    """

    def run(self) :
        """
        Start the process and call run_iteration until the process is shut down
        """
        super().run()
        while self.alive :
            self._run_iteration()
            self._print_still_alive()
            self._check_control_command_queue()

    @abstractmethod
    def _run_iteration(self) :
        """
        The function that is run in an infinite loop while the process is alive
        Not implemented in the base class.
        """
        pass

class ControlledProcessMultiThreaded(ControlledProcess,ABC) :
    """
    A class for running a group of processes in multiple threads until they're explicitly shut down
    """

    @property
    def n_threads(self):
        return self.__n_threads

    def __init__(self,*args,n_threads=UTIL_CONST.DEFAULT_N_THREADS,**kwargs) :
        """
        n_threads = number of threads to use
        """
        self.__n_threads = n_threads
        super().__init__(*args,**kwargs)
        self.__lock = Lock()

    def run(self,args_per_thread=[],kwargs_per_thread={}) :
        """
        args_per_thread = a list of lists of arguments that should be given to the independent threads
                          one list of arguments per thread
        kwargs_per_thread = a list of dicts of keyword arguments that should be given to the independent threads
                          one dict of keyword arguments per thread
        """
        super().run()
        #correct the arguments for each thread
        self.__args_per_thread = args_per_thread
        if self.__args_per_thread==[] or (not type(self.__args_per_thread)==list) :
            self.__args_per_thread = [self.__args_per_thread]
        if not len(self.__args_per_thread)==self.__n_threads :
            if not len(self.__args_per_thread)==1 :
                errmsg = 'ERROR: ControlledProcessMultiThreaded.run was given a list of arguments with '
                errmsg+= f'{len(self.__args_per_thread)} entries, but was set up to use {self.__n_threads} threads!'
                self.logger.error(errmsg,ValueError)
            else :
                self.__args_per_thread = self.__n_threads*self.__args_per_thread
        #correct the keyword arguments for each thread
        self.__kwargs_per_thread = kwargs_per_thread
        if not type(self.__kwargs_per_thread)==list :
            self.__kwargs_per_thread = [self.__kwargs_per_thread]
        if not len(self.__kwargs_per_thread)==self.__n_threads :
            if not len(self.__kwargs_per_thread)==1 :
                errmsg = 'ERROR: ControlledProcessMultiThreaded.run was given a list of arguments with '
                errmsg+= f'{len(self.__kwargs_per_thread)} entries, but was set up to use {self.__n_threads} threads!'
                self.logger.error(errmsg,ValueError)
            else :
                self.__kwargs_per_thread = self.__n_threads*self.__kwargs_per_thread
        #create and start the independent threads
        self.__threads = []
        for i in range(self.__n_threads) :
            self.__threads.append(MyThread(target=self._run_worker,
                                           args=(self.__lock,*self.__args_per_thread[i]),
                                           kwargs=self.__kwargs_per_thread[i]))
            self.__threads[-1].start()
        #loop while the process is alive, checking the control command queue and printing the "still alive" character
        while self.alive :
            self._print_still_alive()
            self._check_control_command_queue()
            self.__restart_crashed_threads()

    def _on_shutdown(self) :
        """
        Can override this method further in subclasses, just be sure to also call super()._on_shutdown() to join threads
        """
        for t in self.__threads :
            t.join()

    @abstractmethod
    def _run_worker(self,lock,*args,**kwargs) :
        """
        A function that should include a while self.alive loop for each independent thread to run 
        until the process is shut down

        lock = a Lock that can be used across all shared threads to guaranteee exactly one process is running

        Not implemented in the base class
        """
        pass

    def __restart_crashed_threads(self) :
        """
        Log Exceptions thrown by any of the threads and restart them
        """
        for ti,thread in enumerate(self.__threads) :
            if thread.caught_exception is not None :
                #log the error
                warnmsg = 'WARNING: a thread raised an Exception, which will be logged as an error below but not '
                warnmsg = 'reraised. The thread that raised the error will be restarted.'
                self.logger.warning(warnmsg)
                self.logger.log_exception_as_error(thread.caught_exception,reraise=False)
                #try to join the thread 
                try :
                    thread.join()
                except :
                    pass
                finally :
                    self.__threads[ti] = None
                #restart the thread
                self.__threads[ti] = MyThread(target=self._run_worker,
                                              args=(self.__lock,*self.__args_per_thread[ti]),
                                              kwargs=self.__kwargs_per_thread[ti])
                self.__threads[ti].start()
