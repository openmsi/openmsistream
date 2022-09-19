"""
A ControlledProcess running with more than one thread
"""

#imports
from threading import Lock
from abc import ABC, abstractmethod
from ..utilities.exception_tracking_thread import ExceptionTrackingThread
from .config import RUN_CONST
from .controlled_process import ControlledProcess

class ControlledProcessMultiThreaded(ControlledProcess,ABC) :
    """
    A class for running a group of processes in multiple threads until they're explicitly shut down.
    The control command queue and the actual running process are in different threads.
    """

    def __init__(self,*args,n_threads=RUN_CONST.DEFAULT_N_THREADS,**kwargs) :
        """
        n_threads = number of threads to use
        """
        self.n_threads = n_threads
        super().__init__(*args,**kwargs)
        self.lock = Lock()
        self.__args_per_thread = []
        self.__kwargs_per_thread = {}
        self.__threads = []

    def run(self,args_per_thread=None,kwargs_per_thread=None) :
        """
        args_per_thread = a list of lists of arguments that should be given to the independent threads
                          one list of arguments per thread
        kwargs_per_thread = a list of dicts of keyword arguments that should be given to the independent threads
                          one dict of keyword arguments per thread
        """
        super().run()
        #correct the arguments for each thread
        if args_per_thread is not None :
            self.__args_per_thread = args_per_thread
        if self.__args_per_thread==[] or (not isinstance(self.__args_per_thread,list)) :
            self.__args_per_thread = [self.__args_per_thread]
        if not len(self.__args_per_thread)==self.n_threads :
            if not len(self.__args_per_thread)==1 :
                errmsg = 'ERROR: ControlledProcessMultiThreaded.run was given a list of arguments with '
                errmsg+= f'{len(self.__args_per_thread)} entries, but was set up to use {self.n_threads} threads!'
                self.logger.error(errmsg,ValueError)
            else :
                self.__args_per_thread = self.n_threads*self.__args_per_thread
        #correct the keyword arguments for each thread
        if kwargs_per_thread is not None :
            self.__kwargs_per_thread = kwargs_per_thread
        if not isinstance(self.__kwargs_per_thread,list) :
            self.__kwargs_per_thread = [self.__kwargs_per_thread]
        if not len(self.__kwargs_per_thread)==self.n_threads :
            if not len(self.__kwargs_per_thread)==1 :
                errmsg = 'ERROR: ControlledProcessMultiThreaded.run was given a list of arguments with '
                errmsg+= f'{len(self.__kwargs_per_thread)} entries, but was set up to use {self.n_threads} threads!'
                self.logger.error(errmsg,ValueError)
            else :
                self.__kwargs_per_thread = self.n_threads*self.__kwargs_per_thread
        #create and start the independent threads
        for i in range(self.n_threads) :
            self.__threads.append(ExceptionTrackingThread(target=self._run_worker,
                                           args=self.__args_per_thread[i],
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
        for thread in self.__threads :
            thread.join()

    @abstractmethod
    def _run_worker(self) :
        """
        A function that should include a while self.alive loop for each independent thread to run
        until the process is shut down

        `self.lock` can be used across all shared threads to guaranteee exactly one process is running at a time

        Not implemented in the base class
        """
        raise NotImplementedError

    def __restart_crashed_threads(self) :
        """
        Log Exceptions thrown by any of the threads and restart them
        """
        for ti,thread in enumerate(self.__threads) :
            if thread.caught_exception is not None :
                #log the error
                warnmsg = 'WARNING: a thread raised an Exception, which will be logged as an error below but not '
                warnmsg+= 'reraised. The thread that raised the error will be restarted.'
                self.logger.warning(warnmsg)
                self.logger.log_exception_as_error(thread.caught_exception,reraise=False)
                #try to join the thread
                try :
                    thread.join()
                except Exception :
                    pass
                finally :
                    self.__threads[ti] = None
                #restart the thread
                self.__threads[ti] = ExceptionTrackingThread(target=self._run_worker,
                                              args=self.__args_per_thread[ti],
                                              kwargs=self.__kwargs_per_thread[ti])
                self.__threads[ti].start()
