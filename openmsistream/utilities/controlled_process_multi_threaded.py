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

    :param n_threads: the number of threads to use in the group of processes
    :type n_threads: int, optional
    """

    def __init__(self,*args,n_threads=RUN_CONST.DEFAULT_N_THREADS,**kwargs) :
        self.n_threads = n_threads
        super().__init__(*args,**kwargs)
        self.lock = Lock()
        self.__args_per_thread = []
        self.__kwargs_per_thread = {}
        self.__threads = []

    def run(self,args_per_thread=None,kwargs_per_thread=None) :
        """
        Create and start the set of independent threads running :func:`~_run_worker`, which can accept optional
        arguments/keyword arguments from the call to this function. Once the threads are started they will be
        automatically restarted if they crash.

        :param args_per_thread: a list of lists of arguments that should be given to the independent threads
            (one list of arguments per thread)
        :type args_per_thread: list, optional
        :param kwargs_per_thread: a list of dicts of keyword arguments that should be given to the independent threads
            (one dict of keyword arguments per thread)
        :type kwargs_per_thread: dict, optional
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
                self.logger.error(errmsg,exc_type=ValueError)
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
                self.logger.error(errmsg,exc_type=ValueError)
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
        Join all of the running threads. Can override this method further in subclasses, just be sure to also call
        :func:`super()._on_shutdown()`.
        """
        for thread in self.__threads :
            thread.join()

    @abstractmethod
    def _run_worker(self) :
        """
        The function that will actually be run in multiple threads while the process is alive. Should include a
        "while self.alive" loop to keep it running.

        `self.lock` can be used to guarantee exactly one copy of this function is running at a time.

        Not implemented in the base class.
        """
        raise NotImplementedError

    def __restart_crashed_threads(self) :
        """
        Log (but don't re-raise) Exceptions thrown by any of the threads and restart them based on the args/kwargs
        initially passed to :func:`~run`.
        """
        for ti,thread in enumerate(self.__threads) :
            if thread.caught_exception is not None :
                #log the error
                warnmsg = 'WARNING: a thread raised an Exception, which will be logged as an error below but not '
                warnmsg+= 're-raised. The thread that raised the error will be restarted.'
                self.logger.warning(warnmsg,exc_info=thread.caught_exception)
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
