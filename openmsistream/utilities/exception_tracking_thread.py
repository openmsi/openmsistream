"""A Thread that re-raises any Exceptions it encounters during running when it is join()ed"""

#imports
from threading import Thread

class ExceptionTrackingThread(Thread) :
    """
    A small utility class to keep track of any exceptions thrown in a child thread
    and raise them at some point
    """

    @property
    def caught_exception(self) :
        """
        Variable to hold any Exception encountered while the thread is running
        """
        return self.__exc

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.__exc = None

    def run(self,*args,**kwargs) :
        """
        Wrapper around Thread.run that holds onto any Exception raised during running
        """
        try :
            super().run(*args,**kwargs)
        except Exception as exc :
            self.__exc = exc

    def join(self,*args,**kwargs) :
        """
        Wrapper around Thread.run that re-raises any Exceptions that were encountered
        """
        super().join(*args,**kwargs)
        if self.__exc is not None :
            raise self.__exc
