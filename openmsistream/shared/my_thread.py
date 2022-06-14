#imports
from threading import Thread

class MyThread(Thread) :
    """
    A small utility class to keep track of any exceptions thrown in a child thread
    and raise them at some point
    """

    @property
    def caught_exception(self) :
        return self.__exc

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.__exc = None
    
    def run(self,*args,**kwargs) :
        try :
            super().run(*args,**kwargs)
        except Exception as e :
            self.__exc = e
    
    def join(self,*args,**kwargs) :
        super().join(*args,**kwargs)
        if self.__exc is not None :
            raise self.__exc