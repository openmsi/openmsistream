#imports
from threading import Thread

#A small utility class to reraise any exceptions thrown in a child thread when join() is called
class MyThread(Thread) :
    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.exc = None
    def run(self,*args,**kwargs) :
        try :
            super().run(*args,**kwargs)
        except Exception as e :
            self.exc = e
    def join(self,*args,**kwargs) :
        super().join(*args,**kwargs)
        if self.exc is not None :
            raise self.exc
