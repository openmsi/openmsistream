#imports
from ..utilities.misc import populated_kwargs
from ..utilities.logging import LogOwner

class DataFile(LogOwner) :
    """
    Base class for representing a single data file
    """

    #################### PROPERTIES ####################

    @property
    def filepath(self):
        return self.__filepath
    @property
    def filename(self):
        return self.__filename
    @filename.setter
    def filename(self,fn) :
        self.__filename = fn

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,filepath,*args,**kwargs) :
        """
        filepath = path to the file
        """
        self.__filepath = filepath
        self.__filename = self.__filepath.name
        kwargs = populated_kwargs(kwargs,{'logger_file':self.__filepath.parent})
        super().__init__(*args,**kwargs)

