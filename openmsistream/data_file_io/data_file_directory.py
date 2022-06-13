#imports
from ..shared.logging import LogOwner
from ..utilities.misc import populated_kwargs

class DataFileDirectory(LogOwner) :
    """
    Base class representing any directory holding data files
    """

    @property
    def dirpath(self) :
        return self.__dirpath
    @property
    def data_files_by_path(self) :
        return self.__data_files_by_path

    def __init__(self,dirpath,*args,**kwargs) :
        """
        dirpath = path to the directory 
        """
        self.__dirpath = dirpath.resolve()
        self.__data_files_by_path = {}
        kwargs = populated_kwargs(kwargs,{'logger_file':self.__dirpath})
        super().__init__(*args,**kwargs)
