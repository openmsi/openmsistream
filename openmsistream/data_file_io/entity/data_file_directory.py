"""A directory of DataFiles"""

#imports
from ...utilities import LogOwner
from ...utilities.misc import populated_kwargs

class DataFileDirectory(LogOwner) :
    """
    Base class representing any directory holding data files

    :param dirpath: path to the directory
    :type dirpath: :class:`pathlib.Path`
    """

    def __init__(self,dirpath,*args,**kwargs) :
        self.dirpath = dirpath.resolve()
        self.data_files_by_path = {}
        kwargs = populated_kwargs(kwargs,{'logger_file':self.dirpath})
        super().__init__(*args,**kwargs)
