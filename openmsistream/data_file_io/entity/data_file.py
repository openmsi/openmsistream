"""A single data file"""

#imports
from ...utilities import LogOwner
from ...utilities.misc import populated_kwargs

class DataFile(LogOwner) :
    """
    Base class for representing a single data file

    :param filepath: the path to the file
    :type filepath: :class:`pathlib.Path`
    """

    def __init__(self,filepath,*args,**kwargs) :
        self.filepath = filepath
        self.filename = self.filepath.name
        kwargs = populated_kwargs(kwargs,{'logger_file':self.filepath.parent})
        super().__init__(*args,**kwargs)
