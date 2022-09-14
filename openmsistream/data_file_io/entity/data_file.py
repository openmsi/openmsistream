"""A single data file"""

#imports
from ...utilities import LogOwner
from ...utilities.misc import populated_kwargs

class DataFile(LogOwner) :
    """
    Base class for representing a single data file
    """

    def __init__(self,filepath,*args,**kwargs) :
        """
        filepath = path to the file
        """
        self.filepath = filepath
        self.filename = self.filepath.name
        kwargs = populated_kwargs(kwargs,{'logger_file':self.filepath.parent})
        super().__init__(*args,**kwargs)
