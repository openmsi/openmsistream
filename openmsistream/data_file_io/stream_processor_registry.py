#imports
import pathlib, datetime
from dataclasses import dataclass
from ..utilities import DataclassTable, LogOwner

@dataclass
class RegistryLine :
    filename : str
    filepath : pathlib.Path
    status : str
    first_offset : int
    started : datetime.datetime
    completed : datetime.datetime

class StreamProcessorRegistry(LogOwner) :
    """
    A class to keep track of the status of files read during stream processing
    """

    @property
    def rerun_file_key_regex(self) :
        return None

    def __init__(self,dirpath,topic_name,*args,**kwargs) :
        """
        dirpath    = path to the directory that should contain the csv file
        topic_name = the name of the topic that will be produced to (used in the filename)
        """
        super().__init__(*args,**kwargs)
        csv_filepath = dirpath / f'files_consumed_from_{topic_name}.csv'
        self.__table = DataclassTable(dataclass_type=RegistryLine,filepath=csv_filepath,logger=self.logger)
