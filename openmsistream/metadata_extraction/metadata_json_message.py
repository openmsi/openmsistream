#imports
import json
from ..data_file_io.entity.reproducer_message import ReproducerMessage

class MetadataJSONMessage(ReproducerMessage) :
    """
    A class to represent the json metadata from a file as a Producible
    """

    @property
    def msg_value(self) :
        if self.__json_content is None :
            raise ValueError(f'ERROR: JSON content should not be None for a created {self.__class__.__name__}!')
        return json.dumps(self.__json_content)

    def __init__(self,*args,json_content=None,**kwargs) :
        super().__init__(*args,**kwargs)
        self.__json_content=json_content
