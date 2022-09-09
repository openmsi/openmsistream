"""The base message type that must be produced by a MetadataJSONReproducer"""

#imports
import json
from ..data_file_io.entity.reproducer_message import ReproducerMessage

class MetadataJSONMessage(ReproducerMessage) :
    """
    A class to represent the json metadata from a file as a Producible
    """

    @property
    def msg_value(self) :
        if self.json_content is None :
            raise ValueError(f'ERROR: JSON content should not be None for a {self.__class__.__name__}!')
        return json.dumps(self.json_content)

    def __init__(self,*args,json_content=None,**kwargs) :
        super().__init__(*args,**kwargs)
        self.json_content=json_content
