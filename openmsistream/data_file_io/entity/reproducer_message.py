#imports
from abc import ABC
from ...kafka_wrapper.producible import Producible
from ..utilities import get_message_prepend

class ReproducerMessage(Producible,ABC) :
    """
    Abstract base class for messages that are computed from data files and produced to a new topic

    :param datafile: The :class:`~DownloadDataFile` object used to compute this message
    :type datafile: :class:`~DownloadDataFile`
    """
    
    @property
    def msg_key(self) :
        key_pp = get_message_prepend(self.__datafile.subdir_str,self.__datafile.filename)
        return f'{key_pp}_processing_result'

    @property
    def callback_kwargs(self) :
        if self.__datafile.subdir_str!='' :
            rel_filepath = f'{self.__datafile.subdir_str}/{self.__datafile.filename}' 
        else :
            rel_filepath = self.__datafile.filename
        return {'filename':self.__datafile.filename,
                'rel_filepath':rel_filepath,
                'n_total_chunks':self.__datafile.n_total_chunks}

    def get_log_msg(self,print_every=None) :
        """
        Given some (optional) "print_every" variable, return the string that should be logged for this message
        If "None" is returned nothing will be logged.
        """
        if print_every :
            return f'Producing processing result from {self.__datafile.filepath}'
        else :
            return None

    def __init__(self,*args,datafile,**kwargs) :
        super().__init__(*args,**kwargs)
        self.__datafile = datafile
