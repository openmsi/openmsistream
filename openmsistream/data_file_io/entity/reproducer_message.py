"""Base class for messages computed from reconstructed data files and produced to new topics"""

#imports
from abc import ABC
from ...kafka_wrapper.producible import Producible
from ..utilities import get_message_prepend

class ReproducerMessage(Producible,ABC) :
    """
    Abstract base class for messages that are computed from data files and produced to a new topic

    :param datafile: The DataFile object used to compute this message
    :type datafile: :class:`~.data_file_io.DownloadDataFileToMemory`
    """

    @property
    def msg_key(self) :
        """
        The automatically defined key for these messages is a string version of the path
        to the original file with "_processing_result" appended
        """
        key_pp = get_message_prepend(self.__datafile.subdir_str,self.__datafile.filename)
        return f'{key_pp}_processing_result'

    @property
    def callback_kwargs(self) :
        """
        The callback kwargs defined for these messages are the original datafile name, its relative filepath,
        and the total number of chunks into which the file was broken.
        """
        return {'filename':self.__datafile.filename,
                'rel_filepath':self.__datafile.relative_filepath,
                'n_total_chunks':self.__datafile.n_total_chunks}

    def get_log_msg(self,print_every=None) :
        """
        Prints a message saying that the processing result from [filepath] is being produced, if print_every is defined
        """
        if print_every :
            return f'Producing processing result from {self.__datafile.filepath}'
        return None

    def __init__(self,datafile,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.__datafile = datafile
