"""Base class for consuming file chunks into memory and then triggering some action when whole files are available"""

#imports
import pathlib, warnings
from abc import ABC
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from kafkacrypto.message import KafkaCryptoMessage
from ...utilities.misc import populated_kwargs
from ...workflow import Runnable
from ..config import DATA_FILE_HANDLING_CONST
from ..utilities import get_encrypted_message_timestamp_string
from ..entity.download_data_file import DownloadDataFileToMemory
from .data_file_chunk_handlers import DataFileChunkHandler

class DataFileStreamHandler(DataFileChunkHandler,Runnable,ABC) :
    """
    Generic base class for consuming file chunks into memory and then doing something once entire files are available
    """

    def __init__(self,*args,output_dir=None,datafile_type=DownloadDataFileToMemory,**kwargs) :
        """
        Constructor method
        """
        #make sure the directory for the output is set
        self._output_dir = self._get_auto_output_dir() if output_dir is None else output_dir
        if not self._output_dir.is_dir() :
            self._output_dir.mkdir(parents=True)
        kwargs = populated_kwargs(kwargs,{'logger_file':self._output_dir})
        super().__init__(*args,datafile_type=datafile_type,**kwargs)
        self.logger.info(f'Log files and output will be in {self._output_dir}')
        if not issubclass(datafile_type,DownloadDataFileToMemory) :
            errmsg = f'ERROR: {self.__class__.__name__} requires a datafile_type that is a subclass of '
            errmsg+= f'DownloadDataFileToMemory but {datafile_type} was given!'
            self.logger.error(errmsg,ValueError)
        self.file_registry = None # needs to be set in subclasses

    def _process_message(self,lock,msg,rootdir_to_set=None):
        """
        Parent class message processing function to check for:
            undecryptable messages (returns False)
            files where reconstruction is just in progress (returns True)
            files with mismatched hashes (returns False)
        Child classes should call super()._process_message() and check the return value
        to find successfully-reconstructed files for further handling.
        """
        retval = super()._process_message(lock, msg, self._output_dir if rootdir_to_set is None else rootdir_to_set)
        #if the message was returned because it couldn't be decrypted, write it to the encrypted messages directory
        if ( hasattr(retval,'key') and hasattr(retval,'value') and
             (isinstance(retval.key,KafkaCryptoMessage) or isinstance(retval.value,KafkaCryptoMessage)) ) :
            self._undecryptable_message_callback(retval)
            return False
        #get the DataFileChunk from the message value
        try :
            dfc = msg.value() #from a regular Kafka Consumer
        except TypeError :
            dfc = msg.value #from KafkaCrypto
        #if the file is just in progress
        if retval is True :
            with lock :
                self.file_registry.register_file_in_progress(dfc)
            return retval
        #if the file hashes didn't match, invoke the callback and return False
        if retval==DATA_FILE_HANDLING_CONST.FILE_HASH_MISMATCH_CODE :
            errmsg = f'ERROR: hashes for file {self.files_in_progress_by_path[dfc.filepath].filename} not matched '
            errmsg+= 'after being fully read! The messages for this file will need to be consumed again if the file '
            errmsg+= 'is to be processed! Please rerun with the same consumer ID to try again.'
            self.logger.error(errmsg)
            with lock :
                self.file_registry.register_file_mismatched_hash(dfc)
            self._mismatched_hash_callback(self.files_in_progress_by_path[dfc.filepath],lock)
            with lock :
                del self.files_in_progress_by_path[dfc.filepath]
                del self.locks_by_fp[dfc.filepath]
            return False
        if retval!=DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE :
            self.logger.error(f'ERROR: unrecognized add_chunk return value: {retval}',NotImplementedError)
            return False
        return retval

    def _undecryptable_message_callback(self,msg) :
        """
        This function is called when a message that could not be decrypted is found.
        If this function is called it is likely that the file the chunk is coming from won't be able to be processed.

        In the base class, this logs a warning.

        :param msg: the :class:`kafkacrypto.Message` object with undecrypted :class:`kafkacrypto.KafkaCryptoMessages`
            for its key and/or value
        :type msg: :class:`kafkacrypto.Message`
        """
        timestamp_string = get_encrypted_message_timestamp_string(msg)
        warnmsg = f'WARNING: encountered a message that failed to be decrypted (timestamp = {timestamp_string}). '
        warnmsg+= 'This message will be skipped, and the file it came from cannot be processed from the stream '
        warnmsg+= 'until it is decryptable. Please rerun with a new Consumer ID to consume these messages again.'
        self.logger.warning(warnmsg)

    def _mismatched_hash_callback(self,datafile,lock) :
        """
        Called when a file reconstructed in memory doesn't match the hash of its contents originally on disk,
        providing an opportunity for fallback/backup processing in the case of failure.

        Does nothing in the base class.

        :param datafile: A :class:`~.data_file_io.DownloadDataFileToMemory` object that has received
            all of its messages from the topic
        :type datafile: :class:`~.data_file_io.DownloadDataFileToMemory`
        :param lock: Acquiring this :class:`threading.Lock` object would ensure that only one instance
            of :func:`~_mismatched_hash_callback` is running at once
        :type lock: :class:`threading.Lock`
        """

    @classmethod
    def _get_auto_output_dir(cls) :
        return pathlib.Path()/f'{cls.__name__}_output'

    @classmethod
    def get_command_line_arguments(cls):
        args = ['config','consumer_group_id','update_seconds']
        kwargs = {'optional_output_dir': cls._get_auto_output_dir(),}
        return args, kwargs
