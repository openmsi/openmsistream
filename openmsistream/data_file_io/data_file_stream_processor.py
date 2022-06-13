#imports
import pathlib, traceback
from abc import ABC, abstractmethod
from kafkacrypto.message import KafkaCryptoMessage
from ..shared.logging import LogOwner
from .config import DATA_FILE_HANDLING_CONST
from .utilities import get_encrypted_message_timestamp_string
from .download_data_file import DownloadDataFileToMemory
from .data_file_chunk_processor import DataFileChunkProcessor

class DataFileStreamProcessor(DataFileChunkProcessor,LogOwner,ABC) :
    """
    A class to consume DataFileChunk messages into memory and perform some operation(s) when entire files are available
    """

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,*args,datafile_type=DownloadDataFileToMemory,**kwargs) :
        """
        datafile_type = the type of datafile that the consumed messages should be assumed to represent
        In this class datafile_type should be something that extends DownloadDataFileToMemory
        """   
        super().__init__(*args,datafile_type=datafile_type,**kwargs)
        if not issubclass(datafile_type,DownloadDataFileToMemory) :
            errmsg = 'ERROR: DataFileStreamProcessor requires a datafile_type that is a subclass of '
            errmsg+= f'DownloadDataFileToMemory but {datafile_type} was given!'
            self.logger.error(errmsg,ValueError)

    def process_files_as_read(self) :
        """
        Consumes messages and stores their data together separated by their original files.
        Uses several parallel threads to consume message and process fully read files. 
        Returns the total number of messages read and a list of the fully processed filenames.
        """
        msg = f'Will process files from messages in the {self.topic_name} topic using {self.n_threads} '
        msg+= f'thread{"s" if self.n_threads>1 else ""}'
        self.logger.info(msg)
        self.run()
        return self.n_msgs_read, self.n_msgs_processed, self.completely_processed_filepaths

    #################### PRIVATE HELPER FUNCTIONS ####################

    @abstractmethod
    def _process_downloaded_data_file(self,datafile,lock) :
        """
        Perform some operations on a given data file that has been fully read from the stream
        Can optionally lock other threads using the given lock
        Returns None if processing was successful and an Exception otherwise
        Not implemented in the base class
        """
        pass

    def _undecryptable_message_callback(self,msg) :
        """
        This function is called when a message that could not be decrypted in found.
        It should return True if an undecrypted message is considered "successfully processed" and False otherwise
        If this function is called it is likely that the file the chunk is coming from won't be able to be processed.

        In the base class, this logs a warning and returns False. 
        Overload this in child classes to do something more sensible.

        msg = the Message object from KafkaCrypto with KafkaCryptoMessages for its key and/or value
        """
        timestamp_string = get_encrypted_message_timestamp_string(msg)
        warnmsg = f'WARNING: encountered a message that failed to be decrypted (timestamp = {timestamp_string}). '
        warnmsg+= 'This message will be skipped, and the file it came from likely cannot be processed from the stream.'
        self.logger.warning(warnmsg)
        return False

    def _process_message(self,lock,msg):
        retval = super()._process_message(lock, msg, (pathlib.Path()).resolve(), self.logger)
        #if the message was returned because it couldn't be decrypted, write it to the encrypted messages directory
        if ( hasattr(retval,'key') and hasattr(retval,'value') and 
             (isinstance(retval.key,KafkaCryptoMessage) or isinstance(retval.value,KafkaCryptoMessage)) ) :
            return self._undecryptable_message_callback(retval)
        if retval==True :
            return retval
        #get the DataFileChunk from the message value
        try :
            dfc = msg.value() #from a regular Kafka Consumer
        except :
            dfc = msg.value #from KafkaCrypto
        #if the file has had all of its messages read successfully, send it to the processing function
        if retval==DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE :
            short_filepath = self.files_in_progress_by_path[dfc.filepath].full_filepath.relative_to(dfc.rootdir)
            msg = f'Processing {short_filepath}...'
            self.logger.info(msg)
            processing_retval = self._process_downloaded_data_file(self.files_in_progress_by_path[dfc.filepath],lock)
            with lock :
                del self.files_in_progress_by_path[dfc.filepath]
                del self.locks_by_fp[dfc.filepath]
            #if it was able to be processed
            if processing_retval is None :
                self.logger.info(f'Fully-read file {short_filepath} successfully processed')
                self.completely_processed_filepaths.append(dfc.filepath)
                return True
            #warn if it wasn't processed correctly
            else :
                if isinstance(processing_retval,Exception) :
                    try :
                        raise processing_retval
                    except Exception :
                        self.logger.info(traceback.format_exc())
                else :
                    self.logger.error(f'Return value from _process_downloaded_data_file = {processing_retval}')
                warnmsg = f'WARNING: Fully-read file {short_filepath} was not able to be processed. '
                warnmsg+= 'Check log lines above for more details on the specific error. '
                warnmsg+= 'The messages for this file will need to be consumed again if the file is to be processed!'
                self.logger.warning(warnmsg)
                return False
        #if the file hashes didn't match, return False
        elif retval==DATA_FILE_HANDLING_CONST.FILE_HASH_MISMATCH_CODE :
            warnmsg = f'WARNING: file hashes for file {self.files_in_progress_by_path[dfc.filepath].filename} '
            warnmsg+= 'not matched after being fully read! This file will not be processed.'
            self.logger.warning(warnmsg)
            with lock :
                del self.files_in_progress_by_path[dfc.filepath]
                del self.locks_by_fp[dfc.filepath]
            return False
        else :
            self.logger.error(f'ERROR: unrecognized add_chunk return value ({retval})!',NotImplementedError)
            return False

    def _on_check(self) :
        msg = f'{self.n_msgs_read} messages read, {self.n_msgs_processed} messages processed, '
        msg+= f'{len(self.completely_processed_filepaths)} files completely reconstructed so far'
        self.logger.debug(msg)
        if len(self.files_in_progress_by_path)>0 or len(self.completely_processed_filepaths)>0 :
            self.logger.debug(self.progress_msg)
