#imports
from abc import ABC, abstractmethod
from threading import Lock
from kafkacrypto.message import KafkaCryptoMessage
from ..my_kafka.controlled_message_processor import ControlledMessageProcessor
from .config import DATA_FILE_HANDLING_CONST
from .data_file_chunk import DataFileChunk
from .download_data_file import DownloadDataFile

class DataFileChunkProcessor(ControlledMessageProcessor,ABC) :
    """
    Use a ControlledProcessMultiThreaded interface to process messages that are expected to be DataFileChunks
    """

    #################### PROPERTIES ####################

    @property
    def progress_msg(self) :
        progress_msg = 'The following files have been recognized so far:\n'
        for datafile in self.files_in_progress_by_path.values() :
            progress_msg+=f'\t{datafile.full_filepath} (in progress)\n'
        for fp in self.completely_processed_filepaths :
            progress_msg+=f'\t{fp} (completed)\n'
        return progress_msg

    @property
    def other_datafile_kwargs(self) :
        return {} #Overload this in child classes to define additional keyword arguments 
                  #that should go to the specific datafile constructor
    @property
    def completely_processed_filepaths(self) :
        return self.__completely_processed_filepaths

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,*args,datafile_type,**kwargs) :
        """
        datafile_type = the type of datafile that the consumed messages will be used to create 
            (must be a subclass of DownloadDataFile)
        """    
        super().__init__(*args,**kwargs)
        self.datafile_type = datafile_type
        if not issubclass(self.datafile_type,DownloadDataFile) :
            errmsg = 'ERROR: DataFileChunkProcessor requires a datafile_type that is a subclass of '
            errmsg+= f'DownloadDataFile but {self.datafile_type} was given!'
            raise ValueError(errmsg)
        self.files_in_progress_by_path = {}
        self.locks_by_fp = {}
        self.__completely_processed_filepaths = []

    #################### PRIVATE HELPER FUNCTIONS ####################

    @abstractmethod
    def _process_message(self, lock, msg, rootdir_to_set, logger):
        """
        Make sure message values are of the expected DataFileChunk type with no root directory set, and then 
        add the chunk to the data file object. If the file is in progress this function returns True.
        Otherwise the code from DownloadDataFile.add_chunk will be returned.

        If instead the message was encrypted and could not be successfully decrypted, this will return 
        the raw Message object with KafkaCryptoMessages as its key and/or value

        lock = the Thread Lock object to use when processing the file this message comes from
        msg = the actual message object from a call to consumer.poll
        rootdir_to_set = root directory for the new DataFileChunk
        logger = the logger object to use
        
        Child classes should call super()._process_message() before doing anything else with
        the message to perform these checks
        """
        # If the message has KafkaCryptoMessages as its key and/or value, then decryption failed. 
        # Return the message object instead of a code.
        if ( hasattr(msg,'key') and hasattr(msg,'value') and 
             (isinstance(msg.key,KafkaCryptoMessage) or isinstance(msg.value,KafkaCryptoMessage)) ) :
            return msg
        #get the DataFileChunk from the message value
        try :
            dfc = msg.value() #from a regular Kafka Consumer
        except :
            dfc = msg.value #from KafkaCrypto
        #make sure the chunk is of the right type
        if not isinstance(dfc,DataFileChunk) :
            errmsg = f'ERROR: expected DataFileChunk messages but received a message of type {type(dfc)}!'
            self.logger.error(errmsg,ValueError)
        #make sure the chunk doesn't already have a rootdir set
        if dfc.rootdir is not None :
            errmsg = f'ERROR: message with key {dfc.message_key} has rootdir={dfc.rootdir} '
            errmsg+= '(should be None as it was just consumed)! Will ignore this message and continue.'
            self.logger.error(errmsg)
        #set the chunk's root directory
        dfc.rootdir = rootdir_to_set
        #add the chunk's data to the file that's being reconstructed
        with lock :
            if dfc.filepath not in self.files_in_progress_by_path.keys() :
                self.files_in_progress_by_path[dfc.filepath] = self.datafile_type(dfc.filepath,
                                                                                  logger=logger,
                                                                                  **self.other_datafile_kwargs)
                self.locks_by_fp[dfc.filepath] = Lock()
        retval = self.files_in_progress_by_path[dfc.filepath].add_chunk(dfc,self.locks_by_fp[dfc.filepath])
        #If the file is just in progress, return True
        if retval in (DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS,
                            DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE) :
            return True
        #otherwise just return the code from add_chunk
        return retval
