#imports
from abc import ABC, abstractmethod
from ..config import RUN_OPT_CONST, DATA_FILE_HANDLING_CONST
from .data_file_chunk_handlers import DataFileChunkProcessor
from .data_file_stream_handler import DataFileStreamHandler
from .file_registry.stream_handler_registries import StreamProcessorRegistry

class DataFileStreamProcessor(DataFileStreamHandler,DataFileChunkProcessor,ABC) :
    """
    A class to consume :class:`~DataFileChunk` messages into memory and perform some operation(s) 
    when entire files are available. This is a base class that cannot be instantiated on its own.

    :param config_path: Path to the config file to use in defining the Broker connection and Consumers
    :type config_path: :class:`pathlib.Path`
    :param topic_name: Name of the topic to which the Consumers should be subscribed
    :type topic_name: str
    :param output_dir: Path to the directory where the log and csv registry files should be kept (if None a default
        will be created in the current directory)
    :type output_dir: :class:`pathlib.Path`, optional
    :param datafile_type: the type of data file that recognized files should be reconstructed as 
        (must be a subclass of :class:`~DownloadDataFileToMemory`)
    :type datafile_type: :class:`~DownloadDataFileToMemory`, optional
    :param n_threads: the number of threads/consumers to run
    :type n_threads: int, optional
    :param consumer_group_ID: the group ID under which each consumer should be created
    :type consumer_group_ID: str, optional

    :raises ValueError: if `datafile_type` is not a subclass of :class:`~DownloadDataFileToMemory`
    """

    def __init__(self,config_file,topic_name,**kwargs) :
        """
        Constructor method signature duplicated above to display in Sphinx docs
        """
        super().__init__(config_file,topic_name,**kwargs)

    def process_files_as_read(self) :
        """
        Consumes messages and stores their data in memory.
        Uses several parallel threads to consume message and calls :func:`~_process_downloaded_data_file` 
        for fully read files. Runs until the user inputs a command to shut it down. 
        
        :return: the total number of messages consumed
        :rtype: int
        :return: the total number of messages processed (registered in memory)
        :rtype: int
        :return: the paths of files successfully processed during the run 
        :rtype: List
        """
        #startup message
        msg = f'Will process files from messages in the {self.topic_name} topic using {self.n_threads} '
        msg+= f'thread{"s" if self.n_threads>1 else ""}'
        self.logger.info(msg)
        #set up the stream processor registry
        self._file_registry = StreamProcessorRegistry(dirpath=self._output_dir,
                                                      topic_name=self.topic_name,
                                                      consumer_group_ID=self.consumer_group_ID,
                                                      logger=self.logger)
        #if there are files that need to be re-processed, set the variables to re-read messages from those files
        if self._file_registry.rerun_file_key_regex is not None :
            msg = f'Consumer{"s" if self.n_threads>1 else ""} will start from the beginning of the topic to '
            msg+= f're-read messages for {self.__file_registry.n_files_to_rerun} previously-failed '
            msg+= f'file{"s" if self.__file_registry.n_files_to_rerun>1 else ""}'
            self.logger.info(msg)
            self.restart_at_beginning=True
            self.message_key_regex=self.__file_registry.rerun_file_key_regex
        #call the run loop
        self.run()
        #return the results of the processing
        return self.n_msgs_read, self.n_msgs_processed, self.completely_processed_filepaths

    def _process_message(self,lock,msg):
        """
        Process a single message to add it to a file being held in memory until all messages are received.

        If the message failed to be decrypted, this method calls :func:`~_undecryptable_message_callback` and returns.
        
        If the message is the first one consumed for a particular file, or any message other than the last one needed, 
        it registers the file as 'in_progress' in the .csv file.
        
        If the message is the last message needed for a file and its contents match the original hash 
        of the file on disk, this method calls :func:`~_process_downloaded_data_file`.

        If the call to :func:`~_process_downloaded_data_file` returns None (success), this method moves the file to the
        'successfully processed' .csv file, and returns.
        
        If the call to :func:`~_process_downloaded_data_file` returns an Exception, this method calls 
        :func:`~_failed_processing_callback`, registers the file as 'failed' in the .csv file, and returns.

        If the message is the last one needed but the contents are somehow different than the original file on disk,
        this method calls :func:`~_mismatched_hash_callback`, registers the file as 'mismatched_hash' in the .csv file, 
        and returns.

        :param lock: Acquiring this :class:`threading.Lock` object ensures that only one instance 
            of :func:`~_process_message` is running at once
        :type lock: :class:`threading.Lock`
        :param msg: The received :class:`confluent_kafka.KafkaMessage` object, or an undecrypted KafkaCrypto message
        :type msg: :class:`confluent_kafka.KafkaMessage` or :class:`kafkacrypto.Message` 

        :return: True if processing the message was successful (file in progress or successfully processed), 
            False otherwise
        :rtype: bool
        """
        retval = super()._process_message(lock,msg)
        #if the file was in progress or had a mismatched hash, return True or False, respectively
        if retval in (True,False) :
            return retval
        #get the DataFileChunk from the message value
        try :
            dfc = msg.value() #from a regular Kafka Consumer
        except :
            dfc = msg.value #from KafkaCrypto
        #if the file has had all of its messages read successfully, send it to the processing function
        if retval==DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE :
            if dfc.rootdir is not None :
                short_filepath = self.files_in_progress_by_path[dfc.filepath].full_filepath.relative_to(dfc.rootdir)
            else :
                short_filepath = self.files_in_progress_by_path[dfc.filepath].filepath
            self.logger.info(f'Processing {short_filepath}...')
            processing_retval = self._process_downloaded_data_file(self.files_in_progress_by_path[dfc.filepath],lock)
            to_return = None
            #if it was able to be processed
            if processing_retval is None :
                self.logger.info(f'Fully-read file {short_filepath} successfully processed')
                with lock :
                    self._file_registry.register_file_successfully_processed(dfc)
                    self.completely_processed_filepaths.append(dfc.filepath)
                to_return = True
            #warn if it wasn't processed correctly and invoke the callback
            else :
                if isinstance(processing_retval,Exception) :
                    errmsg = f'ERROR: Fully-read file {short_filepath} was not able to be processed. '
                    errmsg+= 'The traceback of the Exception thrown during processing will be logged below, but not '
                    errmsg+= 're-raised. The messages for this file will need to be consumed again if the file is to '
                    errmsg+= 'be processed! Please rerun with the same consumer ID to try again.'
                    self.logger.error(errmsg,exc_obj=processing_retval,reraise=False)
                else :
                    errmsg = f'Unrecognized return value from _process_downloaded_data_file: {processing_retval}'
                    self.logger.error(errmsg)
                with lock :
                    self._file_registry.register_file_processing_failed(dfc)
                self._failed_processing_callback(self.files_in_progress_by_path[dfc.filepath],lock)
                to_return = False
            #stop tracking the file
            with lock :
                del self.files_in_progress_by_path[dfc.filepath]
                del self.locks_by_fp[dfc.filepath]
            return to_return

    @abstractmethod
    def _process_downloaded_data_file(self,datafile,lock) :
        """
        Perform some arbitrary operation(s) on a given data file that has been fully read from the stream.
        Can optionally lock other threads using the given lock.
        
        Not implemented in the base class.

        :param datafile: A :class:`~DownloadDataFileToMemory` object that has received 
            all of its messages from the topic
        :type datafile: :class:`~DownloadDataFileToMemory`
        :param lock: Acquiring this :class:`threading.Lock` object would ensure that only one instance 
            of :func:`~_process_downloaded_data_file` is running at once
        :type lock: :class:`threading.Lock`

        :return: None if processing was successful, an Exception otherwise
        """
        pass

    @abstractmethod
    def _failed_processing_callback(self,datafile,lock) :
        """
        Called when :func:`~_process_downloaded_data_file` returns an Exception, 
        providing an opportunity for fallback/backup processing in the case of failure.

        Not implemented in the base class.

        :param datafile: A :class:`~DownloadDataFileToMemory` object that has received 
            all of its messages from the topic
        :type datafile: :class:`~DownloadDataFileToMemory`
        :param lock: Acquiring this :class:`threading.Lock` object would ensure that only one instance 
            of :func:`~_failed_processing_callback` is running at once
        :type lock: :class:`threading.Lock`
        """
        pass

    def _on_check(self) :
        msg = f'{self.n_msgs_read} messages read, {self.n_msgs_processed} messages processed, '
        msg+= f'{len(self.completely_processed_filepaths)} files completely processed so far'
        self.logger.debug(msg)
        if len(self.files_in_progress_by_path)>0 or len(self.completely_processed_filepaths)>0 :
            self.logger.debug(self.progress_msg)

    @classmethod
    def get_command_line_arguments(cls):
        superargs,superkwargs = super().get_command_line_arguments()
        args = [*superargs,'topic_name']
        kwargs = {**superkwargs,'n_threads': RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS}
        return args, kwargs