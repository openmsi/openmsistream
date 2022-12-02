"""A directory holding files that may or may not be marked for uploading. Updates when new files are added."""

#imports
import pathlib, datetime, time
from threading import Lock
from queue import Queue
from ...kafka_wrapper import ProducerGroup
from ...utilities import Runnable, ControlledProcessSingleThread
from ...utilities.misc import populated_kwargs
from ...utilities.exception_tracking_thread import ExceptionTrackingThread
from ..config import RUN_OPT_CONST
from .. import DataFileDirectory
from .file_registry.producer_file_registry import ProducerFileRegistry
from ..entity.upload_data_file import UploadDataFile

class DataFileUploadDirectory(DataFileDirectory,ControlledProcessSingleThread,ProducerGroup,Runnable) :
    """
    Class representing a directory being watched for new files to be added.
    Files added to the directory are broken into chunks and uploaded as messages to a Kafka topic.

    :param dirpath: Path to the directory that should be watched for new files
    :type dirpath: :class:`pathlib.Path`
    :param config_path: Path to the config file to use in defining the Broker connection and Producers
    :type config_path: :class:`pathlib.Path`
    :param upload_regex: only files matching this regular expression will be uploaded
    :type upload_regex: :func:`re.compile`, optional
    :param datafile_type: the type of data file that recognized files should be uploaded as
        (must be a subclass of :class:`~UploadDataFile`)
    :type datafile_type: :class:`~UploadDataFile`, optional

    :raises ValueError: if `datafile_type` is not a subclass of :class:`~UploadDataFile`
    """

    #################### CONSTANTS ####################

    MIN_WAIT_TIME = 0.005 # starting point for how long to wait between pinging the directory looking for new files
    MAX_WAIT_TIME = 60 # never wait more than a minute to check again for new files
    LOG_SUBDIR_NAME = 'LOGS'

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,dirpath,config_path,upload_regex=RUN_OPT_CONST.DEFAULT_UPLOAD_REGEX,
                    datafile_type=UploadDataFile,**kwargs) :
        """
        Constructor method
        """
        #create a subdirectory for the logs
        self.__logs_subdir = dirpath/self.LOG_SUBDIR_NAME
        if not self.__logs_subdir.is_dir() :
            self.__logs_subdir.mkdir(parents=True)
        #put the log file in the subdirectory
        kwargs = populated_kwargs(kwargs,{'logger_file':self.__logs_subdir})
        super().__init__(dirpath,config_path,**kwargs)
        if not issubclass(datafile_type,UploadDataFile) :
            errmsg = 'ERROR: DataFileUploadDirectory requires a datafile_type that is a subclass of '
            errmsg+= f'UploadDataFile but {datafile_type} was given!'
            self.logger.error(errmsg,exc_type=ValueError)
        self.__upload_regex = upload_regex
        self.__datafile_type = datafile_type
        self.__wait_time = self.MIN_WAIT_TIME
        self.__lock = Lock()
        self.__topic_name = None
        self.__chunk_size = None
        self.__file_registry = None
        self.__upload_queue = None
        self.__producers = []
        self.__upload_threads = []

    def upload_files_as_added(self,topic_name,
                              n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                              chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE,
                              max_queue_size=RUN_OPT_CONST.DEFAULT_MAX_UPLOAD_QUEUE_MEGABYTES,
                              upload_existing=False) :
        """
        Watch for new files to be added to the directory.
        Chunk and produce newly added files as messages to a Kafka topic.

        :param topic_name: Name of the topic to produce messages to
        :type topic_name: str
        :param n_threads: The number of threads to use to produce from the shared queue
        :type n_threads: int, optional
        :param chunk_size: The size of each file chunk in bytes
        :type chunk_size: int, optional
        :param max_queue_size: The maximum size in MB of the internal upload queue
            (this is distinct from the librdkafka buffering queue)
        :type max_queue_size: int, optional
        :param upload_existing: True if any files that already exist in the directory should be uploaded. If False
            (the default) then only files added to the directory after startup will be enqueued to the producer
        :type upload_existing: bool, optional
        """
        #set the topic name and chunk size
        self.__topic_name = topic_name
        self.__chunk_size = chunk_size
        #start up a file registry in the watched directory
        self.__file_registry = ProducerFileRegistry(dirpath=self.__logs_subdir,topic_name=topic_name,logger=self.logger)
        #if we're only going to upload new files, exclude what's already in the directory
        if not upload_existing :
            self.__find_new_files(to_upload=False)
        #add or modify datafiles to upload based on the contents of the file registry
        self.__startup_from_file_registry()
        #start the upload queue and thread
        msg = 'Will upload '
        if not upload_existing :
            msg+='new files added to '
        else :
            msg+='files in '
        msg+=f'{self.dirpath} to the {topic_name} topic using {n_threads} threads'
        self.logger.info(msg)
        n_max_queue_items = int((1000000*max_queue_size)/self.__chunk_size)
        self.__upload_queue = Queue(n_max_queue_items)
        #start the producers and upload threads
        for _ in range(n_threads) :
            self.__producers.append(self.get_new_producer())
            thread = ExceptionTrackingThread(target=self.__producers[-1].produce_from_queue_looped,
                                             args=(self.__upload_queue,self.__topic_name),
                                             kwargs={'callback': self.producer_callback},
                    )
            thread.start()
            self.__upload_threads.append(thread)
        #loop until the user inputs a command to stop
        self.run()
        #return a list of filepaths that have been uploaded
        return [fp for fp,datafile in self.data_files_by_path.items() if datafile.fully_produced]

    def producer_callback(self,err,msg,prodid,filename,filepath,n_total_chunks,chunk_i) :
        """
        A reference to this method is given as the callback for each call to :func:`confluent_kafka.Producer.produce`.
        It is called for every message upon acknowledgement by the broker, and it uses the file registries in the
        LOGS subdirectory to keep the information about what has and hasn't been uploaded current with what has
        been received by the broker.

        If the message is the final one to be acknowledged from its corresponding :class:`~UploadDataFile`,
        the :class:`~UploadDataFile` is registered as "fully produced".

        :param err: The error object for the message
        :type err: :class:`confluent_kafka.KafkaError`
        :param msg: The message object
        :type msg: :class:`confluent_kafka.Message`
        :param prodid: The ID of the producer that produced the message (hex(id(producer)) in memory)
        :type prodid: str
        :param filename: The name of the file the message is coming from
        :type filename: str
        :param filepath: The full path to the file the message is coming from
        :type filepath: :class:`pathlib.Path`
        :param n_total_chunks: The total number of chunks in the file this message came from
        :type n_total_chunks: int
        :param chunk_i: The index of the message's file chunk in the full list for the file
        :type chunk_i: int
        """
        # If any error occured, re-enqueue the message's file chunk
        if err is None and msg.error() is not None :
            err = msg.error()
        if err is not None :
            if err.fatal() :
                warnmsg =f'WARNING: Producer with ID {prodid} fatally failed to deliver message for chunk '
                warnmsg+=f'{chunk_i} of {filepath}. This message will be re-enqeued. Error reason: {err.str()}'
            elif not err.retriable() :
                warnmsg =f'WARNING: Producer with ID {prodid} failed to deliver message for chunk {chunk_i} of '
                warnmsg+= f'{filepath} and cannot retry. This message will be re-enqueued. Error reason: {err.str()}'
            self.logger.warning(warnmsg)
            self.__add_chunks_for_filepath(filepath,[chunk_i])
        # Otherwise, register the chunk as successfully sent to the broker
        else :
            rel_filepath = filepath.relative_to(self.dirpath)
            fully_produced = self.__file_registry.register_chunk(filename,rel_filepath,n_total_chunks,chunk_i,prodid)
            #If the file has now been fully produced to the topic, set the variable for the file and log a line
            if fully_produced :
                with self.__lock :
                    self.data_files_by_path[filepath].fully_enqueued = False
                    self.data_files_by_path[filepath].fully_produced = True
                debugmsg = f'{filepath.relative_to(self.dirpath)} has been fully produced to the '
                debugmsg+= f'"{self.__topic_name}" topic as '
                if n_total_chunks==1 :
                    debugmsg+=f'{n_total_chunks} message'
                else :
                    debugmsg+=f'a set of {n_total_chunks} messages'
                self.logger.debug(debugmsg)

    def filepath_should_be_uploaded(self,filepath) :
        """
        Returns True if a given filepath should be uploaded (if it matches the upload regex)

        :param filepath: A candidate upload filepath
        :type filepath: :class:`pathlib.Path`

        :return: True if the filepath is an existing file outside of the LOGS directory
            whose name matches the upload regex, False otherwise
        :rtype: bool

        :raises TypeError: if `filepath` isn't a :class:`pathlib.Path` object
        """
        if not isinstance(filepath,pathlib.PurePath) :
            errmsg = f'ERROR: {filepath} passed to filepath_should_be_uploaded is not a Path!'
            self.logger.error(errmsg,exc_type=TypeError)
        if not filepath.is_file() :
            return False
        if self.__logs_subdir in filepath.parents :
            return False
        if self.__upload_regex.match(filepath.name) :
            return True
        return False

    #################### PRIVATE HELPER FUNCTIONS ####################

    def _run_iteration(self) :
        #check for new files in the directory if we haven't already found some to run
        if not self.have_file_to_upload :
            # wait here with some slight backoff so that the watched directory isn't just constantly pinged
            time.sleep(self.__wait_time)
            self.__find_new_files()
            n_new_callbacks = 0
            for producer in self.__producers :
                new_callbacks = producer.poll(0)
                if new_callbacks is not None :
                    n_new_callbacks+=new_callbacks
                if n_new_callbacks>0 :
                    break
            if (not self.have_file_to_upload) and (n_new_callbacks==0) :
                if self.__wait_time<self.MAX_WAIT_TIME :
                    self.__wait_time*=1.1
            else :
                self.__wait_time = self.MIN_WAIT_TIME
            return
        #find the first file that's running and add some of its chunks to the upload queue
        for datafile in self.data_files_by_path.values() :
            if datafile.upload_in_progress or datafile.waiting_to_upload :
                datafile.enqueue_chunks_for_upload(self.__upload_queue,
                                                    n_threads=len(self.__upload_threads),
                                                    chunk_size=self.__chunk_size)
        #check for new files again
        self.__find_new_files()
        #restart any crashed threads
        self.__restart_crashed_threads()

    def _on_check(self) :
        #poll the producers
        for producer in self.__producers :
            producer.poll(0)
        #log progress so far
        self.logger.info(self.status_msg)
        self.logger.debug(self.progress_msg)
        #reset the wait time
        self.__wait_time = self.MIN_WAIT_TIME

    def _on_shutdown(self) :
        self.logger.info('Will quit after all currently enqueued files are done being transferred.')
        self.logger.debug(self.progress_msg)
        #add the remainder of any files currently in progress
        if self.n_partially_done_files>0 :
            msg='Will finish queueing the remainder of the following files before flushing the producer and quitting:\n'
            for pdfp in self.partially_done_file_paths :
                msg+=f'\t{pdfp}\n'
            self.logger.debug(msg)
        while self.n_partially_done_files>0 :
            for datafile in self.data_files_by_path.values() :
                if datafile.upload_in_progress :
                    datafile.enqueue_chunks_for_upload(self.__upload_queue,
                                                        n_threads=len(self.__upload_threads),
                                                        chunk_size=self.__chunk_size)
                    break
        #stop the uploading threads by adding "None"s to their queues and joining them
        for thread in self.__upload_threads :
            self.__upload_queue.put(None)
        for thread in self.__upload_threads :
            thread.join()
        self.logger.info('Waiting for all enqueued messages to be delivered (this may take a moment)....')
        for producer in self.__producers :
            producer.flush(timeout=-1) #don't move on until all enqueued messages have been sent/received
            producer.close()
        self.close()
        self.__file_registry.consolidate_completed_files()

    def __find_new_files(self,to_upload=True) :
        """
        Search the directory for any unrecognized files and add them to _data_files_by_path

        to_upload = if False, new files found will NOT be marked for uploading
                    (default is new files are expected to be uploaded)
        """
        #This is in a try/except in case a file is moved or a subdirectory is renamed while this method is running
        #it'll just return and try again if so
        try :
            for filepath in self.dirpath.rglob('*') :
                filepath = filepath.resolve()
                if (filepath not in self.data_files_by_path) and self.filepath_should_be_uploaded(filepath) :
                    #wait until the file is actually available
                    file_ready = False
                    while not file_ready :
                        try :
                            with open(filepath) as _ :
                                file_ready = True
                        except PermissionError :
                            time.sleep(0.05)
                    self.data_files_by_path[filepath]=self.__datafile_type(filepath,
                                                                           to_upload=to_upload,
                                                                           rootdir=self.dirpath,
                                                                           logger=self.logger,
                                                                           **self.other_datafile_kwargs)
        except FileNotFoundError :
            return

    def __startup_from_file_registry(self) :
        """
        Look through the file registry to find and prepare to enqueue any files
        that were in progress the last time the code was shut down and ignore
        any files that have previously been fuly uploaded
        """
        #make sure any files listed as "in progress" have their remaining chunks enqueued
        n_files_to_resume = 0
        n_chunks_resumed = 0
        for rel_filepath,chunks in self.__file_registry.get_incomplete_filepaths_and_chunks() :
            debugmsg = f'Found {rel_filepath} in progress from a previous run. Re-enqueuing {len(chunks)} chunks.'
            self.logger.debug(debugmsg)
            self.__add_chunks_for_filepath(self.dirpath/rel_filepath,chunks)
            n_files_to_resume+=1
            n_chunks_resumed+=len(chunks)
        if n_files_to_resume>0 :
            infomsg = f'Found {n_files_to_resume} file'
            if n_files_to_resume!=1 :
                infomsg+='s'
            infomsg+= f' in progress from a previous run. Will re-enqueue {n_chunks_resumed} total chunk'
            if n_chunks_resumed!=1 :
                infomsg+='s'
            self.logger.info(infomsg)
        #make sure any files listed as "completed" will not be uploaded again
        n_files_already_uploaded = 0
        for rel_filepath in self.__file_registry.get_completed_filepaths() :
            filepath = self.dirpath/rel_filepath
            if filepath in self.data_files_by_path :
                if self.data_files_by_path[filepath].to_upload :
                    msg = f'Found {filepath} listed as fully uploaded during a previous run. Will not produce it again.'
                    self.logger.debug(msg)
                self.data_files_by_path[filepath].to_upload=False
            elif filepath.is_file() :
                msg = f'Found {filepath} listed as fully uploaded during a previous run. Will not produce it again.'
                self.logger.debug(msg)
                self.data_files_by_path[filepath]=self.__datafile_type(filepath,
                                                                       to_upload=False,
                                                                       rootdir=self.dirpath,
                                                                       logger=self.logger,
                                                                       **self.other_datafile_kwargs)
            n_files_already_uploaded+=1
        if n_files_already_uploaded>0 :
            infomsg = f'Found {n_files_already_uploaded} file'
            if n_files_already_uploaded!=1 :
                infomsg+='s'
            infomsg+=' fully uploaded during a previous run that will not be produced again.'
            self.logger.info(infomsg)

    def __add_chunks_for_filepath(self,filepath,chunks) :
        """
        Add the given chunks to the list of chunks to upload for a given file
        Creates the file if it doesn't already exist in the dictionary of recognized files
        """
        if filepath in self.data_files_by_path :
            self.data_files_by_path[filepath].to_upload=True
        else :
            self.data_files_by_path[filepath]=self.__datafile_type(filepath,
                                                                    to_upload=True,
                                                                    rootdir=self.dirpath,
                                                                    logger=self.logger,
                                                                    **self.other_datafile_kwargs)
        self.data_files_by_path[filepath].add_chunks_to_upload(chunks,chunk_size=self.__chunk_size)

    def __restart_crashed_threads(self) :
        """
        Iterate over the upload threads and restart any that crashed, logging the exception they threw
        """
        for ti,upload_thread in enumerate(self.__upload_threads) :
            if upload_thread.caught_exception is not None :
                #log the error
                warnmsg = 'WARNING: an upload thread raised an Exception, which will be logged as an error below '
                warnmsg = 'but not re-raised. The Producer and upload thread that raised the error will be restarted.'
                self.logger.warning(warnmsg,exc_info=upload_thread.caught_exception)
                #try to join the thread
                try :
                    upload_thread.join()
                except Exception :
                    pass
                finally :
                    self.__upload_threads[ti] = None
                    self.__producers[ti] = None
                #recreate the producer and restart the thread
                self.__producers[ti] = self.get_new_producer()
                thread = ExceptionTrackingThread(target=self.__producers[ti].produce_from_queue_looped,
                                                 args=(self.__upload_queue,self.__topic_name),
                                                 kwargs={'callback': self.producer_callback},
                            )
                thread.start()
                self.__upload_threads[ti] = thread

    #################### CLASS METHODS ####################

    @classmethod
    def get_command_line_arguments(cls) :
        superargs,superkwargs = super().get_command_line_arguments()
        args = [*superargs,'upload_dir','config','topic_name','upload_regex',
                'chunk_size','queue_max_size','update_seconds','upload_existing']
        kwargs = {**superkwargs,'n_threads':RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS}
        return args, kwargs

    @classmethod
    def run_from_command_line(cls,args=None) :
        """
        Run a :class:`~DataFileUploadDirectory` directly from the command line

        Calls :func:`~upload_files_as_added` on a :class:`~DataFileUploadDirectory` defined by
        command line (or given) arguments

        :param args: the list of arguments to send to the parser instead of getting them from sys.argv
        :type args: List
        """
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        #make the DataFileDirectory for the specified directory
        upload_file_directory = cls(args.upload_dir,args.config,
                                    upload_regex=args.upload_regex,update_secs=args.update_seconds,
                                    streamlevel=args.logger_stream_level,filelevel=args.logger_file_level)
        #listen for new files in the directory and run uploads as they come in until the process is shut down
        run_start = datetime.datetime.now()
        if not args.upload_existing :
            upload_file_directory.logger.info(f'Listening for files to be added to {args.upload_dir}...')
        else :
            upload_file_directory.logger.info(f'Uploading files in/added to {args.upload_dir}...')
        uploaded_filepaths = upload_file_directory.upload_files_as_added(args.topic_name,
                                                                         n_threads=args.n_threads,
                                                                         chunk_size=args.chunk_size,
                                                                         max_queue_size=args.queue_max_size,
                                                                         upload_existing=args.upload_existing)
        run_stop = datetime.datetime.now()
        upload_file_directory.logger.info(f'Done listening to {args.upload_dir} for files to upload')
        if len(uploaded_filepaths)>0 :
            final_msg = f'The following {len(uploaded_filepaths)} file'
            if len(uploaded_filepaths)==1 :
                final_msg+=' was'
            else :
                final_msg+='s were'
            final_msg+=f' uploaded between {run_start} and {run_stop}:\n'
            for fp in uploaded_filepaths :
                final_msg+=f'\t{fp.relative_to(args.upload_dir)}\n'
        else :
            final_msg=f'No files were uploaded between {run_start} and {run_stop}'
        upload_file_directory.logger.info(final_msg)

    #################### PROPERTIES ####################

    @property
    def other_datafile_kwargs(self) :
        """
        Overload this property to send extra keyword arguments to the :class:`~UploadDataFile` constructor
        for each recognized file (useful if using a custom `datafile_type`)
        """
        return {}
    @property
    def status_msg(self) :
        """
        A message stating the number of files currently at each stage of progress
        """
        self.__find_new_files()
        n_wont_be_uploaded = 0
        n_waiting_to_upload = 0
        n_upload_in_progress = 0
        n_fully_enqueued = 0
        n_fully_produced = 0
        for datafile in self.data_files_by_path.values() :
            if not datafile.to_upload :
                n_wont_be_uploaded+=1
            elif datafile.waiting_to_upload :
                n_waiting_to_upload+=1
            elif datafile.upload_in_progress :
                n_upload_in_progress+=1
            elif datafile.fully_enqueued :
                n_fully_enqueued+=1
            elif datafile.fully_produced :
                n_fully_produced+=1
        status_message = f'{len(self.data_files_by_path)} files found in {self.dirpath}'
        if len(self.data_files_by_path)>0 :
            status_message+=' ('
            if n_wont_be_uploaded>0 :
                status_message+=f'{n_wont_be_uploaded} will not be uploaded, '
            if n_waiting_to_upload>0 :
                status_message+=f'{n_waiting_to_upload} waiting to upload, '
            if n_upload_in_progress>0 :
                status_message+=f'{n_upload_in_progress} with upload in progress, '
            if n_fully_enqueued>0 :
                status_message+=f'{n_fully_enqueued} enqueued to be produced, '
            if n_fully_produced>0 :
                status_message+=f'{n_fully_produced} fully produced, '
            status_message=f'{status_message[:-2]})'
        return status_message
    @property
    def progress_msg(self) :
        """
        A message describing the files that are currently recognized as part of the directory
        """
        self.__find_new_files()
        if len(self.data_files_by_path)>0 :
            progress_msg = 'The following files have been recognized so far:\n'
            for datafile in self.data_files_by_path.values() :
                if not datafile.to_upload :
                    continue
                progress_msg+=f'\t{datafile.upload_status_msg}\n'
        else :
            progress_msg = f'No files found yet in {self.dirpath}'
        return progress_msg
    @property
    def have_file_to_upload(self) :
        """
        True if there are any files waiting to be uploaded or in progress
        """
        for datafile in self.data_files_by_path.values() :
            if datafile.upload_in_progress or datafile.waiting_to_upload :
                return True
        return False
    @property
    def partially_done_file_paths(self) :
        """
        A list of filepaths whose uploads are currently in progress
        """
        return [fp for fp,df in self.data_files_by_path.items() if df.upload_in_progress]
    @property
    def n_partially_done_files(self) :
        """
        The number of files currently in the process of being uploaded
        """
        return len(self.partially_done_file_paths)

def main(args=None) :
    """
    Main method to run from command line
    """
    DataFileUploadDirectory.run_from_command_line(args)

if __name__=='__main__' :
    main()
