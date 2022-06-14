#imports
import pathlib, datetime, time
from threading import Lock
from queue import Queue
from ..utilities.misc import populated_kwargs
from ..shared.config import UTIL_CONST
from ..shared.my_thread import MyThread
from ..shared.runnable import Runnable
from ..shared.controlled_process import ControlledProcessSingleThread
from ..my_kafka.producer_group import ProducerGroup
from .config import RUN_OPT_CONST
from .producer_file_registry import ProducerFileRegistry
from .data_file_directory import DataFileDirectory
from .upload_data_file import UploadDataFile

class DataFileUploadDirectory(DataFileDirectory,ControlledProcessSingleThread,ProducerGroup,Runnable) :
    """
    Class representing a directory being watched for new files to be added so they can be uploaded
    """

    #################### CONSTANTS AND PROPERTIES ####################

    MIN_WAIT_TIME = 0.005 # starting point for how long to wait between pinging the directory looking for new files
    MAX_WAIT_TIME = 60 # never wait more than a minute to check again for new files
    LOG_SUBDIR_NAME = 'LOGS'

    @property
    def other_datafile_kwargs(self) :
        return {} # Overload this in subclasses to send extra keyword arguments to the individual datafile constructors
    @property
    def progress_msg(self) :
        self.__find_new_files()
        progress_msg = 'The following files have been recognized so far:\n'
        for datafile in self.data_files_by_path.values() :
            if not datafile.to_upload :
                continue
            progress_msg+=f'\t{datafile.upload_status_msg}\n'
        return progress_msg
    @property
    def have_file_to_upload(self) :
        for datafile in self.data_files_by_path.values() :
            if datafile.upload_in_progress or datafile.waiting_to_upload :
                return True
        return False
    @property
    def partially_done_file_paths(self) :
        return [fp for fp,df in self.data_files_by_path.items() if df.upload_in_progress]
    @property
    def n_partially_done_files(self) :
        return len(self.partially_done_file_paths)

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,*args,upload_regex=UTIL_CONST.DEFAULT_UPLOAD_REGEX,datafile_type=UploadDataFile,**kwargs) :
        """
        upload_regex = only files matching this regular expression will be uploaded
        datafile_type = the type of data file that recognized files should be uploaded as 
            (must be a subclass of UploadDataFile)
        """
        #create a subdirectory for the logs
        self.__logs_subdir = args[0]/self.LOG_SUBDIR_NAME
        if not self.__logs_subdir.is_dir() :
            self.__logs_subdir.mkdir(parents=True)
        #put the log file in the subdirectory
        kwargs = populated_kwargs(kwargs,{'logger_file':self.__logs_subdir})
        super().__init__(*args,**kwargs)
        if not issubclass(datafile_type,UploadDataFile) :
            errmsg = 'ERROR: DataFileUploadDirectory requires a datafile_type that is a subclass of '
            errmsg+= f'UploadDataFile but {datafile_type} was given!'
            self.logger.error(errmsg,ValueError)
        self.__upload_regex = upload_regex
        self.__datafile_type = datafile_type
        self.__wait_time = self.MIN_WAIT_TIME
        self.__lock = Lock()
        
    def upload_files_as_added(self,topic_name,
                              n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                              chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE,
                              max_queue_size=RUN_OPT_CONST.DEFAULT_MAX_UPLOAD_QUEUE_SIZE,
                              upload_existing=False) :
        """
        Listen for new files to be added to the directory. Chunk and produce newly added files as messages to the topic.

        topic_name       = name of the topic to produce messages to
        n_threads        = the number of threads to use to produce from the shared queue
        chunk_size       = the size of each file chunk in bytes
        max_queue_size   = maximum number of items allowed to be placed in the upload queue at once
        upload_existing  = set to True if any files that already exist in the directory should be uploaded
                           i.e., if False (the default) then only files added to the directory after startup
                           will be enqueued to the producer
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
        self.__upload_queue = Queue(max_queue_size)
        #start the producers and upload threads
        self.__producers = []
        self.__upload_threads = []
        for _ in range(n_threads) :
            self.__producers.append(self.get_new_producer())
            t = MyThread(target=self.__producers[-1].produce_from_queue,
                       args=(self.__upload_queue,self.__topic_name),
                       kwargs={'callback': self.producer_callback},
                    )
            t.start()
            self.__upload_threads.append(t)
        #loop until the user inputs a command to stop
        self.run()
        #return a list of filepaths that have been uploaded
        return [fp for fp,datafile in self.data_files_by_path.items() if datafile.fully_produced]

    def filepath_should_be_uploaded(self,filepath) :
        """
        Filter filepaths from a glob and return a boolean that's True if they should be uploaded
        """
        if not isinstance(filepath,pathlib.PurePath) :
            self.logger.error(f'ERROR: {filepath} passed to filepath_should_be_uploaded is not a Path!',TypeError)
        if not filepath.is_file() :
            return False
        if filepath.parent == self.__logs_subdir :
            return False
        if self.__upload_regex.match(filepath.name) :
            return True
        return False

    def producer_callback(self,err,msg,filename,filepath,n_total_chunks,chunk_i) :
        """
        This function is called for every message that is acknowledged by the broker, and it uses the file registries
        to keep the information about what has and hasn't been uploaded current to what's on the broker

        err = The KafkaError object for the message
        msg = The Message object
        filename = the name of the file the message is coming from
        filepath = the full path to the file the message is coming from
        n_total_chunks = the total number of chunks in the message
        chunk_i = the index of the file chunk in the message
        """
        # If any error occured, re-enqueue the message's file chunk 
        if err is None and msg.error() is not None :
            err = msg.error()
        if err is not None : 
            if err.fatal() :
                msg =f'WARNING: fatally failed to deliver message for chunk {chunk_i} of {filepath}. '
                msg+=f'This message will be re-enqeued. Error reason: {err.str()}'
            elif not err.retriable() :
                msg =f'WARNING: Failed to deliver message for chunk {chunk_i} of {filepath} and cannot retry. '
                msg+= f'This message will be re-enqueued. Error reason: {err.str()}'
            self.logger.warning(msg)
            self.__add_chunks_for_filepath(filepath,[chunk_i])
        # Otherwise, register the chunk as successfully sent to the broker
        else :
            with self.__lock :
                fully_produced = self.__file_registry.register_chunk(filename,filepath,n_total_chunks,chunk_i)
                #If the file has now been fully produced to the topic, set the variable for the file and log a line
                if fully_produced :
                    self.data_files_by_path[filepath].fully_produced = True
                    msg = f'{filepath.relative_to(self.dirpath)} has been fully produced to the '
                    msg+= f'"{self.__topic_name}" topic as '
                    if n_total_chunks==1 :
                        msg+=f'{n_total_chunks} message'
                    else :
                        msg+=f'a set of {n_total_chunks} messages'
                    self.logger.info(msg)

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
        self.logger.debug(self.progress_msg)
        #reset the wait time
        self.__wait_time = self.MIN_WAIT_TIME

    def _on_shutdown(self) :
        self.logger.info('Will quit after all currently enqueued files are done being transferred.')
        self.logger.info(self.progress_msg)
        #add the remainder of any files currently in progress
        if self.n_partially_done_files>0 :
            msg='Will finish queueing the remainder of the following files before flushing the producer and quitting:\n'
            for pdfp in self.partially_done_file_paths :
                msg+=f'\t{pdfp}\n'
            self.logger.info(msg)
        while self.n_partially_done_files>0 :
            for datafile in self.data_files_by_path.values() :
                if datafile.upload_in_progress :
                    datafile.enqueue_chunks_for_upload(self.__upload_queue,
                                                        n_threads=len(self.__upload_threads),
                                                        chunk_size=self.__chunk_size)
                    break
        #stop the uploading threads by adding "None"s to their queues and joining them
        for ut in self.__upload_threads :
            self.__upload_queue.put(None)
        for ut in self.__upload_threads :
            ut.join()
        self.logger.info('Waiting for all enqueued messages to be delivered (this may take a moment)....')
        for producer in self.__producers :
            producer.flush(timeout=-1) #don't move on until all enqueued messages have been sent/received
            producer.close()
        self.close()

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
                if (filepath not in self.data_files_by_path.keys()) and self.filepath_should_be_uploaded(filepath) :
                    #wait until the file is actually available
                    file_ready = False
                    while not file_ready :
                        try :
                            fp = open(filepath)
                            fp.close()
                            file_ready = True
                        except PermissionError :
                            time.sleep(5.0)
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
        for filepath,chunks in self.__file_registry.get_incomplete_filepaths_and_chunks() :
            self.logger.info(f'Found {filepath} in progress from a previous run. Will re-enqueue {len(chunks)} chunks.')
            self.__add_chunks_for_filepath(filepath,chunks)
        #make sure any files listed as "completed" will not be uploaded again
        for filepath in self.__file_registry.get_completed_filepaths() :
            if filepath in self.data_files_by_path.keys() :
                if self.data_files_by_path[filepath].to_upload :
                    msg = f'Found {filepath} listed as fully uploaded during a previous run. Will not produce it again.'
                    self.logger.info(msg)
                self.data_files_by_path[filepath].to_upload=False
            elif filepath.is_file() :
                msg = f'Found {filepath} listed as fully uploaded during a previous run. Will not produce it again.'
                self.logger.info(msg)
                self.data_files_by_path[filepath]=self.__datafile_type(filepath,
                                                                       to_upload=False,
                                                                       rootdir=self.dirpath,
                                                                       logger=self.logger,
                                                                       **self.other_datafile_kwargs)    

    def __add_chunks_for_filepath(self,filepath,chunks) :
        """
        Add the given chunks to the list of chunks to upload for a given file
        Creates the file if it doesn't already exist in the dictionary of recognized files
        """
        if filepath in self.data_files_by_path.keys() :
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
                warnmsg = 'but not reraised. The Producer and upload thread that raised the error will be restarted.'
                self.logger.warning(warnmsg)
                self.logger.log_exception_as_error(upload_thread.caught_exception,reraise=False)
                #try to join the thread 
                try :
                    upload_thread.join()
                except :
                    pass
                finally :
                    self.__upload_threads[ti] = None
                    self.__producers[ti] = None
                #recreate the producer and restart the thread
                self.__producers[ti] = self.get_new_producer()
                t = MyThread(target=self.__producers[ti].produce_from_queue,
                             args=(self.__upload_queue,self.__topic_name),
                             kwargs={'callback': self.producer_callback},
                            )
                t.start()
                self.__upload_threads[ti] = t

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
        Function to run the upload directory right from the command line
        """
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        #make the DataFileDirectory for the specified directory
        upload_file_directory = cls(args.upload_dir,args.config,
                                    upload_regex=args.upload_regex,update_secs=args.update_seconds)
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
        final_msg = f'The following {len(uploaded_filepaths)} file'
        if len(uploaded_filepaths)==1 :
            final_msg+=' was'
        else :
            final_msg+='s were'
        final_msg+=f' uploaded between {run_start} and {run_stop}:\n'
        for fp in uploaded_filepaths :
            final_msg+=f'\t{fp.relative_to(args.upload_dir)}\n'
        upload_file_directory.logger.info(final_msg)

#################### MAIN METHOD TO RUN FROM COMMAND LINE ####################

def main(args=None) :
    DataFileUploadDirectory.run_from_command_line(args)

if __name__=='__main__' :
    main()
