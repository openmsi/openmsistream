"""A DataFile that will be broken into chunks and uploaded to a topic"""

#imports
import time
from threading import Thread
from queue import Queue
from hashlib import sha512
from ...utilities import Runnable
from ...kafka_wrapper import ProducerGroup
from ..config import RUN_OPT_CONST
from .. import DataFile
from .data_file_chunk import DataFileChunk

class UploadDataFile(DataFile,Runnable) :
    """
    Class to represent a data file whose messages will be uploaded to a topic

    Used as part of a :class:`~.DataFileUploadDirectory`, but can also be run standalone
    to upload a single file.

    :param filepath: The path to the file on disk
    :type filepath: :class:`pathlib.Path`
    :param to_upload: True (default) if the file should be uploaded. Used to set the existing files as
        "already uploaded" when running a :class:`~.DataFileUploadDirectory`.
    :type to_upload: bool, optional
    :param rootdir: path to the "root" directory that this file is in. Anything in the path beyond it
        will be added to the DataFileChunk so that it will be reconstructed inside a subdirectory
    :type rootdir: None or :class:`pathlib.Path`, optional
    :param filename_append: a string that should be appended to the end of the filename stem to distinguish the file
        that's produced from its original file on disk
    :type filename_append: None or str, optional
    """

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,filepath,to_upload=True,rootdir=None,filename_append=None,**kwargs) :
        """
        Constructor method
        """
        super().__init__(filepath,**kwargs)
        self.__to_upload = to_upload
        if rootdir is None :
            self.rootdir = self.filepath.parent
        else :
            self.rootdir = rootdir
        self.__filename_append = filename_append if filename_append is not None else ''
        self.__fully_enqueued = False
        self.__fully_produced = False
        self.chunks_to_upload = []
        self.__n_total_chunks = 0
        self.__chunk_infos = None
        self.__file_hash = None

    def upload_whole_file(self,config_path,topic_name,
                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                          chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE) :
        """
        Chunk and upload an entire file on disk to a broker's topic.

        :param config_path: Path to the config file to use in defining the Broker connection and Producers
        :type config_path: :class:`pathlib.Path`
        :param topic_name: Name of the topic to which the file's messages should be produced
        :type topic_name: str
        :param n_threads: The number of threads/Producers to run at once during uploading
        :type n_threads: int, optional
        :param chunk_size: The size of the file chunk in each message in bytes
        :type chunk_size: int, optional
        """
        startup_msg = f"Uploading entire file {self.filepath} to {topic_name} in {chunk_size} byte chunks "
        startup_msg+=f"using {n_threads} threads...."
        self.logger.info(startup_msg)
        #add all the chunks to the upload queue
        upload_queue = Queue()
        self.enqueue_chunks_for_upload(upload_queue,chunk_size=chunk_size)
        #add "None" to the queue for each thread as the final values
        for _ in range(n_threads) :
            upload_queue.put(None)
        #produce all the messages in the queue using multiple threads
        producer_group = ProducerGroup(config_path,logger=self.logger)
        producers = []
        upload_threads = []
        for _ in range(n_threads) :
            producers.append(producer_group.get_new_producer())
            thread = Thread(target=producers[-1].produce_from_queue_looped, args=(upload_queue,topic_name))
            thread.start()
            upload_threads.append(thread)
        #join the threads
        for u_t in upload_threads :
            u_t.join()
        self.logger.info('Waiting for all enqueued messages to be delivered (this may take a moment)....')
        for producer in producers :
            producer.flush(timeout=-1) #don't leave the function until all messages have been sent/received
            producer.close()
        producer_group.close()
        self.logger.info(f'Done uploading {self.filepath}')

    def add_chunks_to_upload(self,chunks_to_add=None,chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE) :
        """
        Add chunks from this file to the internal list of chunks to upload,
        possibly with some selection defined by :attr:`~select_bytes`

        :param chunks_to_add: a list of chunk indices to add to the list to be uploaded (Default=None adds all chunks)
        :type chunks_to_add: None or list[int], optional
        :param chunk_size: The size of the file chunk in each message in bytes
        :type chunk_size: int, optional
        """
        if self.__chunk_infos is None :
            try :
                self._build_list_of_file_chunks(chunk_size)
            except Exception as exc :
                fp = self.filepath.relative_to(self.rootdir) if self.rootdir is not None else self.filepath
                errmsg = f'ERROR: was not able to break {fp} into chunks for uploading. '
                errmsg+= 'Check log lines below for details on what went wrong. File will not be uploaded.'
                self.logger.error(errmsg,exc_info=exc)
                self.__to_upload = False
                return
        #add the chunks to the final list as DataFileChunk objects
        for ichunk,chunk in enumerate(self.__chunk_infos,start=1) :
            if chunks_to_add is None or ichunk in chunks_to_add :
                self.chunks_to_upload.append(DataFileChunk(self.filepath,self.filename,self.__file_hash,
                                                            chunk[0],chunk[1],chunk[2],chunk[3],ichunk,
                                                            self.__n_total_chunks,
                                                            rootdir=self.rootdir,
                                                            filename_append=self.__filename_append))
        if len(self.chunks_to_upload)>0 and self.__fully_enqueued :
            self.__fully_enqueued = False

    def enqueue_chunks_for_upload(self,queue,n_threads=None,chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE,
                                   queue_full_timeout=0.001) :
        """
        Add some chunks of this file from the internal list to a given upload queue
        (the internal list will be created if :func:`~add_chunks_to_upload` hasn't already been called).

        When the entire internal list of file chunks has been added to the queue,
        the file will be marked as fully enqueued.

        If the given queue is full, this function will wait a bit and then return.

        :param queue: the Queue to which chunks should be added
        :type queue: :class:`queue.Queue`
        :param n_threads: the number of threads running during uploading; at most 5*this number of chunks will be
            added per call to this method. If this argument isn't given, every chunk in the internal list will be added.
        :type n_threads: None or int, optional
        :param chunk_size: The size of the file chunk in each message in bytes
        :type chunk_size: int, optional
        :param queue_full_timeout: amount of time to wait before returning if the queue is full
            and new messages can't be added
        :type queue_full_timeout: float, optional
        """
        if self.__fully_enqueued :
            warnmsg = f'WARNING: enqueue_chunks_for_upload called for fully enqueued file {self.filepath}, '
            warnmsg+= 'nothing else will be added.'
            self.logger.warning(warnmsg)
            return
        if queue.full() :
            time.sleep(queue_full_timeout)
            return
        if len(self.chunks_to_upload)==0 :
            self.add_chunks_to_upload(chunk_size=chunk_size)
        if n_threads is not None :
            n_chunks_to_add = 5*n_threads
        else :
            n_chunks_to_add = len(self.chunks_to_upload)
        ichunk = 0
        while len(self.chunks_to_upload)>0 and ichunk<n_chunks_to_add :
            while queue.full() :
                time.sleep(queue_full_timeout)
            queue.put(self.chunks_to_upload.pop(0))
            ichunk+=1
        if len(self.chunks_to_upload)==0 :
            self.__fully_enqueued = True

    #################### PRIVATE HELPER FUNCTIONS ####################

    def _build_list_of_file_chunks(self,chunk_size) :
        """
        Build the list of DataFileChunks for this file

        chunk_size = the size of each chunk in bytes
        """
        #first make sure the choices of select_bytes are valid if necessary
        #and sort them by their start byte to keep the file hash in order
        if self.select_bytes :
            if not isinstance(self.select_bytes,list) :
                errmsg=f'ERROR: select_bytes={self.select_bytes} but is expected to be a list!'
                self.logger.error(errmsg,exc_type=ValueError)
            for sbt in self.select_bytes :
                if (not isinstance(sbt,tuple)) or len(sbt)!=2 :
                    errmsg = f'ERROR: found {sbt} in select_bytes but all elements are expected to be two-entry tuples!'
                    self.logger.error(errmsg,exc_type=ValueError)
                elif sbt[0]>=sbt[1] :
                    errmsg = f'ERROR: found {sbt} in select_bytes but start byte cannot be >= stop byte!'
                    self.logger.error(errmsg,exc_type=ValueError)
            sorted_select_bytes = sorted(self.select_bytes,key=lambda x: x[0])
        #start a hash for the file and the lists of chunks
        file_hash = sha512()
        chunks = []
        isb = 0 #index for the current sorted_select_bytes entry if necessary
        #read the binary data in the file as chunks of the given size, adding each chunk to the list
        with open(self.filepath,'rb') as fp :
            chunk_offset = 0
            file_offset = 0 if (not self.select_bytes) else sorted_select_bytes[isb][0]
            n_bytes_to_read = chunk_size
            if self.select_bytes :
                n_bytes_to_read = min(chunk_size,sorted_select_bytes[isb][1]-file_offset)
            chunk = fp.read(n_bytes_to_read)
            while len(chunk) > 0 :
                file_hash.update(chunk)
                chunk_hash = sha512()
                chunk_hash.update(chunk)
                chunk_hash = chunk_hash.digest()
                chunk_length = len(chunk)
                chunks.append([chunk_hash,file_offset,chunk_offset,chunk_length])
                chunk_offset += chunk_length
                file_offset += chunk_length
                if self.select_bytes and file_offset==sorted_select_bytes[isb][1] :
                    isb+=1
                    if isb>(len(sorted_select_bytes)-1) :
                        break
                    file_offset=sorted_select_bytes[isb][0]
                n_bytes_to_read = chunk_size
                if self.select_bytes :
                    n_bytes_to_read = min(chunk_size,sorted_select_bytes[isb][1]-file_offset)
                fp.seek(file_offset)
                chunk = fp.read(n_bytes_to_read)
        file_hash = file_hash.digest()
        self.logger.debug(f'File {self.filepath} has a total of {len(chunks)} chunks')
        #set the hash for the file
        self.__file_hash = file_hash
        #set the total number of chunks for this file
        self.__n_total_chunks = len(chunks)
        #build the list of all of the chunk infos for the file
        self.__chunk_infos = []
        for chunk in chunks :
            self.__chunk_infos.append(chunk)

    #################### CLASS METHODS ####################

    @classmethod
    def get_command_line_arguments(cls) :
        superargs,superkwargs = super().get_command_line_arguments()
        args = [*superargs,'filepath','config','topic_name','chunk_size']
        kwargs = {**superkwargs,'n_threads':RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS}
        return args,kwargs

    @classmethod
    def run_from_command_line(cls,args=None) :
        """
        Run an :class:`~UploadDataFile` directly from the command line

        Calls :func:`~upload_whole_file` on an :class:`~.UploadDataFile` defined by command line (or given) arguments

        :param args: the list of arguments to send to the parser instead of getting them from sys.argv
        :type args: list
        """
        #make the argument parser
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        #make the DataFile for the single specified file
        upload_file = cls(args.filepath,streamlevel=args.logger_stream_level,filelevel=args.logger_file_level)
        #chunk and upload the file
        upload_file.upload_whole_file(args.config,args.topic_name,
                                      n_threads=args.n_threads,
                                      chunk_size=args.chunk_size)

    #################### PROPERTIES ####################

    @property
    def select_bytes(self) :
        """
        In child classes, this property can be a list of tuples of (start_byte,stop_byte) in the file
        that will be the only ranges of bytes added when creating the list of chunks.
        The empty list in the base class will cause all bytes of the file to be uploaded.
        """
        return []
    @property
    def to_upload(self):
        """
        whether or not this file will be considered when uploading some group of data files
        """
        return self.__to_upload
    @to_upload.setter
    def to_upload(self,to_upload) :
        if to_upload and (not self.__to_upload) :
            self.logger.debug(f'Setting {self.filepath} to be uploaded')
        elif (not to_upload) and self.__to_upload :
            self.logger.debug(f'Setting {self.filepath} to NOT be uploaded')
        self.__to_upload = to_upload
    @property
    def fully_enqueued(self) :
        """
        True if this file has had all of its chunks added to an upload queue
        """
        return self.__fully_enqueued
    @fully_enqueued.setter
    def fully_enqueued(self,fully_enqueued) :
        self.__fully_enqueued = fully_enqueued
    @property
    def fully_produced(self) :
        """
        True if this file has had all of its chunks successfully sent to the broker
        (only used when run as part of a :class:`~.DataFileUploadDirectory`)
        """
        return self.__fully_produced
    @fully_produced.setter
    def fully_produced(self,fp) :
        self.__fully_produced = fp
    @property
    def waiting_to_upload(self) :
        """
        True if this file is waiting for its upload to begin
        """
        if (not self.__to_upload) or self.__fully_enqueued or self.__fully_produced :
            return False
        if len(self.chunks_to_upload)>0 :
            return False
        return True
    @property
    def upload_in_progress(self) :
        """
        True if this file is in the process of being enqueued to be uploaded
        """
        if (not self.__to_upload) or self.__fully_enqueued or self.__fully_produced :
            return False
        if len(self.chunks_to_upload)==0 :
            return False
        return True
    @property
    def upload_status_msg(self) :
        """
        A message stating the file's name and status w.r.t. being enqueued to be uploaded
        """
        if self.rootdir is None :
            msg = f'{self.filepath} '
        else :
            msg = f'{self.filepath.relative_to(self.rootdir)} '
        if not self.__to_upload :
            msg+='(will not be uploaded)'
        elif self.__fully_produced :
            msg+=f'({self.__n_total_chunks} message'
            if self.__n_total_chunks!=1 :
                msg+='s'
            msg+=' fully produced to broker)'
        elif self.__fully_enqueued :
            msg+=f'({self.__n_total_chunks} message'
            if self.__n_total_chunks!=1 :
                msg+='s'
            msg+=' fully enqueued)'
        elif self.upload_in_progress :
            msg+=f'(in progress with {self.__n_total_chunks-len(self.chunks_to_upload)}'
            msg+=f'/{self.__n_total_chunks} messages enqueued)'
        elif self.waiting_to_upload :
            msg+=f'({self.__n_total_chunks} messages waiting to be enqueued)'
        else :
            msg+='(status unknown)'
        return msg

def main(args=None) :
    """
    main method to run from command line
    """
    UploadDataFile.run_from_command_line(args)

if __name__=='__main__' :
    main()
