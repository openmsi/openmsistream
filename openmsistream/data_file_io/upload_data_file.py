#imports
import traceback, time
from threading import Thread
from queue import Queue
from hashlib import sha512
from ..shared.runnable import Runnable
from ..my_kafka.producer_group import ProducerGroup
from .config import RUN_OPT_CONST
from .data_file_chunk import DataFileChunk
from .data_file import DataFile

class UploadDataFile(DataFile,Runnable) :
    """
    Class to represent a data file whose messages will be uploaded to a topic
    """

    #################### PROPERTIES ####################

    @property
    def select_bytes(self) :
        return []   # in child classes this can be a list of tuples of (start_byte,stop_byte) 
                    # in the file that will be the only ranges of bytes added when creating the list of chunks
    @property
    def rootdir(self) :
        return self.__rootdir
    @property
    def chunks_to_upload(self) :
        return self.__chunks_to_upload
    @property
    def to_upload(self):
        return self.__to_upload #whether or not this file will be considered when uploading some group of data files
    @to_upload.setter
    def to_upload(self,tu) :
        if tu and (not self.__to_upload) :
            self.logger.debug(f'Setting {self.filepath} to be uploaded')
        elif (not tu) and self.__to_upload :
            self.logger.debug(f'Setting {self.filepath} to NOT be uploaded')
        self.__to_upload = tu
    @property
    def fully_produced(self) : #whether or not this file has had all of its chunks successfully sent to the broker
        return self.__fully_produced
    @fully_produced.setter
    def fully_produced(self,fp) :
        self.__fully_produced = fp
    @property
    def fully_enqueued(self) : #whether or not this file has had all of its chunks added to an upload queue somewhere
        return self.__fully_enqueued
    @property
    def waiting_to_upload(self) : #whether or not this file is waiting for its upload to begin
        if (not self.__to_upload) or self.__fully_enqueued :
            return False
        if len(self.__chunks_to_upload)>0 :
            return False
        return True
    @property
    def upload_in_progress(self) : #whether this file is in the process of being enqueued to be uploaded
        if (not self.__to_upload) or self.__fully_enqueued :
            return False
        if len(self.__chunks_to_upload)==0 :
            return False
        return True
    @property
    def upload_status_msg(self) : #a message stating the file's name and status w.r.t. being enqueued to be uploaded 
        if self.__rootdir is None :
            msg = f'{self.filepath} '
        else :
            msg = f'{self.filepath.relative_to(self.__rootdir)} '
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
            msg+=f'(in progress with {self.__n_total_chunks-len(self.__chunks_to_upload)}'
            msg+=f'/{self.__n_total_chunks} messages enqueued)'
        elif self.waiting_to_upload :
            msg+=f'({self.__n_total_chunks} messages waiting to be enqueued)'
        else :
            msg+='(status unknown)'
        return msg

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,*args,to_upload=True,rootdir=None,filename_append='',**kwargs) :
        """
        to_upload       = if False, the file will be ignored for purposes of uploading to a topic (default is True)
        rootdir         = path to the "root" directory that this file is in; anything in the path beyond it 
                          will be added to the DataFileChunk so that it will be reconstructed inside a subdirectory
        filename_append = a string that should be appended to the end of the filename stem to distinguish the file 
                          that's produced from its original file on disk
        """
        super().__init__(*args,**kwargs)
        self.__to_upload = to_upload
        if rootdir is None :
            self.__rootdir = self.filepath.parent
        else :
            self.__rootdir = rootdir
        self.__filename_append = filename_append
        self.__fully_enqueued = False
        self.__fully_produced = False
        self.__chunks_to_upload = []
        self.__n_total_chunks = 0
        self.__chunk_infos = None

    def add_chunks_to_upload(self,chunks_to_add=None,chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE) :
        """
        Add chunks from this file to the list of chunks to upload, possibly with some selection

        chunks_to_add = a list of chunk indices to add to the list to be uploaded
                        (Default=None adds all chunks)
        """
        if self.__chunk_infos is None :
            try :
                self._build_list_of_file_chunks(chunk_size)
            except Exception :
                self.logger.info(traceback.format_exc())
                fp = self.filepath.relative_to(self.__rootdir) if self.__rootdir is not None else self.filepath
                errmsg = f'ERROR: was not able to break {fp} into chunks for uploading. '
                errmsg+= 'Check log lines above for details on what went wrong. File will not be uploaded.'
                self.logger.error(errmsg)
                self.__to_upload = False
                return
        #add the chunks to the final list as DataFileChunk objects
        for ic,c in enumerate(self.__chunk_infos,start=1) :
            if chunks_to_add is None or ic in chunks_to_add :
                self.__chunks_to_upload.append(DataFileChunk(self.filepath,self.filename,self.__file_hash,
                                                            c[0],c[1],c[2],c[3],ic,self.__n_total_chunks,
                                                            rootdir=self.__rootdir,
                                                            filename_append=self.__filename_append))
        if len(self.__chunks_to_upload)>0 and self.__fully_enqueued :
            self.__fully_enqueued = False

    def enqueue_chunks_for_upload(self,queue,n_threads=None,chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE,
                                   queue_full_timeout=0.001) :
        """
        Add chunks of this file to a given upload queue. 
        If the file runs out of chunks it will be marked as fully enqueued.
        If the given queue is full this function will do absolutely nothing and will just return.

        Possible keyword arguments:
        n_threads          = the number of threads running during uploading; at most 5*this number of chunks will be 
                             added per call to this function if this argument isn't given, every chunk will be added
        chunk_size         = the size of each file chunk in bytes 
                             (used to create the list of file chunks if it doesn't already exist)
                             the default value will be used if this argument isn't given
        queue_full_timeout = amount of time to wait if the queue is full and new messages can't be added
        """
        if self.__fully_enqueued :
            warnmsg = f'WARNING: enqueue_chunks_for_upload called for fully enqueued file {self.filepath}, '
            warnmsg+= 'nothing else will be added.'
            self.logger.warning(warnmsg)
            return
        if queue.full() :
            time.sleep(queue_full_timeout)
            return
        if len(self.__chunks_to_upload)==0 :
            self.add_chunks_to_upload(chunk_size=chunk_size)
        if n_threads is not None :
            n_chunks_to_add = 5*n_threads
        else :
            n_chunks_to_add = len(self.__chunks_to_upload)
        ic = 0
        while len(self.__chunks_to_upload)>0 and ic<n_chunks_to_add :
            while queue.full() :
                time.sleep(queue_full_timeout)
            queue.put(self.__chunks_to_upload.pop(0))
            ic+=1
        if len(self.__chunks_to_upload)==0 :
            self.__fully_enqueued = True
    
    def upload_whole_file(self,config_path,topic_name,
                          n_threads=RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,
                          chunk_size=RUN_OPT_CONST.DEFAULT_CHUNK_SIZE) :
        """
        Chunk and upload an entire file on disk to a broker's topic.

        config_path = path to the config file to use in defining the producer
        topic_name  = name of the topic to produce messages to
        n_threads   = the number of threads to run at once during uploading
        chunk_size  = the size of each file chunk in bytes
        """
        startup_msg = f"Uploading entire file {self.filepath} to {topic_name} in {chunk_size} byte chunks "
        startup_msg+=f"using {n_threads} threads...."
        self.logger.info(startup_msg)
        #add all the chunks to the upload queue
        upload_queue = Queue()
        self.enqueue_chunks_for_upload(upload_queue,chunk_size=chunk_size)
        #add "None" to the queue for each thread as the final values
        for ti in range(n_threads) :
            upload_queue.put(None)
        #produce all the messages in the queue using multiple threads
        producer_group = ProducerGroup(config_path,logger=self.logger)
        producers = []
        upload_threads = []
        for ti in range(n_threads) :
            producers.append(producer_group.get_new_producer())
            t = Thread(target=producers[-1].produce_from_queue, args=(upload_queue,topic_name))
            t.start()
            upload_threads.append(t)
        #join the threads
        for ut in upload_threads :
            ut.join()
        self.logger.info('Waiting for all enqueued messages to be delivered (this may take a moment)....')
        for producer in producers :
            producer.flush(timeout=-1) #don't leave the function until all messages have been sent/received
            producer.close()
        producer_group.close()
        self.logger.info('Done!')

    #################### PRIVATE HELPER FUNCTIONS ####################
    
    def _build_list_of_file_chunks(self,chunk_size) :
        """
        Build the list of DataFileChunks for this file

        chunk_size = the size of each chunk in bytes
        """
        #first make sure the choices of select_bytes are valid if necessary 
        #and sort them by their start byte to keep the file hash in order
        if self.select_bytes!=[] :
            if type(self.select_bytes)!=list :
                self.logger.error(f'ERROR: select_bytes={self.select_bytes} but is expected to be a list!',ValueError)
            for sbt in self.select_bytes :
                if type(sbt)!=tuple or len(sbt)!=2 :
                    errmsg = f'ERROR: found {sbt} in select_bytes but all elements are expected to be two-entry tuples!'
                    self.logger.error(errmsg,ValueError)
                elif sbt[0]>=sbt[1] :
                    errmsg = f'ERROR: found {sbt} in select_bytes but start byte cannot be >= stop byte!'
                    self.logger.error(errmsg,ValueError)
            sorted_select_bytes = sorted(self.select_bytes,key=lambda x: x[0])
        #start a hash for the file and the lists of chunks
        file_hash = sha512()
        chunks = []
        isb = 0 #index for the current sorted_select_bytes entry if necessary
        #read the binary data in the file as chunks of the given size, adding each chunk to the list 
        with open(self.filepath,'rb') as fp :
            chunk_offset = 0
            file_offset = 0 if self.select_bytes==[] else sorted_select_bytes[isb][0]
            n_bytes_to_read = chunk_size 
            if self.select_bytes!=[] :
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
                if self.select_bytes!=[] and file_offset==sorted_select_bytes[isb][1] :
                    isb+=1
                    if isb>(len(sorted_select_bytes)-1) :
                        break
                    file_offset=sorted_select_bytes[isb][0]
                n_bytes_to_read = chunk_size 
                if self.select_bytes!=[] :
                    n_bytes_to_read = min(chunk_size,sorted_select_bytes[isb][1]-file_offset)
                fp.seek(file_offset)
                chunk = fp.read(n_bytes_to_read)
        file_hash = file_hash.digest()
        self.logger.info(f'File {self.filepath} has a total of {len(chunks)} chunks')
        #set the hash for the file
        self.__file_hash = file_hash
        #set the total number of chunks for this file
        self.__n_total_chunks = len(chunks)
        #build the list of all of the chunk infos for the file
        self.__chunk_infos = []
        for c in chunks :
            self.__chunk_infos.append(c)

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
        Run the upload data file directly from the command line
        """
        #make the argument parser
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        #make the DataFile for the single specified file
        upload_file = cls(args.filepath)
        #chunk and upload the file
        upload_file.upload_whole_file(args.config,args.topic_name,
                                      n_threads=args.n_threads,
                                      chunk_size=args.chunk_size)
        upload_file.logger.info(f'Done uploading {args.filepath}')


#################### MAIN METHOD TO RUN FROM COMMAND LINE ####################

def main(args=None) :
    UploadDataFile.run_from_command_line(args)

if __name__=='__main__' :
    main()
