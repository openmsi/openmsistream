#imports
import pathlib
from hashlib import sha512
from ..shared.logging import Logger
from ..shared.producible import Producible

class DataFileChunk(Producible) :
    """
    Class to deal with single chunks of file info
    """

    #################### PROPERTIES ####################

    @property
    def filepath(self) :
        return self.__filepath #the path to the file

    @property
    def rootdir(self) :
        return self.__rootdir #the path to the file's root directory (already set if chunk is to be produced, 
                              #but must be set later if chunk is a consumed message)

    @rootdir.setter
    def rootdir(self,rd) : #also resets the overall filepath (used in consuming messages for files in subdirectories)
        self.__rootdir=rd
        try :
            self.__filepath = self.__filepath.relative_to(self.__rootdir)
        except ValueError :
            pass
        self.__filepath = self.__rootdir / self.__filepath
    
    @property
    def data(self) :
        return self.__data #the binary data in the file chunk (populated at time of production or when consumed)

    @data.setter
    def data(self,d) :
        self.__data=d
    
    @property
    def subdir_str(self) :
        if self.__rootdir is None :
            return self.__filepath.parent.as_posix()
        relpath = self.__filepath.parent.relative_to(self.__rootdir)
        if relpath==pathlib.Path() :
            return ''
        return relpath.as_posix()

    @property
    def msg_key(self) :
        key_pp = f'{"_".join(self.subdir_str.split("/"))}'
        if key_pp!='' :
            key_pp+='_'
        return f'{key_pp}{self.filename}_chunk_{self.chunk_i}_of_{self.n_total_chunks}'

    @property
    def msg_value(self) :
        return self

    @property
    def callback_kwargs(self):
        return {
            'filepath' : self.__filepath,
            'filename' : self.filename,
            'n_total_chunks' : self.n_total_chunks,
            'chunk_i' : self.chunk_i,
            }
        

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,filepath,filename,file_hash,chunk_hash,chunk_offset_read,chunk_offset_write,chunk_size,chunk_i,
                 n_total_chunks,rootdir=None,filename_append='',data=None) :
        """
        filepath           = path to this chunk's file 
                             (fully resolved if being produced, may be relative if it was consumed)
        filename           = the name of the file
        file_hash          = hash of this chunk's entire file data
        chunk_hash         = hash of this chunk's data
        chunk_offset_read  = offset (in bytes) of this chunk within the original file
        chunk_offset_write = offset (in bytes) of this chunk within the reconstructed file 
                             (may be different than chunk_offset_read due to excluding some bytes in uploading)
        chunk_size         = size of this chunk (in bytes)
        chunk_i            = index of this chunk within the larger file
        n_total_chunks     = the total number of chunks to expect from the original file
        rootdir            = path to the "root" directory; anything beyond in the filepath is considered a subdirectory
                             (optional, can also be set later)
        filename_append    = string to append to the stem of the filename when the file is reconstructed
        data               = the actual binary data of this chunk of the file 
                             (can be set later if this chunk is being produced and not consumed)
        """
        self.__filepath = filepath
        self.filename = filename
        self.file_hash = file_hash
        self.chunk_hash = chunk_hash
        self.chunk_offset_read = chunk_offset_read
        self.chunk_offset_write = chunk_offset_write
        self.chunk_size = chunk_size
        self.chunk_i = chunk_i
        self.n_total_chunks = n_total_chunks
        self.__rootdir = rootdir
        self.filename_append = filename_append
        self.__data = data

    def __eq__(self,other) :
        if not isinstance(other,DataFileChunk) :
            return NotImplemented
        #compare everything but the filepath
        retval = self.filename == other.filename
        retval = retval and self.file_hash == other.file_hash
        retval = retval and self.chunk_hash == other.chunk_hash
        retval = retval and self.chunk_offset_write == other.chunk_offset_write
        retval = retval and self.chunk_size == other.chunk_size
        retval = retval and self.chunk_i == other.chunk_i
        retval = retval and self.n_total_chunks == other.n_total_chunks
        retval = retval and self.subdir_str == other.subdir_str
        retval = retval and self.filename_append == other.filename_append
        retval = retval and self.__data == other.data
        return retval

    def __str__(self) :
        s = 'DataFileChunk('
        s+=f'filename: {self.filename}, '
        s+=f'file_hash: {self.file_hash}, '
        s+=f'chunk_hash: {self.chunk_hash}, '
        s+=f'chunk_offset_read: {self.chunk_offset_read}, '
        s+=f'chunk_offset_write: {self.chunk_offset_write}, '
        s+=f'chunk_size: {self.chunk_size}, '
        s+=f'chunk_i: {self.chunk_i}, '
        s+=f'n_total_chunks: {self.n_total_chunks}, '
        s+=f'subdir_str: {self.subdir_str}, '
        s+=f'filename_append: {self.filename_append}, '
        #s+=f'data: {self.__data}, '
        s+=')'
        return s

    def get_log_msg(self, print_every=None):
        if (self.chunk_i-1)%print_every==0 or self.chunk_i==self.n_total_chunks :
            return f'uploading {self.filename} chunk {self.chunk_i} (out of {self.n_total_chunks})'
        else :
            return None

    def populate_with_file_data(self,logger=None) :
        """
        Populate this chunk with the actual data from the file
        """
        #create a new logger if one isn't given
        if logger is None :
            logger = Logger(self.__class__.__name__)
        #make sure the file exists
        if not self.filepath.is_file() :
            logger.error(f'ERROR: file {self.filepath} does not exist!',FileNotFoundError)
        #get the data from the file
        with open(self.filepath, "rb") as fp:
            fp.seek(self.chunk_offset_read)
            data = fp.read(self.chunk_size)
        #make sure it's of the expected size
        if len(data) != self.chunk_size:
            msg = f'ERROR: chunk {self.chunk_hash} size {len(data)} != expected size {self.chunk_size} in file '
            msg+= f'{self.filepath}, offset {self.chunk_offset_read}'
            logger.error(msg,ValueError)
        #check that its hash matches what was found at the time of putting it in the queue
        check_chunk_hash = sha512()
        check_chunk_hash.update(data)
        check_chunk_hash = check_chunk_hash.digest()
        if self.chunk_hash != check_chunk_hash:
            msg = f'ERROR: chunk hash {check_chunk_hash} != expected hash {self.chunk_hash} in file {self.filepath}, '
            msg+= f'offset {self.chunk_offset_read}'
            logger.error(msg,ValueError)
        #set the chunk's data value
        self.__data = data
