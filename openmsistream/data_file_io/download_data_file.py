#imports
import os
from hashlib import sha512
from contextlib import nullcontext
from abc import ABC, abstractmethod
from .config import DATA_FILE_HANDLING_CONST
from .data_file import DataFile

class DownloadDataFile(DataFile,ABC) :
    """
    Class to represent a data file that will be read as messages from a topic
    """

    #################### PROPERTIES AND STATIC METHODS ####################

    @staticmethod
    def get_full_filepath(dfc) :
        """
        Return the full filepath of a file that will be written to disk given one of its DataFileChunks
        """
        if dfc.filename_append=='' :
            return dfc.filepath 
        else :
            filename_split = dfc.filepath.name.split('.')
            full_fp = dfc.filepath.parent/(filename_split[0]+dfc.filename_append+'.'+('.'.join(filename_split[1:])))
            return full_fp

    @property
    def full_filepath(self) :
        return self.__full_filepath

    @property
    def subdir_str(self) :
        return self.__subdir_str

    @property
    @abstractmethod
    def check_file_hash(self) :
        pass #the hash of the data in the file after it was read; not implemented in the base class

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        #start an empty set of this file's downloaded offsets
        self._chunk_offsets_downloaded = []
        self.__full_filepath = None
        self.__subdir_str = None

    def add_chunk(self,dfc,thread_lock=nullcontext(),*args,**kwargs) :
        """
        A function to process a chunk that's been read from a topic
        Returns a number of codes based on what effect adding the chunk had
        
        This function calls _on_add_chunk, 
        
        dfc = the DataFileChunk object whose data should be added
        thread_lock = the lock object to acquire/release so that race conditions don't affect 
                      reconstruction of the files (optional, only needed if running this function asynchronously)
        """
        #if this chunk's offset has already been written to disk, return the "already written" code
        with thread_lock :
            already_written = dfc.chunk_offset_write in self._chunk_offsets_downloaded
        if already_written :
            return DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
        #the filepath of this DownloadDataFile and of the given DataFileChunk must match
        if dfc.filepath!=self.filepath :
            errmsg = f'ERROR: filepath mismatch between data file chunk with {dfc.filepath} and '
            errmsg+= f'data file with {self.filepath}'
            self.logger.error(errmsg,ValueError)
        #modify the filepath to include any append to the name
        full_filepath = self.__class__.get_full_filepath(dfc)
        if self.__full_filepath is None :
            self.__full_filepath = full_filepath
            self.filename = self.__full_filepath.name
        elif self.__full_filepath!=full_filepath :
            errmsg = f'ERROR: filepath for data file chunk {dfc.chunk_i}/{dfc.n_total_chunks} with offset '
            errmsg+= f'{dfc.chunk_offset_write} is {full_filepath} but the file being reconstructed is '
            errmsg+= f'expected to have filepath {self.__full_filepath}'
            self.logger.error(errmsg,ValueError)
        #add the subdirectory string to this file
        if self.__subdir_str is None :
            self.__subdir_str = dfc.subdir_str
        elif self.__subdir_str!=dfc.subdir_str :
            errmsg = f"Mismatched subdirectory strings! From file = {self.__subdir_str}, from chunk = {dfc.subdir_str}"
            self.logger.error(errmsg,ValueError)
        #acquire the thread lock to make sure this process is the only one dealing with this particular file
        with thread_lock:
            #call the function to actually add the chunk
            self._on_add_chunk(dfc,*args,**kwargs)
            #add the offset of the added chunk to the set of reconstructed file chunks
            self._chunk_offsets_downloaded.append(dfc.chunk_offset_write)
            last_chunk = len(self._chunk_offsets_downloaded)==dfc.n_total_chunks
        #if this chunk was the last that needed to be added, check the hashes
        if last_chunk :
            if self.check_file_hash!=dfc.file_hash :
                return DATA_FILE_HANDLING_CONST.FILE_HASH_MISMATCH_CODE
            else :
                return DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE
        else :
            return DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    #################### PRIVATE HELPER FUNCTIONS ####################

    @abstractmethod
    def _on_add_chunk(dfc,*args,**kwargs) :
        """
        A function to actually process a new chunk being added to the file
        This function is executed while a thread lock is acquired so it will never run asynchronously
        Also any DataFileChunks passed to this function are guaranteed to have unique offsets
        Not implemented in the base class
        """
        pass

class DownloadDataFileToDisk(DownloadDataFile) :
    """
    Class to represent a data file that will be reconstructed on disk using messages read from a topic
    """

    #################### PROPERTIES ####################

    @property
    def check_file_hash(self) :
        check_file_hash = sha512()
        with open(self.full_filepath,'rb') as fp :
            data = fp.read()
        check_file_hash.update(data)
        return check_file_hash.digest()

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        #create the parent directory of the file if it doesn't exist yet (in case the file is in a new subdirectory)
        if not self.filepath.parent.is_dir() :
            self.filepath.parent.mkdir(parents=True)

    def _on_add_chunk(self,dfc) :
        """
        Add the data from a given file chunk to this file on disk
        """
        mode = 'r+b' if self.full_filepath.is_file() else 'w+b'
        with open(self.full_filepath,mode) as fp :
            fp.seek(dfc.chunk_offset_write)
            fp.write(dfc.data)
            fp.flush()
            os.fsync(fp.fileno())
            fp.close()

class DownloadDataFileToMemory(DownloadDataFile) :
    """
    Class to represent a data file that will be held in memory and populated by the contents of messages from a topic
    """

    #################### PROPERTIES ####################

    @property
    def bytestring(self) :
        if self.__bytestring is None :
            self.__create_bytestring()
        return self.__bytestring

    @property
    def check_file_hash(self) :
        check_file_hash = sha512()
        check_file_hash.update(self.bytestring)
        return check_file_hash.digest()

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        #start a dictionary of the file data by their offsets
        self.__chunk_data_by_offset = {}
        #placeholder for the eventual full data bytestring
        self.__bytestring = None

    #################### PRIVATE HELPER FUNCTIONS ####################

    def _on_add_chunk(self,dfc) :
        """
        Add the data from a given file chunk to the dictionary of data by offset
        """
        self.__chunk_data_by_offset[dfc.chunk_offset_write] = dfc.data

    def __create_bytestring(self) :
        """
        Makes all of the data held in the dictionary into a single bytestring ordered by offset of each chunk
        """
        bytestring = b''
        for data in [self.__chunk_data_by_offset[offset] for offset in sorted(self.__chunk_data_by_offset.keys())] :
            bytestring+=data
        self.__bytestring = bytestring
