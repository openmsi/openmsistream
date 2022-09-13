"""
Code for managing the two csv files listing files that are
in the process of being produced/have been fully produced
"""

#imports
import pathlib, datetime
from typing import Set
from dataclasses import dataclass
from ....utilities import DataclassTable, LogOwner

@dataclass
class RegistryLineInProgress :
    """
    A line in the table listing files in progress
    """
    filename : str
    filepath : pathlib.Path
    n_chunks : int
    n_chunks_delivered : int
    n_chunks_to_send : int
    started : datetime.datetime
    chunks_delivered : Set[int]
    chunks_to_send : Set[int]

@dataclass
class RegistryLineCompleted :
    """
    A line in the table listing completed files
    """
    filename : str
    filepath : pathlib.Path
    n_chunks : int
    started : datetime.datetime
    completed : datetime.datetime

class ProducerFileRegistry(LogOwner) :
    """
    A class to manage two atomic csv files listing which portions
    of which files have been uploaded to a particular topic
    """

    def __init__(self,dirpath,topic_name,*args,**kwargs) :
        """
        dirpath    = path to the directory that should contain the csv file
        topic_name = the name of the topic that will be produced to (used in the filename)
        """
        super().__init__(*args,**kwargs)
        #A table to list the files currently in progress
        in_prog_filepath = dirpath / f'files_to_upload_to_{topic_name}.csv'
        self.__in_prog = DataclassTable(dataclass_type=RegistryLineInProgress,filepath=in_prog_filepath,
                                        logger=self.logger)
        #A table to list the files that have been completely produced
        completed_filepath = dirpath / f'files_fully_uploaded_to_{topic_name}.csv'
        self.__completed = DataclassTable(dataclass_type=RegistryLineCompleted,filepath=completed_filepath,
                                          logger=self.logger)

    def get_incomplete_filepaths_and_chunks(self) :
        """
        Generate tuples of (filepath, chunks to upload) for each file that has not yet been completely uploaded
        """
        for obj_address in self.__in_prog.obj_addresses :
            attr_dict = self.__in_prog.get_entry_attrs(obj_address,'filepath','chunks_to_send')
            if len(attr_dict['chunks_to_send'])>0 :
                yield attr_dict['filepath'],attr_dict['chunks_to_send']

    def get_completed_filepaths(self) :
        """
        Generate filepaths for each file that has been completely uploaded
        """
        for obj_address in self.__completed.obj_addresses :
            yield self.__completed.get_entry_attrs(obj_address,'filepath')

    def register_chunk(self,filename,filepath,n_total_chunks,chunk_i) :
        """
        Register a chunk as having been successfully produced
        Returns True if all chunks for the file have been produced
        """
        #get a dictionary of the existing object addresses keyed by their filepaths
        existing_obj_addresses = self.__in_prog.obj_addresses_by_key_attr('filepath')
        #if the file is already recognized as in progress
        if filepath in existing_obj_addresses :
            return self.__register_chunk_of_existing_file(filename,filepath,n_total_chunks,chunk_i)
        #otherwise it's a new file to list somewhere
        return self.__register_chunk_for_new_file(filename,filepath,n_total_chunks,chunk_i)

    def __register_chunk_of_existing_file(self,filename,filepath,n_total_chunks,chunk_i) :
        """
        Register a chunk that belongs to an existing file.
        Returns True if all chunks for the file have been received by the broker.
        """
        #get a dictionary of the existing object addresses keyed by their filepaths
        existing_obj_addresses = self.__in_prog.obj_addresses_by_key_attr('filepath')
        #make sure there's only one object with this filepath
        if len(existing_obj_addresses[filepath])!=1 :
            errmsg = f'ERROR: found {len(existing_obj_addresses[filepath])} files in the producer registry '
            errmsg+= f'for filepath {filepath}'
            self.logger.error(errmsg,RuntimeError)
        existing_addr = existing_obj_addresses[filepath][0]
        #make sure the total numbers of chunks match
        existing_n_chunks = self.__in_prog.get_entry_attrs(existing_addr,'n_chunks')
        if existing_n_chunks!=n_total_chunks :
            errmsg = f'ERROR: {filepath} in {self.__class__.__name__} is listed as having '
            errmsg+= f'{existing_n_chunks} total chunks, but the producer callback for this file '
            errmsg+= f'lists {n_total_chunks} chunks! Did the chunk size change?'
            self.logger.error(errmsg,RuntimeError)
        #editing the object's attributes has to be done in one thread at a time
        with self.__in_prog.lock :
            #get its current state
            attrs = self.__in_prog.get_entry_attrs(existing_addr,'chunks_delivered','chunks_to_send')
            #if the chunk is already registered, just return
            if chunk_i in attrs['chunks_delivered'] :
                return False
            #otherwise add/remove it from the sets
            attrs['chunks_delivered'].add(chunk_i)
            try :
                attrs['chunks_to_send'].remove(chunk_i)
            except KeyError :
                pass
            #reset the attributes for the object
            new_attrs = {
                'n_chunks_delivered':len(attrs['chunks_delivered']),
                'n_chunks_to_send':len(attrs['chunks_to_send']),
                'chunks_delivered':attrs['chunks_delivered'],
                'chunks_to_send':attrs['chunks_to_send'],
                }
        self.__in_prog.set_entry_attrs(existing_addr,**new_attrs)
        # if there are no more chunks to send and all chunks have been delivered,
        # move this file from "in progress" to "completed" and force-dump the files
        if ( self.__in_prog.get_entry_attrs(existing_addr,'n_chunks_delivered')==n_total_chunks and
                self.__in_prog.get_entry_attrs(existing_addr,'n_chunks_to_send')==0 ) :
            started = self.__in_prog.get_entry_attrs(existing_addr,'started')
            completed_entry = RegistryLineCompleted(filename,filepath,n_total_chunks,
                                                    started,datetime.datetime.now())
            self.__completed.add_entries(completed_entry)
            self.__completed.dump_to_file()
            self.__in_prog.remove_entries(existing_addr)
            self.__in_prog.dump_to_file()
            return True
        return False

    def __register_chunk_for_new_file(self,filename,filepath,n_total_chunks,chunk_i) :
        """
        Register the first chunk for a file.
        Returns True if the file only had one chunk.
        """
        to_deliver = set([])
        for ichunk in range(1,n_total_chunks+1) :
            if ichunk!=chunk_i :
                to_deliver.add(ichunk)
        #if there are other chunks to deliver, register it to "in_progress"
        if len(to_deliver)>0 :
            in_prog_entry = RegistryLineInProgress(filename,filepath,n_total_chunks,
                                                    1,n_total_chunks-1,datetime.datetime.now(),
                                                    set([chunk_i]),to_deliver)
            self.__in_prog.add_entries(in_prog_entry)
            self.__in_prog.dump_to_file()
            return False
        #otherwise register it as a completed file
        completed_entry = RegistryLineCompleted(filename,filepath,n_total_chunks,
                                                datetime.datetime.now(),datetime.datetime.now())
        self.__completed.add_entries(completed_entry)
        self.__completed.dump_to_file()
        return True
