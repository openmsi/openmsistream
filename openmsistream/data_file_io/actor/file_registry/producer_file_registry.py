"""
Code for managing the two csv files listing files that are
in the process of being produced/have been fully produced
"""

#imports
import pathlib, datetime
from typing import Set
from dataclasses import dataclass
from ....utilities import DataclassTableReadOnly, DataclassTableAppendOnly, DataclassTable, LogOwner

@dataclass
class RegistryLineInProgress :
    """
    A line in the table listing files in progress
    """
    filename : str
    rel_filepath : pathlib.Path
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
    rel_filepath : pathlib.Path
    n_chunks : int
    started : datetime.datetime
    completed : datetime.datetime

class ProducerFileRegistry(LogOwner) :
    """
    A class to manage two atomic csv files listing which portions
    of which files have been uploaded to a particular topic
    """

    def __init__(self,dirpath,topic_name,*args,max_completed_lines_per_file=1000,**kwargs) :
        """
        dirpath    = path to the directory that should contain the csv file
        topic_name = the name of the topic that will be produced to (used in the filename)
        """
        super().__init__(*args,**kwargs)
        #A table to list the files currently in progress
        in_prog_filepath = dirpath / f'upload_to_{topic_name}_in_progress.csv'
        self.__in_prog = DataclassTable(dataclass_type=RegistryLineInProgress,filepath=in_prog_filepath,
                                        logger=self.logger)
        #Pattern for the "completed" .csv file names
        self.completed_filepath_pattern = dirpath / f'uploaded_to_{topic_name}.csv'
        #keep track of the maximum number of lines per "completed" file
        self.max_completed_lines_per_file = max_completed_lines_per_file
        #Make a dictionary to hold tables of any newly received "completed" entries
        self.__completed_tables_by_path = {}
        #Consolidate existing "completed" csv files into one
        self.consolidate_completed_files()

    def __del__(self) :
        self.consolidate_completed_files()

    def consolidate_completed_files(self) :
        """
        Search the directory holding the files for all "completed" file entries and write them into a single file
        """
        #add any lines from files in the directory to the one consolidated file at the expected path
        globpattern = f'{self.completed_filepath_pattern.stem}*{self.completed_filepath_pattern.suffix}'
        consolidated_file = None
        for fp in self.completed_filepath_pattern.parent.glob(globpattern) :
            if fp==self.completed_filepath_pattern :
                continue
            if fp in self.__completed_tables_by_path :
                self.__completed_tables_by_path[fp].dump_to_file()
            file_to_add = DataclassTableReadOnly(dataclass_type=RegistryLineCompleted,filepath=fp,logger=self.logger)
            if consolidated_file is None :
                consolidated_file = DataclassTableAppendOnly(dataclass_type=RegistryLineCompleted,
                                                             filepath=self.completed_filepath_pattern,
                                                             logger=self.logger)
            consolidated_file.add_entries(file_to_add.objects)
        if consolidated_file is None :
            return
        #dump out the consolidated file
        consolidated_file.dump_to_file()
        #make sure that all the lines from the individual files have been successfully copied
        all_lines = (DataclassTableReadOnly(dataclass_type=RegistryLineCompleted,
                                            filepath=self.completed_filepath_pattern,logger=self.logger)).lines
        for fp in self.completed_filepath_pattern.parent.glob(globpattern) :
            if fp==self.completed_filepath_pattern :
                continue
            added_file = DataclassTableReadOnly(dataclass_type=RegistryLineCompleted,filepath=fp,logger=self.logger)
            for entry_line in added_file.lines :
                if entry_line not in all_lines :
                    errmsg = f'ERROR: failed to consolidate individual files into {self.completed_filepath_pattern}. '
                    errmsg+= 'Individual files will be retained and should be manually concatenated. '
                    errmsg+= 'Duplicate entries may be present.'
                    raise RuntimeError(errmsg)
            if fp in self.__completed_tables_by_path :
                self.__completed_tables_by_path.pop(fp)
            fp.unlink()

    def get_incomplete_filepaths_and_chunks(self) :
        """
        Generate tuples of (rel_filepath, chunks to upload) for each file that has not yet been completely uploaded
        """
        for obj_address in self.__in_prog.obj_addresses :
            attr_dict = self.__in_prog.get_entry_attrs(obj_address,'rel_filepath','chunks_to_send')
            if len(attr_dict['chunks_to_send'])>0 :
                yield attr_dict['rel_filepath'],attr_dict['chunks_to_send']

    def get_completed_filepaths(self) :
        """
        Generate relative filepaths for each file that has been completely uploaded
        """
        self.consolidate_completed_files()
        completed = DataclassTableReadOnly(dataclass_type=RegistryLineCompleted,
                                           filepath=self.completed_filepath_pattern,logger=self.logger)
        for obj_address in completed.obj_addresses :
            yield completed.get_entry_attrs(obj_address,'rel_filepath')

    def register_chunk(self,filename,rel_filepath,n_total_chunks,chunk_i,prodid) :
        """
        Register a chunk as having been successfully produced by a particular producer
        Returns True if all chunks for the file have been produced
        """
        #acquire the lock so no other threads can change the in progress file
        self.__in_prog.lock.acquire()
        #get a dictionary of the existing object addresses keyed by their filepaths
        existing_obj_addresses = self.__in_prog.obj_addresses_by_key_attr('rel_filepath')
        #if the file is already recognized as in progress
        if rel_filepath in existing_obj_addresses :
            return self.__register_chunk_of_existing_file(filename,rel_filepath,n_total_chunks,chunk_i,prodid)
        #otherwise it's a new file to list somewhere
        return self.__register_chunk_for_new_file(filename,rel_filepath,n_total_chunks,chunk_i,prodid)

    def __register_chunk_of_existing_file(self,filename,rel_filepath,n_total_chunks,chunk_i,prodid) :
        """
        Register a chunk that belongs to an existing file.
        Returns True if all chunks for the file have been received by the broker.
        """
        #get a dictionary of the existing object addresses keyed by their filepaths
        existing_obj_addresses = self.__in_prog.obj_addresses_by_key_attr('rel_filepath')
        #make sure there's only one object with this filepath
        if len(existing_obj_addresses[rel_filepath])!=1 :
            errmsg = f'ERROR: found {len(existing_obj_addresses[rel_filepath])} files in the producer registry '
            errmsg+= f'for filepath {rel_filepath}'
            self.__in_prog.lock.release()
            self.logger.error(errmsg,exc_type=RuntimeError)
        existing_addr = existing_obj_addresses[rel_filepath][0]
        #make sure the total numbers of chunks match
        existing_n_chunks = self.__in_prog.get_entry_attrs(existing_addr,'n_chunks')
        if existing_n_chunks!=n_total_chunks :
            errmsg = f'ERROR: {rel_filepath} in {self.__class__.__name__} is listed as having '
            errmsg+= f'{existing_n_chunks} total chunks, but the producer callback for this file '
            errmsg+= f'lists {n_total_chunks} chunks! Did the chunk size change?'
            self.__in_prog.lock.release()
            self.logger.error(errmsg,exc_type=RuntimeError)
        #get its current state
        attrs = self.__in_prog.get_entry_attrs(existing_addr,'chunks_delivered','chunks_to_send')
        #if the chunk is already registered, just return
        if chunk_i in attrs['chunks_delivered'] :
            self.__in_prog.lock.release()
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
            completed_entry = RegistryLineCompleted(filename,rel_filepath,n_total_chunks,
                                                    started,datetime.datetime.now())
            self.__add_completed_entry(completed_entry,prodid)
            self.__in_prog.remove_entries(existing_addr)
            self.__in_prog.dump_to_file()
            self.__in_prog.lock.release()
            return True
        self.__in_prog.lock.release()
        return False

    def __register_chunk_for_new_file(self,filename,rel_filepath,n_total_chunks,chunk_i,prodid) :
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
            in_prog_entry = RegistryLineInProgress(filename,rel_filepath,n_total_chunks,
                                                    1,n_total_chunks-1,datetime.datetime.now(),
                                                    set([chunk_i]),to_deliver)
            self.__in_prog.add_entries(in_prog_entry)
            self.__in_prog.dump_to_file()
            self.__in_prog.lock.release()
            return False
        #otherwise register it as a completed file
        self.__in_prog.lock.release()
        completed_entry = RegistryLineCompleted(filename,rel_filepath,n_total_chunks,
                                                datetime.datetime.now(),datetime.datetime.now())
        self.__add_completed_entry(completed_entry,prodid)
        return True

    def __add_completed_entry(self,completed_entry,prodid) :
        """
        Add a given entry from a given produced as "completed", possibly juggling the files around
        if they're getting too large
        """
        #create the path to the table file that should be added to
        prod_name = f'{self.completed_filepath_pattern.stem}_p{prodid}{self.completed_filepath_pattern.suffix}'
        table_path = self.completed_filepath_pattern.with_name(prod_name)
        create_new_table_at_path = False
        #if the table already exists
        if table_path in self.__completed_tables_by_path :
            #if the table has the max # of entries or more entries already
            if self.__completed_tables_by_path[table_path].n_entries>=self.max_completed_lines_per_file :
                #dump it out to its file
                self.__completed_tables_by_path[table_path].dump_to_file()
                #rename the dumped file with a timestamp
                timestamp = str(datetime.datetime.now().timestamp()).replace('.','_')
                new_path = table_path.with_name(f'{table_path.stem}_{timestamp}{table_path.suffix}')
                table_path.rename(new_path)
                #reset the table associated with the filepath for this producer
                create_new_table_at_path = True
        else :
            create_new_table_at_path = True
        if create_new_table_at_path :
            self.__completed_tables_by_path[table_path] = DataclassTableAppendOnly(RegistryLineCompleted,
                                                                                   filepath=table_path,
                                                                                   logger=self.logger)
        self.__completed_tables_by_path[table_path].add_entries(completed_entry)
        self.__completed_tables_by_path[table_path].dump_to_file()
