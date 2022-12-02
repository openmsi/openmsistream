"""
Code for managing the two .csv files listing files that are
being reconstructed from the topic and files that have been fully handled
"""

#imports
import pathlib, datetime, re, threading
from abc import ABC
from dataclasses import dataclass
from ....utilities import DataclassTableReadOnly, DataclassTableAppendOnly, DataclassTable, LogOwner
from ...utilities import get_message_prepend

@dataclass
class StreamHandlerRegistryLineInProgress :
    """
    A line in the table listing files in progress
    """
    filename : str #the name of the file
    rel_filepath : pathlib.Path #the path to the file, relative to its root directory
    status : str #the status of the file, either "in_progress", "failed", or "mismatched_hash"
    n_chunks : int #the total number of chunks in the file
    first_message : datetime.datetime #timestamp of when the first message for the file was consumed
    most_recent_message : datetime.datetime #timestamp of when the most recent chunk from the file was consumed

@dataclass
class StreamHandlerRegistryLineSucceeded :
    """
    A line in the table listing files that have been successfully handled
    """
    filename : str #the name of the file
    rel_filepath : pathlib.Path #the path to the file, relative to its root directory
    n_chunks : int #the total number of chunks in the file
    first_message : datetime.datetime #timestamp of when the most recent chunk from the file was consumed
    processed_at : datetime.datetime #timestamp of when the file was processed

class StreamHandlerRegistry(LogOwner,ABC) :
    """
    A general base class to keep track of the status of files read during stream handling
    """

    IN_PROGRESS = 'in_progress'
    MISMATCHED_HASH = 'mismatched_hash'

    @property
    def in_progress_table(self) :
        """
        A read-only version of the table listing files in progress
        """
        return self._in_progress_table.as_read_only()

    @property
    def succeeded_table(self) :
        """
        A read-only version of the table listing files that have been successfully handled
        """
        self.consolidate_succeeded_files()
        return DataclassTableReadOnly(StreamHandlerRegistryLineSucceeded,
                                      filepath=self.succeeded_filepath,logger=self.logger)

    @property
    def filepaths_to_rerun(self) :
        """
        A list of all the filepaths that have not yet been successfully processed
        """
        to_rerun = []
        for addr in self._in_progress_table.obj_addresses :
            to_rerun.append(self._in_progress_table.get_entry_attrs(addr,'rel_filepath'))
        return to_rerun

    @property
    def rerun_file_key_regex(self) :
        """
        Regex matching keys for messages from files that need to be rerun
        """
        if self.n_files_to_rerun<=0 :
            return None
        regex_str = r'^('
        for fp in self.filepaths_to_rerun :
            if fp!=fp.name :
                subdir_str = str(fp.parent.as_posix())
            else :
                subdir_str = None
            regex_str+=f'{get_message_prepend(subdir_str,fp.name)}|'
        regex_str=f'{regex_str[:-1]})_.*$'
        return re.compile(regex_str)

    @property
    def n_files_to_rerun(self) :
        """
        The number of files in the registry that are marked as "in_progress"
        """
        return len(self.filepaths_to_rerun)

    def __init__(self,in_progress_filepath,succeeded_filepath,*args,max_succeeded_lines_per_file=1000,**kwargs) :
        """
        in_progress_filepath = path to the file that should hold the "in_progress" datatable
        succeeded_filepath = path to the file that should hole the "succeeded" datatable
        """
        super().__init__(*args,**kwargs)
        self._in_progress_table = DataclassTable(dataclass_type=StreamHandlerRegistryLineInProgress,
                                                  filepath=in_progress_filepath,logger=self.logger)
        self.succeeded_filepath = succeeded_filepath
        self.max_succeeded_lines_per_file = max_succeeded_lines_per_file
        self.__succeeded_tables_by_id = {}
        self.consolidate_succeeded_files()

    def register_file_in_progress(self,dfc) :
        """
        Add/update a line in the table showing that a file is in progress
        """
        self._add_or_modify_in_progress_entry(dfc,self.IN_PROGRESS)

    def register_file_mismatched_hash(self,dfc) :
        """
        Add/update a line in the table to show that a consumed file was mismatched with its original content hash
        """
        self._add_or_modify_in_progress_entry(dfc,self.MISMATCHED_HASH)

    def consolidate_succeeded_files(self) :
        """
        Search the directory holding the files for all "completed" file entries and write them into a single file
        """
        #add any lines from files in the directory to the one consolidated file at the expected path
        globpattern = f'{self.succeeded_filepath.stem}*{self.succeeded_filepath.suffix}'
        consolidated_file = None
        for fp in self.succeeded_filepath.parent.glob(globpattern) :
            if fp==self.succeeded_filepath :
                continue
            for table in self.__succeeded_tables_by_id.values() :
                if fp==table.filepath :
                    table.dump_to_file()
            file_to_add = DataclassTableReadOnly(dataclass_type=StreamHandlerRegistryLineSucceeded,
                                                 filepath=fp,logger=self.logger)
            if consolidated_file is None :
                consolidated_file = DataclassTableAppendOnly(dataclass_type=StreamHandlerRegistryLineSucceeded,
                                                             filepath=self.succeeded_filepath,
                                                             logger=self.logger)
            consolidated_file.add_entries(file_to_add.objects)
        if consolidated_file is None :
            return
        #dump out the consolidated file
        consolidated_file.dump_to_file()
        #make sure that all the lines from the individual files have been successfully copied
        all_lines = (DataclassTableReadOnly(dataclass_type=StreamHandlerRegistryLineSucceeded,
                                            filepath=self.succeeded_filepath,logger=self.logger)).lines
        for fp in self.succeeded_filepath.parent.glob(globpattern) :
            if fp==self.succeeded_filepath :
                continue
            added_file = DataclassTableReadOnly(dataclass_type=StreamHandlerRegistryLineSucceeded,
                                                filepath=fp,logger=self.logger)
            for entry_line in added_file.lines :
                if entry_line not in all_lines :
                    errmsg = f'ERROR: failed to consolidate individual files into {self.succeeded_filepath}. '
                    errmsg+= 'Individual files will be retained and should be manually concatenated. '
                    errmsg+= 'Duplicate entries may be present.'
                    raise RuntimeError(errmsg)
            to_pop = [table_id for table_id,table in self.__succeeded_tables_by_id.items() if fp==table.filepath]
            for table_id in to_pop :
                self.__succeeded_tables_by_id.pop(table_id)
            fp.unlink()

    def _add_or_modify_in_progress_entry(self,dfc,new_status) :
        with self._in_progress_table.lock :
            existing_entry_addr = self._get_in_progress_address_for_rel_filepath(dfc.relative_filepath)
            if existing_entry_addr is None :
                new_entry = StreamHandlerRegistryLineInProgress(dfc.filename,
                                                                dfc.relative_filepath,
                                                                new_status,
                                                                dfc.n_total_chunks,
                                                                datetime.datetime.now(),
                                                                datetime.datetime.now())
                self._in_progress_table.add_entries(new_entry)
                self._in_progress_table.dump_to_file()
            else :
                self._in_progress_table.set_entry_attrs(existing_entry_addr,
                                                        status=new_status,
                                                        most_recent_message=datetime.datetime.now())

    def _get_in_progress_address_for_rel_filepath(self,rel_filepath) :
        existing_obj_addresses = self._in_progress_table.obj_addresses_by_key_attr('rel_filepath')
        if rel_filepath not in existing_obj_addresses :
            return None
        if len(existing_obj_addresses[rel_filepath])!=1 :
            errmsg = f'ERROR: found more than one {self.__class__.__name__} entry for relative filepath '
            errmsg+= f'{rel_filepath} in file at {self._in_progress_table.filepath}'
            self.logger.error(errmsg,exc_type=ValueError)
        return existing_obj_addresses[rel_filepath][0]

    def _add_to_succeeded_table(self,new_entry,thread_identifier=None) :
        if thread_identifier is None :
            thread_identifier = f't{threading.get_ident()}'
        if thread_identifier in self.__succeeded_tables_by_id :
            if self.__succeeded_tables_by_id[thread_identifier].n_entries>=self.max_succeeded_lines_per_file :
                old_fp = self.__succeeded_tables_by_id[thread_identifier].filepath
                timestamp = str(datetime.datetime.now().timestamp()).replace('.','_')
                new_fp = old_fp.with_name(f'{old_fp.stem}_{timestamp}{old_fp.suffix}')
                old_fp.rename(new_fp)
                new_table = DataclassTableAppendOnly(StreamHandlerRegistryLineSucceeded,
                                                     filepath=old_fp,logger=self.logger)
                self.__succeeded_tables_by_id[thread_identifier] = new_table
        else :
            new_name = f'{self.succeeded_filepath.stem}_{thread_identifier}{self.succeeded_filepath.suffix}'
            new_fp = self.succeeded_filepath.with_name(new_name)
            new_table = DataclassTableAppendOnly(StreamHandlerRegistryLineSucceeded,
                                                 filepath=new_fp,logger=self.logger)
            self.__succeeded_tables_by_id[thread_identifier] = new_table
        self.__succeeded_tables_by_id[thread_identifier].add_entries(new_entry)
        self.__succeeded_tables_by_id[thread_identifier].dump_to_file()

class StreamProcessorRegistry(StreamHandlerRegistry) :
    """
    A class to keep track of the status of files read during stream processing
    """

    FAILED = 'failed'

    def __init__(self,dirpath,topic_name,consumer_group_id,*args,**kwargs) :
        """
        dirpath    = path to the directory that should contain the csv files
        topic_name = the name of the topic that will be produced to (used in the filenames)
        """
        in_progress_filepath = dirpath / f'consuming_from_{topic_name}_in_progress_by_{consumer_group_id}.csv'
        succeeded_filepath = dirpath / f'processed_from_{topic_name}_by_{consumer_group_id}.csv'
        super().__init__(in_progress_filepath,succeeded_filepath,*args,**kwargs)

    def register_file_successfully_processed(self,dfc) :
        """
        Add/update a line in the table to show that a file has successfully been processed
        """
        self._in_progress_table.lock.acquire()
        existing_entry_addr = self._get_in_progress_address_for_rel_filepath(dfc.relative_filepath)
        if existing_entry_addr is not None :
            attrs = self._in_progress_table.get_entry_attrs(existing_entry_addr)
            self._in_progress_table.lock.release()
            new_entry = StreamHandlerRegistryLineSucceeded(attrs['filename'],
                                                           attrs['rel_filepath'],
                                                           attrs['n_chunks'],
                                                           attrs['first_message'],
                                                           datetime.datetime.now())
        else :
            self._in_progress_table.lock.release()
            new_entry = StreamHandlerRegistryLineSucceeded(dfc.filename,
                                                           dfc.relative_filepath,
                                                           dfc.n_total_chunks,
                                                           datetime.datetime.now(),
                                                           datetime.datetime.now())
        self._add_to_succeeded_table(new_entry)
        if existing_entry_addr is not None :
            try :
                self._in_progress_table.remove_entries(existing_entry_addr)
                self._in_progress_table.dump_to_file()
            except ValueError :
                pass

    def register_file_processing_failed(self,dfc) :
        """
        Add/update a line in the table to show that a file has failed to be processed
        """
        self._add_or_modify_in_progress_entry(dfc,self.FAILED)

class StreamReproducerRegistry(StreamHandlerRegistry) :
    """
    A class to keep track of the status of files read from one topic with associated information
    created and re-produced to a different topic
    """

    PRODUCING_MESSAGE_FAILED = 'producing_message_failed'
    COMPUTING_RESULT_FAILED = 'computing_result_message_failed'

    def __init__(self,dirpath,consumer_topic_name,producer_topic_name,consumer_group_id,*args,**kwargs) :
        """
        dirpath    = path to the directory that should contain the csv file
        topic_name = the name of the topic that will be produced to (used in the filename)
        """
        in_progress_filepath = dirpath / f'consuming_from_{consumer_topic_name}_in_progress_by_{consumer_group_id}.csv'
        succeeded_filepath = dirpath / f'results_produced_to_{producer_topic_name}.csv'
        super().__init__(in_progress_filepath,succeeded_filepath,*args,**kwargs)

    def register_file_results_produced(self,filename,rel_filepath,n_total_chunks,prodid) :
        """
        Add/update a line in the table to show that a file has successfully been processed
        """
        self._in_progress_table.lock.acquire()
        existing_entry_addr = self._get_in_progress_address_for_rel_filepath(rel_filepath)
        if existing_entry_addr is not None :
            attrs = self._in_progress_table.get_entry_attrs(existing_entry_addr)
            self._in_progress_table.lock.release()
            new_entry = StreamHandlerRegistryLineSucceeded(attrs['filename'],
                                                           attrs['rel_filepath'],
                                                           attrs['n_chunks'],
                                                           attrs['first_message'],
                                                           datetime.datetime.now())
        else :
            self._in_progress_table.lock.release()
            new_entry = StreamHandlerRegistryLineSucceeded(filename,
                                                           rel_filepath,
                                                           n_total_chunks,
                                                           datetime.datetime.now(),
                                                           datetime.datetime.now())
        self._add_to_succeeded_table(new_entry,f'p{prodid}')
        if existing_entry_addr is not None :
            try :
                self._in_progress_table.remove_entries(existing_entry_addr)
                self._in_progress_table.dump_to_file()
            except ValueError :
                pass

    def register_file_computing_result_failed(self,filename,rel_filepath,n_total_chunks) :
        """
        Update a line in the table to show that a file failed to have its processing result computed
        """
        self._add_or_modify_in_progress_entry_without_chunk(filename,rel_filepath,n_total_chunks,
                                                            self.COMPUTING_RESULT_FAILED)

    def register_file_result_production_failed(self,filename,rel_filepath,n_total_chunks) :
        """
        Update a line in the table to show that the call to Producer.produce() for a message computed
        from a file returned an error
        """
        self._add_or_modify_in_progress_entry_without_chunk(filename,rel_filepath,n_total_chunks,
                                                            self.PRODUCING_MESSAGE_FAILED)

    def _add_or_modify_in_progress_entry_without_chunk(self,filename,rel_filepath,n_total_chunks,new_status) :
        with self._in_progress_table.lock :
            existing_entry_addr = self._get_in_progress_address_for_rel_filepath(rel_filepath)
            if existing_entry_addr is None :
                new_entry = StreamHandlerRegistryLineInProgress(filename,
                                                                rel_filepath,
                                                                new_status,
                                                                n_total_chunks,
                                                                datetime.datetime.now(),
                                                                datetime.datetime.now())
                self._in_progress_table.add_entries(new_entry)
                self._in_progress_table.dump_to_file()
            else :
                self._in_progress_table.set_entry_attrs(existing_entry_addr,
                                                        status=new_status,
                                                        most_recent_message=datetime.datetime.now())
