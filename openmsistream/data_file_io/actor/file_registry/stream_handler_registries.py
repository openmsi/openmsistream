"""
Code for managing the two .csv files listing files that are
being reconstructed from the topic and files that have been fully handled
"""

#imports
import datetime, re
from abc import ABC
from dataclasses import dataclass
from ....utilities import DataclassTable, LogOwner
from ...utilities import get_message_prepend

@dataclass
class StreamHandlerRegistryLineInProgress :
    """
    A line in the table listing files in progress
    """
    filename : str #the name of the file
    rel_filepath : str #the (posix) path to the file, relative to its root directory
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
    rel_filepath : str #the (posix) path to the file, relative to its root directory
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
        The table listing files in progress
        """
        return self._in_progress_table

    @property
    def succeeded_table(self) :
        """
        The table listing files that have been successfully handled
        """
        return self._succeeded_table

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
            if r'/' in fp :
                subdir_str = f'{"/".join((fp.split("/"))[:-1])}'
                filename = (fp.split("/"))[-1]
            else :
                subdir_str = None
                filename = fp
            regex_str+=f'{get_message_prepend(subdir_str,filename)}|'
        regex_str=f'{regex_str[:-1]})_.*$'
        return re.compile(regex_str)

    @property
    def n_files_to_rerun(self) :
        """
        The number of files in the registry that are marked as "in_progress"
        """
        return len(self.filepaths_to_rerun)

    def __init__(self,in_progress_filepath,succeeded_filepath,*args,**kwargs) :
        """
        in_progress_filepath = path to the file that should hold the "in_progress" datatable
        succeeded_filepath = path to the file that should hole the "succeeded" datatable
        """
        super().__init__(*args,**kwargs)
        self._in_progress_table = DataclassTable(dataclass_type=StreamHandlerRegistryLineInProgress,
                                                  filepath=in_progress_filepath,logger=self.logger)
        self._succeeded_table = DataclassTable(dataclass_type=StreamHandlerRegistryLineSucceeded,
                                                  filepath=succeeded_filepath,logger=self.logger)

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

    def _add_or_modify_in_progress_entry(self,dfc,new_status) :
        filename, rel_filepath = self._get_name_and_rel_filepath_for_data_file_chunk(dfc)
        existing_entry_addr = self._get_in_progress_address_for_rel_filepath(rel_filepath)
        if existing_entry_addr is None :
            new_entry = StreamHandlerRegistryLineInProgress(filename,
                                                              rel_filepath,
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

    def _get_name_and_rel_filepath_for_data_file_chunk(self,dfc) :
        filename = dfc.filename
        rel_filepath = f'{dfc.subdir_str}/{filename}' if dfc.subdir_str!='' else filename
        return filename,rel_filepath

    def _get_in_progress_address_for_rel_filepath(self,rel_filepath) :
        existing_obj_addresses = self._in_progress_table.obj_addresses_by_key_attr('rel_filepath')
        if rel_filepath not in existing_obj_addresses :
            return None
        if len(existing_obj_addresses[rel_filepath])!=1 :
            errmsg = f'ERROR: found more than one {self.__class__.__name__} entry for relative filepath '
            errmsg+= f'{rel_filepath} in file at {self._in_progress_table.filepath}'
            self.logger.error(errmsg,ValueError)
        return existing_obj_addresses[rel_filepath][0]

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
        in_progress_filepath = dirpath / f'files_consumed_from_{topic_name}_by_{consumer_group_id}.csv'
        succeeded_filepath = dirpath / f'files_successfully_processed_from_{topic_name}_by_{consumer_group_id}.csv'
        super().__init__(in_progress_filepath,succeeded_filepath,*args,**kwargs)

    def register_file_successfully_processed(self,dfc) :
        """
        Add/update a line in the table to show that a file has successfully been processed
        """
        filename, rel_filepath = self._get_name_and_rel_filepath_for_data_file_chunk(dfc)
        existing_entry_addr = self._get_in_progress_address_for_rel_filepath(rel_filepath)
        if existing_entry_addr is not None :
            attrs = self._in_progress_table.get_entry_attrs(existing_entry_addr)
            new_entry = StreamHandlerRegistryLineSucceeded(attrs['filename'],
                                                           attrs['rel_filepath'],
                                                           attrs['n_chunks'],
                                                           attrs['first_message'],
                                                           datetime.datetime.now())
        else :
            new_entry = StreamHandlerRegistryLineSucceeded(filename,
                                                           rel_filepath,
                                                           dfc.n_total_chunks,
                                                           datetime.datetime.now(),
                                                           datetime.datetime.now())
        self._succeeded_table.add_entries(new_entry)
        self._succeeded_table.dump_to_file()
        if existing_entry_addr is not None :
            self._in_progress_table.remove_entries(existing_entry_addr)
            self._in_progress_table.dump_to_file()

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
        in_progress_filepath = dirpath / f'files_consumed_from_{consumer_topic_name}_by_{consumer_group_id}.csv'
        succeeded_filepath = dirpath / f'files_with_results_produced_to_{producer_topic_name}.csv'
        super().__init__(in_progress_filepath,succeeded_filepath,*args,**kwargs)

    def register_file_results_produced(self,filename,rel_filepath,n_total_chunks) :
        """
        Add/update a line in the table to show that a file has successfully been processed
        """
        existing_entry_addr = self._get_in_progress_address_for_rel_filepath(rel_filepath)
        if existing_entry_addr is not None :
            attrs = self._in_progress_table.get_entry_attrs(existing_entry_addr)
            new_entry = StreamHandlerRegistryLineSucceeded(attrs['filename'],
                                                           attrs['rel_filepath'],
                                                           attrs['n_chunks'],
                                                           attrs['first_message'],
                                                           datetime.datetime.now())
        else :
            new_entry = StreamHandlerRegistryLineSucceeded(filename,
                                                           rel_filepath,
                                                           n_total_chunks,
                                                           datetime.datetime.now(),
                                                           datetime.datetime.now())
        self._succeeded_table.add_entries(new_entry)
        self._succeeded_table.dump_to_file()
        if existing_entry_addr is not None :
            self._in_progress_table.remove_entries(existing_entry_addr)
            self._in_progress_table.dump_to_file()

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
