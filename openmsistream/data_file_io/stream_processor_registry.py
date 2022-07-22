#imports
import datetime, re
from dataclasses import dataclass
from ..utilities import DataclassTable, LogOwner
from .utilities import get_message_prepend

@dataclass
class StreamProcessorRegistryLineInProgress :
    filename : str #the name of the file
    rel_filepath : str #the (posix) path to the file, relative to its root directory
    status : str #the status of the file, either "in_progress", "failed", or "mismatched_hash"
    n_chunks : int #the total number of chunks in the file
    first_message : datetime.datetime #timestamp of when the first message for the file was consumed
    most_recent_message : datetime.datetime #timestamp of when the most recent chunk from the file was consumed

@dataclass
class StreamProcessorRegistryLineSucceeded :
    filename : str #the name of the file
    rel_filepath : str #the (posix) path to the file, relative to its root directory
    n_chunks : int #the total number of chunks in the file
    first_message : datetime.datetime #timestamp of when the most recent chunk from the file was consumed
    processed_at : datetime.datetime #timestamp of when the file was processed

class StreamProcessorRegistry(LogOwner) :
    """
    A class to keep track of the status of files read during stream processing
    """

    IN_PROGRESS = 'in_progress'
    FAILED = 'failed'
    MISMATCHED_HASH = 'mismatched_hash'

    @property
    def in_progress_table(self) :
        return self.__in_progress_table

    @property
    def succeeded_table(self) :
        return self.__succeeded_table

    @property
    def filepaths_to_rerun(self) :
        """
        A list of all the filepaths that have not yet been successfully processed
        """
        to_rerun = []
        for addr in self.__in_progress_table.obj_addresses :
            to_rerun.append(self.__in_progress_table.get_entry_attrs(addr,'rel_filepath'))
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
        The number of files in the registry that don't have the "success" status
        """
        n_files = 0
        for fp in self.filepaths_to_rerun :
            n_files+=1
        return n_files

    def __init__(self,dirpath,topic_name,consumer_group_ID,*args,**kwargs) :
        """
        dirpath    = path to the directory that should contain the csv file
        topic_name = the name of the topic that will be produced to (used in the filename)
        """
        super().__init__(*args,**kwargs)
        in_progress_filepath = dirpath / f'files_consumed_from_{topic_name}_by_{consumer_group_ID}.csv'
        self.__in_progress_table = DataclassTable(dataclass_type=StreamProcessorRegistryLineInProgress,
                                                  filepath=in_progress_filepath,logger=self.logger)
        succeeded_filepath = dirpath / f'files_successfully_processed_from_{topic_name}_by_{consumer_group_ID}.csv'
        self.__succeeded_table = DataclassTable(dataclass_type=StreamProcessorRegistryLineSucceeded,
                                                  filepath=succeeded_filepath,logger=self.logger)

    def register_file_in_progress(self,dfc) :
        """
        Add/update a line in the table showing that a file is in progress
        """
        self._add_or_modify_in_progress_entry(dfc,self.IN_PROGRESS)

    def register_file_successfully_processed(self,dfc) :
        """
        Add/update a line in the table to show that a file has successfully been processed
        """
        filename, rel_filepath = self._get_name_and_rel_filepath_for_data_file_chunk(dfc)
        existing_entry_addr = self._get_in_progress_address_for_rel_filepath(rel_filepath)
        if existing_entry_addr is not None :
            attrs = self.__in_progress_table.get_entry_attrs(existing_entry_addr)
            new_entry = StreamProcessorRegistryLineSucceeded(attrs['filename'],
                                                             attrs['rel_filepath'],
                                                             attrs['n_chunks'],
                                                             attrs['first_message'],
                                                             datetime.datetime.now())
        else :
            new_entry = StreamProcessorRegistryLineSucceeded(filename,
                                                             rel_filepath,
                                                             dfc.n_total_chunks,
                                                             datetime.datetime.now(),
                                                             datetime.datetime.now())
        self.__succeeded_table.add_entries(new_entry)
        self.__succeeded_table.dump_to_file()
        if existing_entry_addr is not None :
            self.__in_progress_table.remove_entries(existing_entry_addr)
            self.__in_progress_table.dump_to_file()

    def register_file_processing_failed(self,dfc) :
        """
        Add/update a line in the table to show that a file has failed to be processed
        """
        self._add_or_modify_in_progress_entry(dfc,self.FAILED)

    def register_file_mismatched_hash(self,dfc) :
        """
        Add/update a line in the table to show that a consumed file was mismatched with its original content hash 
        """
        self._add_or_modify_in_progress_entry(dfc,self.MISMATCHED_HASH)

    def _add_or_modify_in_progress_entry(self,dfc,new_status) :
        filename, rel_filepath = self._get_name_and_rel_filepath_for_data_file_chunk(dfc)
        existing_entry_addr = self._get_in_progress_address_for_rel_filepath(rel_filepath)
        if existing_entry_addr is None :
            new_entry = StreamProcessorRegistryLineInProgress(filename,
                                                              rel_filepath,
                                                              new_status,
                                                              dfc.n_total_chunks,
                                                              datetime.datetime.now(),
                                                              datetime.datetime.now())
            self.__in_progress_table.add_entries(new_entry)
            self.__in_progress_table.dump_to_file()
        else :
            self.__in_progress_table.set_entry_attrs(existing_entry_addr,
                                                    status=new_status,
                                                    most_recent_message=datetime.datetime.now())

    def _get_name_and_rel_filepath_for_data_file_chunk(self,dfc) :
        filename = dfc.filename
        rel_filepath = f'{dfc.subdir_str}/{filename}' if dfc.subdir_str!='' else filename
        return filename,rel_filepath
    
    def _get_in_progress_address_for_rel_filepath(self,rel_filepath) :
        existing_obj_addresses = self.__in_progress_table.obj_addresses_by_key_attr('rel_filepath')
        if rel_filepath not in existing_obj_addresses.keys() :
            return None
        elif len(existing_obj_addresses[rel_filepath])!=1 :
            errmsg = f'ERROR: found more than one {self.__class__.__name__} entry for relative filepath '
            errmsg+= f'{rel_filepath} in file at {self.__in_progress_table.filepath}'
            self.logger.error(errmsg,ValueError)
        else :
            return existing_obj_addresses[rel_filepath][0]
