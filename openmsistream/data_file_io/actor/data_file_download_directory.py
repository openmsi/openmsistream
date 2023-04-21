"""
Write DataFileChunks directly to disk as they are consumed from topics.
Preserve subdirectory structure if applicable
"""

#imports
import datetime, warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from kafkacrypto.message import KafkaCryptoMessage
from ...utilities import Runnable
from ..config import DATA_FILE_HANDLING_CONST, RUN_OPT_CONST
from ..utilities import get_encrypted_message_key_and_value_filenames
from .. import DataFileDirectory, DownloadDataFileToDisk
from .data_file_chunk_handlers import DataFileChunkProcessor

class DataFileDownloadDirectory(DataFileDirectory,DataFileChunkProcessor,Runnable) :
    """
    Class representing a directory into which files are being reconstructed.

    :param dirpath: Path to the directory where reconstructed files should be saved
    :type dirpath: :class:`pathlib.Path`
    :param config_path: Path to the config file to use in defining the Broker connection and Consumers
    :type config_path: :class:`pathlib.Path`
    :param topic_name: Name of the topic to which the Consumers should be subscribed
    :type topic_name: str
    :param datafile_type: the type of data file that recognized files should be reconstructed as
        (must be a subclass of :class:`~.data_file_io.DownloadDataFileToDisk`)
    :type datafile_type: :class:`~.data_file_io.DownloadDataFileToDisk`, optional

    :raises ValueError: if `datafile_type` is not a subclass of
        :class:`~.data_file_io.DownloadDataFileToDisk`
    """

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,dirpath,config_path,topic_name,datafile_type=DownloadDataFileToDisk,**kwargs) :
        """
        datafile_type = the type of datafile that the consumed messages should be assumed to represent
        In this class datafile_type should be something that extends DownloadDataFileToDisk
        """
        super().__init__(dirpath,config_path,topic_name,datafile_type=datafile_type,**kwargs)
        if not issubclass(self.datafile_type,DownloadDataFileToDisk) :
            errmsg = 'ERROR: DataFileDownloadDirectory requires a datafile_type that is a subclass of '
            errmsg+= f'DownloadDataFileToDisk but {self.datafile_type} was given!'
            self.logger.error(errmsg,exc_type=ValueError)
        self.__encrypted_messages_subdir = self.dirpath/'ENCRYPTED_MESSAGES'

    def reconstruct(self) :
        """
        Consumes messages and writes their data to disk using several parallel threads to reconstruct the files
        to which they correspond. Runs until the user inputs a command to shut it down.

        :return: the total number of messages consumed
        :rtype: int
        :return: the total number of message processed (written to disk)
        :rtype: int
        :return: the total number of completely reconstructed files
        :rtype: int
        :return: paths of up to 50 most recent files whose reconstruction was completed during the run
        :rtype: list
        """
        msg = f'Will reconstruct files from messages in the {self.topic_name} topic using {self.n_threads} '
        msg+= f'thread{"s" if self.n_threads!=1 else ""}'
        self.logger.info(msg)
        self.run()
        return (
            self.n_msgs_read,
            self.n_msgs_processed,
            self.n_processed_files,
            self.recent_processed_filepaths
        )

    #################### PRIVATE HELPER FUNCTIONS ####################

    def _process_message(self, lock, msg, rootdir_to_set=None):
        retval = super()._process_message(lock,msg,self.dirpath if rootdir_to_set is None else rootdir_to_set)
        #if the message was returned because it couldn't be decrypted, write it to the encrypted messages directory
        if ( hasattr(retval,'key') and hasattr(retval,'value') and
             (isinstance(retval.key,KafkaCryptoMessage) or isinstance(retval.value,KafkaCryptoMessage)) ) :
            if not self.__encrypted_messages_subdir.is_dir() :
                self.__encrypted_messages_subdir.mkdir(parents=True)
            key_fn, value_fn = get_encrypted_message_key_and_value_filenames(retval,self.topic_name)
            key_fp = self.__encrypted_messages_subdir/key_fn
            value_fp = self.__encrypted_messages_subdir/value_fn
            warnmsg = 'WARNING: encountered a message that failed to be decrypted. Key bytes will be written to '
            warnmsg+= f'{key_fp.relative_to(self.dirpath)} and value bytes will be written to '
            warnmsg+= f'{value_fp.relative_to(self.dirpath)}'
            self.logger.warning(warnmsg)
            with open(key_fp,'wb') as fp :
                fp.write(bytes(retval.key))
            with open(value_fp,'wb') as fp :
                fp.write(bytes(retval.value))
            return False #because the message wasn't processed successfully
        if retval is True :
            return retval
        #get the DataFileChunk from the message value
        try :
            dfc = msg.value() #from a regular Kafka Consumer
        except TypeError :
            dfc = msg.value #from KafkaCrypto
        #If the file was successfully reconstructed, return True
        if retval==DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE :
            self.logger.debug(f'File {dfc.relative_filepath} successfully reconstructed from stream')
            with lock :
                self.recent_processed_filepaths.append(dfc.relative_filepath)
                while len(self.recent_processed_filepaths)>self.N_RECENT_FILES :
                    _ = self.recent_processed_filepaths.pop(0)
                self.n_processed_files+=1
                del self.files_in_progress_by_path[dfc.relative_filepath]
                del self.locks_by_fp[dfc.relative_filepath]
            return True
        #If the file hash was mismatched after reconstruction, return False
        if retval==DATA_FILE_HANDLING_CONST.FILE_HASH_MISMATCH_CODE :
            warnmsg = f'WARNING: hashes for file {dfc.relative_filepath} not matched after reconstruction! '
            warnmsg+= 'All data have been written to disk, but not as they were uploaded.'
            self.logger.warning(warnmsg)
            with lock :
                del self.files_in_progress_by_path[dfc.relative_filepath]
                del self.locks_by_fp[dfc.relative_filepath]
            return False
        #if this is reached the return code was unrecognized
        self.logger.error(f'ERROR: unrecognized add_chunk return value ({retval})!',exc_type=NotImplementedError)
        return False

    def _on_check(self) :
        msg = f'{self.n_msgs_read} messages read, {self.n_msgs_processed} messages processed, '
        msg+= f'{self.n_processed_files} files completely reconstructed so far'
        self.logger.info(msg)
        if len(self.files_in_progress_by_path)>0 or len(self.recent_processed_filepaths)>0 :
            self.logger.debug(self.progress_msg)

    #################### CLASS METHODS ####################

    @classmethod
    def get_command_line_arguments(cls) :
        superargs,superkwargs = super().get_command_line_arguments()
        args = [*superargs,'output_dir','config','topic_name','update_seconds','consumer_group_id']
        kwargs = {**superkwargs,'n_threads':RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS}
        return args,kwargs

    @classmethod
    def run_from_command_line(cls,args=None) :
        """
        Run a :class:`~DataFileDownloadDirectory` directly from the command line

        Calls :func:`~reconstruct` on a :class:`~DataFileDownloadDirectory` defined by
        command line (or given) arguments

        :param args: the list of arguments to send to the parser instead of getting them from sys.argv
        :type args: list, optional
        """
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        #make the download directory
        reconstructor_directory = cls(args.output_dir,args.config,args.topic_name,
                                      n_threads=args.n_threads,
                                      consumer_group_id=args.consumer_group_id,
                                      update_secs=args.update_seconds,
                                      streamlevel=args.logger_stream_level,filelevel=args.logger_file_level,
                                     )
        #start the reconstructor running
        run_start = datetime.datetime.now()
        reconstructor_directory.logger.info(f'Listening for files to reconstruct in {args.output_dir}')
        n_read,n_processed,n_complete_files,complete_filepaths = reconstructor_directory.reconstruct()
        reconstructor_directory.close()
        run_stop = datetime.datetime.now()
        #shut down when that function returns
        reconstructor_directory.logger.info(f'File reconstructor writing to {args.output_dir} shut down')
        msg = f'{n_read} total messages were consumed'
        if len(complete_filepaths)>0 :
            msg+=f', {n_processed} messages were successfully processed,'
            msg+=f' and {n_complete_files} file'
            msg+=' was' if n_complete_files==1 else 's were'
            msg+=' successfully reconstructed'
        else :
            msg+=f' and {n_processed} messages were successfully processed'
        msg+=f' from {run_start} to {run_stop}\n'
        msg+=f'Most recent completed files (up to {cls.N_RECENT_FILES}):'
        msg+='\n\t'.join(complete_filepaths)
        reconstructor_directory.logger.info(msg)

def main(args=None) :
    """
    Main method to run from command line
    """
    DataFileDownloadDirectory.run_from_command_line(args)

if __name__=='__main__' :
    main()
