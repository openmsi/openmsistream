#imports
import datetime
from kafkacrypto.message import KafkaCryptoMessage
from ..shared.runnable import Runnable
from .config import DATA_FILE_HANDLING_CONST, RUN_OPT_CONST
from .utilities import get_encrypted_message_key_and_value_filenames
from .download_data_file import DownloadDataFileToDisk
from .data_file_directory import DataFileDirectory
from .data_file_chunk_processor import DataFileChunkProcessor

class DataFileDownloadDirectory(DataFileDirectory,DataFileChunkProcessor,Runnable) :
    """
    Class representing a directory into which files are being reconstructed
    """

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,*args,datafile_type=DownloadDataFileToDisk,**kwargs) :
        """
        datafile_type = the type of datafile that the consumed messages should be assumed to represent
        In this class datafile_type should be something that extends DownloadDataFileToDisk
        """    
        super().__init__(*args,datafile_type=datafile_type,**kwargs)
        if not issubclass(self.datafile_type,DownloadDataFileToDisk) :
            errmsg = 'ERROR: DataFileDownloadDirectory requires a datafile_type that is a subclass of '
            errmsg+= f'DownloadDataFileToDisk but {self.datafile_type} was given!'
            self.logger.error(errmsg,ValueError)
        self.__encrypted_messages_subdir = self.dirpath/'ENCRYPTED_MESSAGES'

    def reconstruct(self) :
        """
        Consumes messages and writes their data to disk using several parallel threads to reconstruct the files 
        to which they correspond. Runs until the user inputs a command to shut it down. Returns the total number 
        of messages consumed, as well as the number of files whose reconstruction was completed during the run. 
        """
        msg = f'Will reconstruct files from messages in the {self.topic_name} topic using {self.n_threads} '
        msg+= f'thread{"s" if self.n_threads!=1 else ""}'
        self.logger.info(msg)
        self.run()
        return self.n_msgs_read, self.n_msgs_processed, self.completely_processed_filepaths

    #################### PRIVATE HELPER FUNCTIONS ####################

    def _process_message(self, lock, msg):
        retval = super()._process_message(lock,msg,self.dirpath,self.logger)
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
            with open(key_fp,'wb') as f :
                f.write(bytes(retval.key))
            with open(value_fp,'wb') as f :
                f.write(bytes(retval.value))
            return False #because the message wasn't processed successfully
        if retval==True :
            return retval
        #get the DataFileChunk from the message value
        try :
            dfc = msg.value() #from a regular Kafka Consumer
        except :
            dfc = msg.value #from KafkaCrypto
        #If the file was successfully reconstructed, return True
        if retval==DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE :
            infomsg = f'File {self.files_in_progress_by_path[dfc.filepath].full_filepath.relative_to(dfc.rootdir)} '
            infomsg+= 'successfully reconstructed from stream'
            self.logger.info(infomsg)
            self.completely_processed_filepaths.append(dfc.filepath)
            with lock :
                del self.files_in_progress_by_path[dfc.filepath]
                del self.locks_by_fp[dfc.filepath]
            return True
        #If the file hash was mismatched after reconstruction, return False
        elif retval==DATA_FILE_HANDLING_CONST.FILE_HASH_MISMATCH_CODE :
            warnmsg = f'WARNING: hashes for file {self.files_in_progress_by_path[dfc.filepath].filename} not matched '
            warnmsg+= 'after reconstruction! All data have been written to disk, but not as they were uploaded.'
            self.logger.warning(warnmsg)
            with lock :
                del self.files_in_progress_by_path[dfc.filepath]
                del self.locks_by_fp[dfc.filepath]
            return False
        else :
            self.logger.error(f'ERROR: unrecognized add_chunk return value ({retval})!',NotImplementedError)
            return False

    def _on_check(self) :
        msg = f'{self.n_msgs_read} messages read, {self.n_msgs_processed} messages processed, '
        msg+= f'{len(self.completely_processed_filepaths)} files completely reconstructed so far'
        self.logger.debug(msg)
        if len(self.files_in_progress_by_path)>0 or len(self.completely_processed_filepaths)>0 :
            self.logger.debug(self.progress_msg)

    #################### CLASS METHODS ####################

    @classmethod
    def get_command_line_arguments(cls) :
        superargs,superkwargs = super().get_command_line_arguments()
        args = [*superargs,'output_dir','config','topic_name','update_seconds','consumer_group_ID']
        kwargs = {**superkwargs,'n_threads':RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS}
        return args,kwargs

    @classmethod
    def run_from_command_line(cls,args=None) :
        """
        Run the download directory right from the command line
        """
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        #make the download directory
        reconstructor_directory = cls(args.output_dir,args.config,args.topic_name,
                                      n_threads=args.n_threads,
                                      consumer_group_ID=args.consumer_group_ID,
                                      update_secs=args.update_seconds,
                                     )
        #start the reconstructor running
        run_start = datetime.datetime.now()
        reconstructor_directory.logger.info(f'Listening for files to reconstruct in {args.output_dir}')
        n_read,n_processed,complete_filenames = reconstructor_directory.reconstruct()
        reconstructor_directory.close()
        run_stop = datetime.datetime.now()
        #shut down when that function returns
        reconstructor_directory.logger.info(f'File reconstructor writing to {args.output_dir} shut down')
        msg = f'{n_read} total messages were consumed'
        if len(complete_filenames)>0 :
            msg+=f', {n_processed} messages were successfully processed,'
            msg+=f' and the following {len(complete_filenames)} file'
            msg+=' was' if len(complete_filenames)==1 else 's were'
            msg+=' successfully reconstructed'
        else :
            msg+=f' and {n_processed} messages were successfully processed'
        msg+=f' from {run_start} to {run_stop}'
        for fn in complete_filenames :
            msg+=f'\n\t{fn}'
        reconstructor_directory.logger.info(msg)

#################### MAIN METHOD TO RUN FROM COMMAND LINE ####################

def main(args=None) :
    DataFileDownloadDirectory.run_from_command_line(args)

if __name__=='__main__' :
    main()
