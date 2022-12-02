"""Transfer contents of DataFiles read from chunks in a topic to an S3 bucket when complete files become available"""

#imports
from ..data_file_io.actor.data_file_stream_processor import DataFileStreamProcessor
from .config_file_parser import S3ConfigFileParser
from .s3_data_transfer import S3DataTransfer

class S3TransferStreamProcessor(DataFileStreamProcessor) :
    """
    A class to reconstruct data files read as messages from a topic, hold them in memory,
    and transfer them to an S3 bucket when all of their messages have been received

    :param bucket_name: Name of the S3 bucket into which reconstructed files should be transferred
    :type bucket_name: str
    :param config_path: Path to the config file to use in defining the Broker connection and Consumers
    :type config_path: :class:`pathlib.Path`
    :param topic_name: Name of the topic to which the Consumers should be subscribed
    :type topic_name: str
    """

    def __init__(self, bucket_name, config_path, topic_name, **kwargs) :
        super().__init__(config_path, topic_name, **kwargs)
        parser = S3ConfigFileParser(config_path,logger=self.logger)
        self.__s3_config = parser.s3_configs
        self.__s3_config['bucket_name'] = bucket_name
        self.bucket_name = bucket_name
        self.s3d = S3DataTransfer(self.__s3_config,logger=self.logger)

    def make_stream(self):
        """
        Runs :func:`~DataFileStreamProcessor.process_files_as_read` to reconstruct files in memory
        and transfer completed files to the S3 bucket. Runs until the user inputs a command to shut it down.

        :return: the total number of messages consumed
        :rtype: int
        :return: the total number of messages processed (registered in memory)
        :rtype: int
        :return: the paths of files successfully transferred to the S3 bucket during the run
        :rtype: list
        """
        return self.process_files_as_read()

    def _process_downloaded_data_file(self, datafile, lock):
        """
        Transfer a fully-reconstructed file to the S3 bucket and verify that its contents in the bucket
        match its original hash from disk. Logs a warning if the file hashes don't match.

        :param datafile: A :class:`~DownloadDataFileToMemory` object that has received
            all of its messages from the topic
        :type datafile: :class:`~DownloadDataFileToMemory`
        :param lock: Acquiring this :class:`threading.Lock` object would ensure that only one instance
            of :func:`~_process_downloaded_data_file` is running at once
        :type lock: :class:`threading.Lock`

        :return: None if processing was successful, a caught Exception otherwise
        """
        object_key = self.__get_datafile_object_key(datafile)
        try :
            self.s3d.transfer_object_stream(object_key, datafile)
        except Exception as exc :
            self.logger.error(f'ERROR: failed to transfer {datafile.filename} to the object store')
            return exc
        if self.s3d.compare_consumer_datafile_with_s3_object_stream(self.bucket_name, object_key, datafile):
            self.logger.debug(object_key + ' matched with consumer datafile')
            # self.s3d.delete_object_from_bucket(self.bucket_name, object_key)
        else :
            warnmsg = f'WARNING: {object_key} transferred to bucket but the file on the bucket does not match '
            warnmsg+= 'the file originally read from disk!'
            self.logger.warning(warnmsg)
        return None

    def __get_datafile_object_key(self,datafile) :
        file_name = str(datafile.filename)
        sub_dir = datafile.subdir_str
        object_key = self.topic_name
        if sub_dir!='' :
            object_key+= '/' + sub_dir
        object_key+= '/' + file_name
        return object_key

    @classmethod
    def get_command_line_arguments(cls):
        superargs, superkwargs = super().get_command_line_arguments()
        args = [*superargs, 'bucket_name']
        kwargs = {**superkwargs,'config':'test_s3_transfer'}
        return args, kwargs

    @classmethod
    def run_from_command_line(cls, args=None):
        """
        Run a :class:`~S3TransferStreamProcessor` directly from the command line

        Calls :func:`~make_stream` on a :class:`~S3TransferStreamProcessor` defined by
        command line (or given) arguments

        :param args: the list of arguments to send to the parser instead of getting them from sys.argv
        :type args: list, optional
        """
        # make the argument parser
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        s3_stream_proc = cls(args.bucket_name,
                             args.config, args.topic_name,
                             output_dir=args.output_dir,
                             n_threads=args.n_threads,
                             update_secs=args.update_seconds,
                             consumer_group_id=args.consumer_group_id,
                             streamlevel=args.logger_stream_level,filelevel=args.logger_file_level)
        # cls.bucket_name = args.bucket_name
        msg = f'Listening to the {args.topic_name} topic for files to add to the {args.bucket_name} bucket....'
        s3_stream_proc.logger.info(msg)
        n_read,n_processed,complete_filenames = s3_stream_proc.make_stream()
        s3_stream_proc.close()
        msg = f'{n_read} total messages were consumed, {n_processed} messages were successfully processed,'
        msg+= f'and {len(complete_filenames)} files were transferred to the {args.bucket_name} bucket'
        s3_stream_proc.logger.info(msg)
        if len(complete_filenames)>0 :
            msg =f'The following {len(complete_filenames)} file'
            msg+=' was' if len(complete_filenames)==1 else 's were'
            msg+=f' successfully transferred to the {args.bucket_name} bucket'
            for fn in complete_filenames :
                msg+=f'\n\t{fn}'
            s3_stream_proc.logger.debug(msg)

    def _on_check(self) :
        msg = f'{self.n_msgs_read} messages read, {self.n_msgs_processed} messages processed, '
        msg+= f'{len(self.completely_processed_filepaths)} files transferred so far'
        self.logger.info(msg)
        if len(self.files_in_progress_by_path)>0 or len(self.completely_processed_filepaths)>0 :
            self.logger.debug(self.progress_msg)

def main(args=None):
    """
    Main method to run from command line
    """
    S3TransferStreamProcessor.run_from_command_line(args)

if __name__ == '__main__':
    main()
