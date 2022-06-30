from ..running.runnable import Runnable
from ..data_file_io.config import RUN_OPT_CONST
from ..data_file_io.data_file_stream_processor import DataFileStreamProcessor
from .config_file_parser import S3ConfigFileParser
from .s3_data_transfer import S3DataTransfer

class S3TransferStreamProcessor(DataFileStreamProcessor, Runnable) :

    def __init__(self, bucket_name, config_path, topic_name, **otherkwargs) :
        super().__init__(config_path, topic_name, **otherkwargs)
        parser = S3ConfigFileParser(config_path,logger=self.logger)
        self.__s3_config = parser.s3_configs
        self.__s3_config['bucket_name'] = bucket_name
        self.bucket_name = bucket_name
        self.s3d = S3DataTransfer(self.__s3_config,logger=self.logger)

    def make_stream(self):
        return self.process_files_as_read()

    def stream_processor_run(self) :
        self.make_stream()

    def close_session_client(self) :
        self.s3d.close_session()

    def _process_downloaded_data_file(self, datafile, lock):
        self.s3d.transfer_object_stream(self.topic_name, datafile)
        if self.s3d.compare_consumer_datafile_with_s3_object_stream(self.topic_name, self.bucket_name, datafile):
            file_name = str(datafile.filename)
            sub_dir = datafile.subdir_str
            object_key = self.topic_name 
            if sub_dir is not None :
                object_key+= '/' + sub_dir 
            object_key+= '/' + file_name
            self.logger.info(object_key + ' matched with consumer datafile')
            # self.s3d.delete_object_from_bucket(self.bucket_name, object_key)
        return None

    @classmethod
    def get_command_line_arguments(cls):
        superargs, superkwargs = super().get_command_line_arguments()
        args = [*superargs, 'bucket_name',
                'update_seconds', 'logger_file', 'config', 'topic_name', 'consumer_group_ID']
        kwargs = {**superkwargs,
                  'n_threads': RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS, 
                  }
        return args, kwargs

    @classmethod
    def run_from_command_line(cls, args=None):
        # make the argument parser
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        s3_stream_proc = cls(args.bucket_name,
                             args.config, args.topic_name,
                             n_threads=args.n_threads,
                             update_secs=args.update_seconds,
                             consumer_group_ID=args.consumer_group_ID,
                             logger_file=args.logger_file)
        # cls.bucket_name = args.bucket_name
        msg = f'Listening to the {args.topic_name} topic for files to add to the {args.bucket_name} bucket....'
        s3_stream_proc.logger.info(msg)
        n_read,n_processed,complete_filenames = s3_stream_proc.make_stream()
        s3_stream_proc.close()
        msg = f'{n_read} total messages were consumed'
        if len(complete_filenames)>0 :
            msg+=f', {n_processed} messages were successfully processed,'
            msg+=f' and the following {len(complete_filenames)} file'
            msg+=' was' if len(complete_filenames)==1 else 's were'
            msg+=f' successfully transferred to the {args.bucket_name} bucket'
            for fn in complete_filenames :
                msg+=f'\n\t{fn}'
        else :
            msg+=f' and {n_processed} messages were successfully processed'
        s3_stream_proc.logger.info(msg)

    def _on_check(self) :
        msg = f'{self.n_msgs_read} messages read, {self.n_msgs_processed} messages processed, '
        msg+= f'{len(self.completely_processed_filepaths)} files transferred so far'
        self.logger.debug(msg)
        if len(self.files_in_progress_by_path)>0 or len(self.completely_processed_filepaths)>0 :
            self.logger.debug(self.progress_msg)

#################### MAIN METHOD TO RUN FROM COMMAND LINE ####################

def main(args=None):
    S3TransferStreamProcessor.run_from_command_line(args)

if __name__ == '__main__':
    main()
