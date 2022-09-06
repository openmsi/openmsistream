#imports
from ...running.runnable import Runnable
from ..metadata_json_reproducer import MetadataJSONReproducer

class XRDCSVMetadataReproducer(MetadataJSONReproducer,Runnable) :
    """
    An example class showing how to use a MetadataJSONReproducer to extract metadata from the header 
    of a .csv data file from an XRD measurement (read as chunks from a topic) and produce that metadata 
    as JSON to another topic

    :param config_path: Path to the config file to use in defining the Broker connection, Consumers, and Producers
    :type config_path: :class:`pathlib.Path`
    :param consumer_topic_name: Name of the topic to which the Consumer(s) should be subscribed
    :type consumer_topic_name: str
    :param producer_topic_name: Name of the topic to which the Producer(s) should produce the processing results
    :type producer_topic_name: str
    :param output_dir: Path to the directory where the log and csv registry files should be kept (if None a default
        will be created in the current directory)
    :type output_dir: :class:`pathlib.Path`, optional
    :param datafile_type: the type of data file that recognized files should be reconstructed as 
        (must be a subclass of :class:`~DownloadDataFileToMemory`)
    :type datafile_type: :class:`~DownloadDataFileToMemory`, optional
    :param n_producer_threads: the number of producers to run. The total number of producer/consumer threads 
        started is `max(n_consumer_threads,n_producer_threads)`.
    :type n_producer_threads: int, optional
    :param n_consumer_threads: the number of consumers to run. The total number of producer/consumer threads 
        started is `max(n_consumer_threads,n_producer_threads)`.
    :type n_consumer_threads: int, optional
    :param consumer_group_ID: the group ID under which each consumer should be created
    :type consumer_group_ID: str, optional

    :raises ValueError: if `datafile_type` is not a subclass of :class:`~DownloadDataFileToMemory`
    """

    def __init__(self,config_file,consumer_topic_name,producer_topic_name,**kwargs) :
        """
        Constructor method signature duplicated above to display in Sphinx docs
        """
        super().__init__(config_file,consumer_topic_name,producer_topic_name,**kwargs)

    @classmethod
    def run_from_command_line(cls,args=None) :
        """
        Run :func:`~DataFileStreamReproducer.produce_processing_results_for_files_as_read` to continually 
        extract metadata from consumed .csv files and produce them as json contents to another topic
        """
        # make the argument parser
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        xrd_csv_metadata_reproducer = cls(args.config,args.consumer_topic_name,args.producer_topic_name,
                                          consumer_group_ID=args.consumer_group_ID,
                                          n_consumer_threads=args.n_consumer_threads,
                                          n_producer_threads=args.n_producer_threads,
                                          output_dir=args.output_dir,
                                          update_secs=args.update_seconds,
                                          )
        # cls.bucket_name = args.bucket_name
        msg = f'Listening to the {args.consumer_topic_name} topic for XRD CSV files to send their metadata to the '
        msg+= f'{args.producer_topic_name} topic....'
        xrd_csv_metadata_reproducer.logger.info(msg)
        n_r,n_p,f_r_fns,m_p_fns = xrd_csv_metadata_reproducer.produce_processing_results_for_files_as_read()
        xrd_csv_metadata_reproducer.close()
        msg = f'{n_r} total messages were consumed'
        msg+=f', {n_p} messages were successfully processed'
        msg+=f', {len(f_r_fns)} file{"s" if len(f_r_fns)!=1 else ""} were fully-read'
        if len(m_p_fns)>0 :
            msg+=f', and the following {len(m_p_fns)} file'
            msg+=' had' if len(m_p_fns)==1 else 's had'
            msg+=f' json metadata produced to the {args.producer_topic_name} topic:'
            for fn in m_p_fns :
                msg+=f'\n\t{fn}'
        xrd_csv_metadata_reproducer.logger.info(msg)

    def _get_metadata_dict_for_file(self,datafile) :
        print('oh hi :3c')

def main(args=None) :
    XRDCSVMetadataReproducer.run_from_command_line(args)

if __name__=='__main__' :
    main()