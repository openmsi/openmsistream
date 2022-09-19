"""
Base class for extracting metadata dictionaries from files in topics
and producing those dictionaries as JSON-formatted strings to another topic
"""

#imports
from abc import ABC, abstractmethod
from ..data_file_io.actor.data_file_stream_reproducer import DataFileStreamReproducer
from .metadata_json_message import MetadataJSONMessage

class MetadataJSONReproducer(DataFileStreamReproducer,ABC) :
    """
    A class to extend for processing a stream of DataFileChunks, extracting metadata from
    fully-reconstructed files, and producing those metadata fields as JSON to a different topic.

    This is a base class that cannot be instantiated on its own.

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
        (must be a subclass of :class:`~.data_file_io.DownloadDataFileToMemory`)
    :type datafile_type: :class:`~.data_file_io.DownloadDataFileToMemory`, optional
    :param n_producer_threads: the number of producers to run. The total number of producer/consumer threads
        started is `max(n_consumer_threads,n_producer_threads)`.
    :type n_producer_threads: int, optional
    :param n_consumer_threads: the number of consumers to run. The total number of producer/consumer threads
        started is `max(n_consumer_threads,n_producer_threads)`.
    :type n_consumer_threads: int, optional
    :param consumer_group_id: the group ID under which each consumer should be created
    :type consumer_group_id: str, optional

    :raises ValueError: if `datafile_type` is not a subclass of
        :class:`~.data_file_io.DownloadDataFileToMemory`
    """

    def __init__(self,config_file,consumer_topic_name,producer_topic_name,**kwargs) :
        """
        Constructor method signature duplicated above to display in Sphinx docs
        """
        super().__init__(config_file,consumer_topic_name,producer_topic_name,**kwargs)

    def _get_processing_result_message_for_file(self,datafile,lock) :
        try :
            json_content = self._get_metadata_dict_for_file(datafile)
        except Exception as exc :
            errmsg = f'ERROR: failed to extract JSON metadata from {datafile.full_filepath}! '
            errmsg+= 'Error will be logged below, but not reraised.'
            self.logger.error(errmsg,exc_obj=exc,reraise=False)
            return None
        if not isinstance(json_content,dict) :
            errmsg = f'ERROR: JSON content found for {datafile.full_filepath} is of type {type(json_content)}, '
            errmsg+= 'not a dictionary! This file will be skipped.'
            self.logger.error(errmsg)
            return None
        return MetadataJSONMessage(datafile,json_content=json_content)

    @abstractmethod
    def _get_metadata_dict_for_file(self,datafile) :
        """
        Given a :class:`~.data_file_io.DownloadDataFileToMemory`, extract and return its metadata as a
        dictionary. The returned dictionary will be serialized to JSON and produced to the destination topic.

        This function can raise errors to be logged if metadata extraction doesn't proceed as expected.

        Not implemented in base class

        :param datafile: A :class:`~.data_file_io.DownloadDataFileToMemory` object that has received
            all of its messages from the topic
        :type datafile: :class:`~.data_file_io.DownloadDataFileToMemory`

        :return: metadata keys/values
        :rtype: dict
        """
        raise NotImplementedError

    def _on_shutdown(self) :
        self.logger.info('Will quit after all currently enqueued messages are received.')
        self.logger.info(self.progress_msg)
        super()._on_shutdown()

    @classmethod
    def run_from_command_line(cls,args=None) :
        """
        Run :func:`~produce_processing_results_for_files_as_read` to continually
        extract metadata from consumed files and produce them as json contents to another topic
        """
        # make the argument parser
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        xrd_csv_metadata_reproducer = cls(args.config,args.consumer_topic_name,args.producer_topic_name,
                                          consumer_group_id=args.consumer_group_id,
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
        msg = f'{n_r} total message{"s were" if n_r!=1 else " was"} consumed'
        msg+=f', {n_p} message{"s were" if n_p!=1 else " was"} successfully processed'
        msg+=f', {len(f_r_fns)} file{"s were" if len(f_r_fns)!=1 else " was"} fully-read'
        if len(m_p_fns)>0 :
            msg+=f', and the following {len(m_p_fns)} file'
            msg+=' had' if len(m_p_fns)==1 else 's had'
            msg+=f' json metadata produced to the {args.producer_topic_name} topic:'
            for fn in m_p_fns :
                msg+=f'\n\t{fn}'
        xrd_csv_metadata_reproducer.logger.info(msg)
