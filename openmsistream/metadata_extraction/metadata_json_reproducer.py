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

    def _get_processing_result_message_for_file(self,datafile,lock) :
        try :
            json_content = self._get_json_metadata_for_file(datafile)
        except Exception as e :
            errmsg = f'ERROR: failed to extract JSON metadata from {datafile.full_filepath}! '
            errmsg+= 'Error will be logged below, but not reraised.'
            self.logger.error(errmsg,exc_obj=e,reraise=False)
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
        Given a :class:`~DownloadDataFileToMemory`, extract and return its metadata as a dictionary. 
        The returned dictionary will be serialized to JSON and produced to the destination topic.

        This function can raise errors to be logged if metadata extraction doesn't proceed as expected.

        Not implemented in base class

        :param datafile: A :class:`~DownloadDataFileToMemory` object that has received 
            all of its messages from the topic
        :type datafile: :class:`~DownloadDataFileToMemory`

        :return: metadata keys/values
        :rtype: dict
        """
        raise NotImplementedError