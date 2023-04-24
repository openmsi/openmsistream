"""Constants used for internally processing file chunks and for some default parameter values"""

# imports
import re


class DataFileHandlingConstants:
    """
    Constants for internally handling DataFileChunks
    """

    CHUNK_ALREADY_WRITTEN_CODE = 10
    FILE_HASH_MISMATCH_CODE = -1
    FILE_SUCCESSFULLY_RECONSTRUCTED_CODE = 3
    FILE_IN_PROGRESS = 2


DATA_FILE_HANDLING_CONST = DataFileHandlingConstants()


class RunOptionConstants:
    """
    Some default command line options/function kwargs
    """

    # name of the config file that will be used by default
    DEFAULT_CONFIG_FILE = "test"
    # name of the topic to produce to by default
    DEFAULT_TOPIC_NAME = "test"
    # name of the config file used in "real" production
    PRODUCTION_CONFIG_FILE = "prod"
    # default number of threads to use when uploading a file
    N_DEFAULT_UPLOAD_THREADS = 2
    # default number of threads to use when downloading chunks of a file
    N_DEFAULT_DOWNLOAD_THREADS = 2

    # matches everything except something starting with a '.' or ending in '.log'
    DEFAULT_UPLOAD_REGEX = re.compile(r"^((?!(\.|.*.log))).*$")

    # default size in bytes of each file upload chunk
    DEFAULT_CHUNK_SIZE = 524288
    # default maximum size (in MB) of the internal upload Queue
    DEFAULT_MAX_UPLOAD_QUEUE_MEGABYTES = 500


RUN_OPT_CONST = RunOptionConstants()
