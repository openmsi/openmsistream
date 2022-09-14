"""Constants used for internally processing file chunks and for some default parameter values"""

#imports
import re

class DataFileHandlingConstants :
    """
    Constants for internally handling DataFileChunks
    """

    CHUNK_ALREADY_WRITTEN_CODE = 10 # code indicating that a particular chunk has already been written
    FILE_HASH_MISMATCH_CODE = -1 # code indicating that a file's hashes didn't match
    FILE_SUCCESSFULLY_RECONSTRUCTED_CODE = 3 # code indicating that a file was successfully fully reconstructed
    FILE_IN_PROGRESS = 2 # code indicating that a file is in the process of being reconstructed

DATA_FILE_HANDLING_CONST=DataFileHandlingConstants()

class RunOptionConstants :
    """
    Some default command line options/function kwargs
    """

    DEFAULT_CONFIG_FILE = 'test' # name of the config file that will be used by default
    DEFAULT_TOPIC_NAME = 'test' # name of the topic to produce to by default
    PRODUCTION_CONFIG_FILE = 'prod' # name of the config file used in "real" production
    N_DEFAULT_UPLOAD_THREADS = 2 # default number of threads to use when uploading a file
    N_DEFAULT_DOWNLOAD_THREADS = 2 # default number of threads to use when downloading chunks of a file

    #matches everything except something starting with a '.' or ending in '.log'
    DEFAULT_UPLOAD_REGEX = re.compile(r'^((?!(\.|.*.log))).*$')

    DEFAULT_CHUNK_SIZE = 16384 # default size in bytes of each file upload chunk
    DEFAULT_MAX_UPLOAD_QUEUE_SIZE = 3000 # default maximum number of items allowed in the upload Queue at once

RUN_OPT_CONST = RunOptionConstants()
