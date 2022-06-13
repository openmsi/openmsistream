#constants for data file upload/download/handling

class DataFileHandlingConstants :
    @property
    def CHUNK_ALREADY_WRITTEN_CODE(self) :
        return 10 # code indicating that a particular chunk has already been written
    @property
    def FILE_HASH_MISMATCH_CODE(self) :
        return -1 # code indicating that a file's hashes didn't match
    @property
    def FILE_SUCCESSFULLY_RECONSTRUCTED_CODE(self) :
        return 3  # code indicating that a file was successfully fully reconstructed
    @property
    def FILE_IN_PROGRESS(self) :
        return 2  # code indicating that a file is in the process of being reconstructed
    
DATA_FILE_HANDLING_CONST=DataFileHandlingConstants()

class RunOptionConstants :
    @property
    def DEFAULT_CONFIG_FILE(self) :
        return 'test' # name of the config file that will be used by default
    @property
    def DEFAULT_TOPIC_NAME(self) :
        return 'test' # name of the topic to produce to by default
    @property
    def PRODUCTION_CONFIG_FILE(self) :
        return 'prod' # name of the config file used in "real" production
    @property
    def N_DEFAULT_UPLOAD_THREADS(self) :
        return 2      # default number of threads to use when uploading a file
    @property
    def N_DEFAULT_DOWNLOAD_THREADS(self) :
        return 2      # default number of threads to use when downloading chunks of a file
    @property
    def DEFAULT_CHUNK_SIZE(self) :
        return 16384  # default size in bytes of each file upload chunk
    @property
    def DEFAULT_MAX_UPLOAD_QUEUE_SIZE(self) :
        return 3000   # default maximum number of items allowed in the upload Queue at once

RUN_OPT_CONST = RunOptionConstants()
