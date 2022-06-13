#imports
import pathlib, re

class UtilityConstants :
    """
    Constants for routines in shared
    """
    @property
    def CONFIG_FILE_EXT(self) :
        return '.config'
    @property
    def CONFIG_FILE_DIR(self) :
        return pathlib.Path(__file__).parent.parent / 'my_kafka' / 'config_files'
    @property
    def DEFAULT_N_THREADS(self) :
        return 2
    @property
    def DEFAULT_UPLOAD_REGEX(self) :
        #matches everything except something starting with a '.' or ending in '.log'
        return re.compile(r'^((?!(\.|.*.log))).*$') 
    @property
    def DEFAULT_UPDATE_SECONDS(self) :
        #how many seconds to wait by default between printing the "still alive" character/message for a running process
        return 300     

UTIL_CONST = UtilityConstants()
