#imports
import pathlib

class RunConstants :

    @property
    def CONFIG_FILE_EXT(self) :
        return '.config'

    @property
    def CONFIG_FILE_DIR(self) :
        return pathlib.Path(__file__).parent.parent / 'kafka_wrapper' / 'config_files'

    @property
    def DEFAULT_N_THREADS(self) :
        return 2

    @property
    def DEFAULT_UPDATE_SECONDS(self) :
        #how many seconds to wait by default between printing the "still alive" character/message for a running process
        return 300     

RUN_CONST = RunConstants()
