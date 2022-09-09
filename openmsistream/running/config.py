"""Constants used in running different controlled processes in general"""

#imports
import pathlib

class RunConstants :
    """
    Constants used for running controlled processes, reading config files, etc.
    """

    CONFIG_FILE_EXT = '.config'
    CONFIG_FILE_DIR = pathlib.Path(__file__).parent.parent / 'kafka_wrapper' / 'config_files'
    DEFAULT_N_THREADS = 2
    DEFAULT_UPDATE_SECONDS = 300

RUN_CONST = RunConstants()
