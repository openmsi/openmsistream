"""Constants used in running different controlled processes in general"""

# imports
import os, pathlib, re


class RunConstants:
    """
    Constants used for running controlled processes, reading config files, etc.
    """

    CONFIG_FILE_EXT = ".config"
    CONFIG_FILE_DIR = (
        os.environ["OPENMSISTREAM_CONFIG_FILE_DIR"]
        if "OPENMSISTREAM_CONFIG_FILE_DIR" in os.environ
        else (
            pathlib.Path(__file__).parent.parent / "kafka_wrapper" / "config_files"
        ).resolve()
    )
    # name of the config file that will be used by default
    DEFAULT_CONFIG_FILE = "test"
    # name of the topic to produce to by default
    DEFAULT_TOPIC_NAME = "test"
    # default number of threads to use when uploading a file
    N_DEFAULT_UPLOAD_THREADS = 2
    # default number of threads to use when downloading chunks of a file
    N_DEFAULT_DOWNLOAD_THREADS = 2
    # name of the config file used in "real" production
    PRODUCTION_CONFIG_FILE = "prod"
    # matches everything except something starting with a '.' or ending in '.log'
    DEFAULT_UPLOAD_REGEX = re.compile(r"^((?!(\.|.*.log))).*$")
    # default size in bytes of each file upload chunk
    DEFAULT_CHUNK_SIZE = 524288
    # default maximum size (in MB) of the internal upload Queue
    DEFAULT_MAX_UPLOAD_QUEUE_MEGABYTES = 500
    # default number of seconds that a file must remain static for uploading to start
    DEFAULT_WATCHDOG_LAG_TIME = 3
    # default Girder collection name
    DEFAULT_COLLECTION_NAME = "WholeTale Catalog"


RUN_CONST = RunConstants()
