"""Custom argument parser and associated functions"""

# imports
import pathlib, re
from openmsitoolbox import OpenMSIArgumentParser
from openmsitoolbox.argument_parsing.parser_callbacks import (
    existing_dir,
    positive_int,
    int_power_of_two,
)
from .config import RUN_CONST

#################### CALLBACK FUNCTIONS ####################


def config_path(configarg):
    """
    convert a string or path argument into a config file path
    (raise an exception if the file can't be found)
    """
    if isinstance(configarg, str) and "." not in configarg:
        configarg += RUN_CONST.CONFIG_FILE_EXT
    configpath = pathlib.Path(configarg)
    if configpath.is_file():
        return configpath.resolve()
    if (RUN_CONST.CONFIG_FILE_DIR / configpath).is_file():
        return (RUN_CONST.CONFIG_FILE_DIR / configpath).resolve()
    raise ValueError(
        f"ERROR: config argument {configarg} is not a recognized config file!"
    )


def detect_bucket_name(argstring):
    """
    detects if the bucket name contains invalid characters
    """
    if argstring is None:  # Then the argument wasn't given and nothing should be done
        return None
    illegal_characters = [
        "#",
        "%",
        "&",
        "{",
        "}",
        "\\",
        "/",
        "<",
        ">",
        "*",
        "?",
        " ",
        "$",
        "!",
        "'",
        '"',
        ":",
        "@",
        "+",
        "`",
        "|",
        "=",
    ]
    for illegal_character in illegal_characters:
        if illegal_character in argstring:
            raise RuntimeError(f"ERROR: Illegal characters in bucket_name {argstring}")
    return argstring


def positive_float(argval):
    """
    make sure a given value is a positive float
    """
    argval = float(argval)
    if (not isinstance(argval, float)) or (argval < 0):
        raise ValueError(f"ERROR: invalid argument: {argval} must be a positive float!")
    return argval


#################### MYARGUMENTPARSER CLASS ####################


class OpenMSIStreamArgumentParser(OpenMSIArgumentParser):
    """
    An ArgumentParser with some commonly-used arguments in it.

    All constructor arguments get passed to the underlying :class:`argparse.ArgumentParser` object.

    Arguments for the parser are defined in the :attr:`~OpenMSIStreamArgumentParser.ARGUMENTS`
    class variable, which is a dictionary. The keys are names of arguments, and the values
    are lists. The first entry in each list is a string reading "positional" or "optional"
    depending on the type of argument, and the second entry is a dictionary of keyword arguments
    to send to :func:`argparse.ArgumentParser.add_argument`.
    """

    DEF_HEARTBEAT_INTERVAL = 15 * 60  # send heartbeats every 15 minutes by default

    DEF_LOG_INTERVAL = 60  # send logs every 1 minute by default

    ARGUMENTS = {
        **OpenMSIArgumentParser.ARGUMENTS,
        "upload_dir": [
            "positional",
            {
                "type": existing_dir,
                "help": "Path to the directory to watch for files to upload",
            },
        ],
        "bucket_name": [
            "positional",
            {
                "type": detect_bucket_name,
                "help": (
                    "Name of the object store bucket to which consumed files should be "
                    "transferred. Name should only contain valid characters"
                ),
            },
        ],
        "config": [
            "optional",
            {
                "default": RUN_CONST.DEFAULT_CONFIG_FILE,
                "type": config_path,
                "help": (
                    f"Name of config file to use in {RUN_CONST.CONFIG_FILE_DIR.resolve()}, "
                    "or path to a file in a different location"
                ),
            },
        ],
        "topic_name": [
            "optional",
            {
                "default": RUN_CONST.DEFAULT_TOPIC_NAME,
                "help": "Name of the topic to produce to or consume from",
            },
        ],
        "consumer_topic_name": [
            "optional",
            {
                "default": RUN_CONST.DEFAULT_TOPIC_NAME,
                "help": "Name of the topic to consume from",
            },
        ],
        "producer_topic_name": [
            "optional",
            {
                "default": RUN_CONST.DEFAULT_TOPIC_NAME,
                "help": "Name of the topic to produce to",
            },
        ],
        "n_consumer_threads": [
            "optional",
            {
                "default": RUN_CONST.N_DEFAULT_DOWNLOAD_THREADS,
                "type": positive_int,
                "help": "Number of consumer threads to use",
            },
        ],
        "n_producer_threads": [
            "optional",
            {
                "default": RUN_CONST.N_DEFAULT_UPLOAD_THREADS,
                "type": positive_int,
                "help": "Number of producer threads to use",
            },
        ],
        "upload_regex": [
            "optional",
            {
                "default": RUN_CONST.DEFAULT_UPLOAD_REGEX,
                "type": re.compile,
                "help": "Only files with paths matching this regular expression will be uploaded",
            },
        ],
        "download_regex": [
            "optional",
            {
                "type": re.compile,
                "help": "Only files with paths matching this regular expression will be uploaded",
            },
        ],
        "chunk_size": [
            "optional",
            {
                "default": RUN_CONST.DEFAULT_CHUNK_SIZE,
                "type": int_power_of_two,
                "help": (
                    "Max size (in bytes) of chunks into which files should be broken "
                    "as they are uploaded"
                ),
            },
        ],
        "queue_max_size": [
            "optional",
            {
                "default": RUN_CONST.DEFAULT_MAX_UPLOAD_QUEUE_MEGABYTES,
                "type": positive_int,
                "help": (
                    "Maximum allowed size in MB of the internal upload queue. "
                    "Use to adjust RAM usage if necessary."
                ),
            },
        ],
        "mode": [
            "optional",
            {
                "choices": ["memory", "disk", "both"],
                "default": "memory",
                "help": (
                    "Choose whether files should be reconstructed in 'memory', "
                    "on 'disk' (with the output directory as the root directory) "
                    "or 'both' (giving access to the data bytestring, "
                    "but also writing to disk)"
                ),
            },
        ],
        "upload_existing": [
            "optional",
            {
                "action": "store_true",
                "help": (
                    "Add this flag to upload files already existing in addition to those "
                    "added to the directory after this code starts running (by default "
                    "only files added after startup will be uploaded)"
                ),
            },
        ],
        "watchdog_lag_time": [
            "optional",
            {
                "default": RUN_CONST.DEFAULT_WATCHDOG_LAG_TIME,
                "type": int,
                "help": (
                    "Number of seconds that a file must remain static (unmodified) "
                    "for its upload to begin"
                ),
            },
        ],
        "use_polling_observer": [
            "optional",
            {
                "action": "store_true",
                "help": (
                    "Add this flag to use a PollingObserver to trigger watchdog events "
                    "instead of the default Observer. This is recommended when reading "
                    "files from CIFS/SMB mounted directories, or if the default Observer "
                    "causes any problems in a deployment."
                ),
            },
        ],
        "consumer_group_id": [
            "optional",
            {"default": "create_new", "help": "ID to use for all consumers in the group"},
        ],
        "treat_undecryptable_as_plaintext": [
            "optional",
            {
                "action": "store_true",
                "help": (
                    "If this flag is present, Consumer(s) will return message "
                    "keys/values that will never be decryptable as plaintext. "
                    "This speeds up processing of messages that will never be "
                    "decryptable. For use in cases like enabling/disabling encryption "
                    "across a platform, or when encrypted and unencrypted messages are "
                    "mixed in the same topic."
                ),
            },
        ],
        "max_wait_per_decrypt": [
            "optional",
            {
                "action": "store_true",
                "help": (
                    "Setting this allows changing the time a KafkaCrypto deserializer "
                    "waits before giving up. Default: 5 sec."
                ),
            },
        ],
        "max_initial_wait_per_decrypt": [
            "optional",
            {
                "action": "store_true",
                "help": (
                    "Setting this allows changing the time a KafkaCrypto deserializer "
                    "waits the first time before giving up. Default: 60 sec."
                ),
            },
        ],
        "girder_api_url": [
            "positional",
            {
                "help": (
                    "The full path to the REST API of a Girder instance, "
                    "e.g. http://my.girder.com/api/v1."
                )
            },
        ],
        "girder_api_key": [
            "positional",
            {"help": "The API key to use for authenticating to the Girder instance"},
        ],
        "girder_root_folder_id": [
            "optional",
            {
                "help": (
                    "The ID of the Girder Folder relative to which files should be "
                    "uploaded. If this argument is given, it will supersede both of the "
                    "'collection_name' and 'girder_root_folder_path' arguments."
                ),
            },
        ],
        "collection_name": [
            "optional",
            {
                "help": (
                    "The name of the top-level Collection to which files should be uploaded. "
                    "Superseded if 'girder_root_folder_id' is given."
                ),
            },
        ],
        "girder_root_folder_path": [
            "optional",
            {
                "help": (
                    "The name of the Folder inside the top-level Collection relative to "
                    "which files should be uploaded. A path to a subdirectory in the "
                    "Collection can be given using forward slashes. "
                    "Superseded if 'girder_root_folder_id' is given. "
                    "(default = collection_name/topic_name)"
                ),
            },
        ],
        "metadata": [
            "optional",
            {
                "help": (
                    "Adding this argument will add a corresponding metadata "
                    "field to all created folders and items. Assumes JSON serialized string."
                ),
            },
        ],
        "heartbeat_topic_name": [
            "optional",
            {
                "help": (
                    "Name of the topic to which heartbeat messages for the long-running "
                    "program should be produced. This argument must be included to "
                    "produce heartbeat messages"
                ),
            },
        ],
        "heartbeat_program_id": [
            "optional",
            {
                "help": (
                    "ID to include in keys of heartbeat messages to uniquely identify "
                    "this particular long-running program instance. (Default is the hex "
                    "code of the heartbeat producer's address in memory which is NOT "
                    "static)"
                ),
            },
        ],
        "heartbeat_interval_secs": [
            "optional",
            {
                "default": DEF_HEARTBEAT_INTERVAL,
                "type": positive_float,
                "help": (
                    "How often (in seconds) messages should be produced to the "
                    "heartbeat topic configured for the long-running program"
                ),
            },
        ],
        "log_topic_name": [
            "optional",
            {
                "help": (
                    "Name of the topic to which log messages for the long-running "
                    "program should be produced. This argument must be included to "
                    "produce log messages"
                ),
            },
        ],
        "log_program_id": [
            "optional",
            {
                "help": (
                    "ID to include in keys of log messages to uniquely identify "
                    "this particular long-running program instance. (Default is the hex "
                    "code of the log producer's address in memory which is NOT "
                    "static)"
                ),
            },
        ],
        "log_interval_secs": [
            "optional",
            {
                "default": DEF_LOG_INTERVAL,
                "type": positive_float,
                "help": (
                    "How often (in seconds) messages should be produced to the "
                    "log topic configured for the long-running program"
                ),
            },
        ],
    }

    def parse_args(self, *args, **kwargs):
        """
        Overloaded from base class to use topic_name as default for consumer_topic_name /
        producer_topic_name when the former is specified and the latter is not.
        """
        rns = super().parse_args(*args, **kwargs)
        if hasattr(rns, "topic_name") and rns.topic_name is not None:
            if (
                not hasattr(rns, "consumer_topic_name")
                or rns.consumer_topic_name is None
                or rns.consumer_topic_name == RUN_CONST.DEFAULT_TOPIC_NAME
            ):
                setattr(rns, "consumer_topic_name", rns.topic_name)
            if (
                not hasattr(rns, "producer_topic_name")
                or rns.producer_topic_name is None
                or rns.producer_topic_name == RUN_CONST.DEFAULT_TOPIC_NAME
            ):
                setattr(rns, "producer_topic_name", rns.topic_name)
        return rns
