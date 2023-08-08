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
                    "Name of the object store bucket to which consumed files should be transferred."
                    "Name should only contain valid characters"
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
        "consumer_group_id": [
            "optional",
            {"default": "create_new", "help": "ID to use for all consumers in the group"},
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
        "provider": [
            "optional",
            {
                "help": (
                    'Adding this argument will add a corresponding "provider" metadata '
                    "field to all created folders and items"
                ),
            },
        ],
    }
