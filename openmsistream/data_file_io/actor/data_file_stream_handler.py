"""
    Base class for consuming file chunks into memory and then triggering some action 
    when whole files are available
"""

# imports
import pathlib
from abc import ABC

from kafkacrypto.message import KafkaCryptoMessage
from openmsitoolbox import Runnable
from openmsitoolbox.utilities.misc import populated_kwargs
from ...utilities import OpenMSIStreamArgumentParser
from ..config import DATA_FILE_HANDLING_CONST
from ..utilities import get_encrypted_message_timestamp_string
from ..entity.download_data_file import (
    DownloadDataFileToMemory,
    DownloadDataFileToDisk,
    DownloadDataFileToMemoryAndDisk,
)
from .data_file_chunk_handlers import DataFileChunkHandler


class DataFileStreamHandler(DataFileChunkHandler, Runnable, ABC):
    """
    Consumes file chunks into memory and then does something once entire files are available
    """

    ARGUMENT_PARSER_TYPE = OpenMSIStreamArgumentParser
    LOG_SUBDIR_NAME = "LOGS"  # name of the directory that holds the logs

    def __init__(
        self, *args, output_dir=None, mode="memory", datafile_type=None, **kwargs
    ):
        """
        Constructor method
        """
        # make sure the directory for the output is set
        self._output_dir = (
            self._get_auto_output_dir() if output_dir is None else output_dir
        )
        if not self._output_dir.is_dir():
            self._output_dir.mkdir(parents=True)
        # create a subdirectory for the logs
        self._logs_subdir = self._output_dir / self.LOG_SUBDIR_NAME
        if "logger_file" in kwargs and kwargs["logger_file"] is not None:
            logger_file_arg = kwargs["logger_file"].resolve()
            if "." in logger_file_arg.name:
                self._logs_subdir = logger_file_arg.parent
            else:
                self._logs_subdir = logger_file_arg
            kwargs["logger_file"] = self._logs_subdir
        if not self._logs_subdir.is_dir():
            self._logs_subdir.mkdir(parents=True)
        # put the log file in the subdirectory
        kwargs = populated_kwargs(kwargs, {"logger_file": self._logs_subdir})
        # figure out or check the datafile type from the "mode" argument
        self.mode = mode
        self.delete_on_disk_mode = kwargs['delete_on_disk_mode']
        if mode == "memory":
            base_datafile_type = DownloadDataFileToMemory
        elif mode == "disk":
            base_datafile_type = DownloadDataFileToDisk
        elif mode == "both":
            base_datafile_type = DownloadDataFileToMemoryAndDisk
        else:
            raise ValueError(f"ERROR: unrecognized mode argument '{mode}'")
        if not datafile_type:
            datafile_type = base_datafile_type
        if not issubclass(datafile_type, base_datafile_type):
            errmsg = (
                f"ERROR: {self.__class__.__name__} requires a datafile_type that is a "
                f"subclass of {base_datafile_type} but {datafile_type} was given!"
            )
            raise ValueError(errmsg)
        super().__init__(*args, datafile_type=datafile_type, **kwargs)
        self.logger.info(f"Log files and output will be in {self._output_dir}")
        self.file_registry = None  # needs to be set in subclasses

    def _process_message(self, lock, msg, rootdir_to_set=None):
        """
        Parent class message processing function to check for:
            undecryptable messages (returns False)
            files where reconstruction is just in progress (returns True)
            files with mismatched hashes (returns False)
        Child classes should call super()._process_message() and check the return value
        to find successfully-reconstructed files for further handling.
        """
        retval = super()._process_message(
            lock, msg, self._output_dir if rootdir_to_set is None else rootdir_to_set
        )
        # if the message was returned because it couldn't be decrypted,
        # write it to the encrypted messages directory
        if (
            hasattr(retval, "key")
            and hasattr(retval, "value")
            and (
                isinstance(retval.key, KafkaCryptoMessage)
                or isinstance(retval.value, KafkaCryptoMessage)
            )
        ):
            self._undecryptable_message_callback(retval)
            return False
        # get the DataFileChunk from the message value
        try:
            dfc = msg.value()  # from a regular Kafka Consumer
        except TypeError:
            dfc = msg.value  # from KafkaCrypto
        # if the file is just in progress
        if retval is True:
            with lock:
                self.file_registry.register_file_in_progress(dfc)
            return retval
        # if the file hashes didn't match, invoke the callback and return False
        if retval == DATA_FILE_HANDLING_CONST.FILE_HASH_MISMATCH_CODE:
            warnmsg = (
                f"WARNING: hashes for file {dfc.filename} not matched after being read! "
                "The messages for this file will need to be consumed again if the file "
                "is to be processed! Please rerun with the same consumer ID to try again."
            )
            self.logger.warning(warnmsg)
            with lock:
                self.file_registry.register_file_mismatched_hash(dfc)
            self._mismatched_hash_callback(
                self.files_in_progress_by_path[dfc.filepath], lock
            )
            with lock:
                del self.files_in_progress_by_path[dfc.relative_filepath]
                del self.locks_by_fp[dfc.relative_filepath]
            return False
        if retval != DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE:
            self.logger.error(
                f"ERROR: unrecognized add_chunk return value: {retval}",
                exc_type=NotImplementedError,
            )
            return False
        return retval

    def _undecryptable_message_callback(self, msg):
        """
        This function is called when a message that could not be decrypted is found.
        If this function is called it is likely that the file the chunk is coming from
        won't be able to be processed.

        In the base class, this logs a warning.

        :param msg: the :class:`kafkacrypto.Message` object with undecrypted
            :class:`kafkacrypto.KafkaCryptoMessage`s for its key and/or value
        :type msg: :class:`kafkacrypto.Message`
        """
        timestamp_string = get_encrypted_message_timestamp_string(msg)
        warnmsg = (
            "WARNING: encountered a message that failed to be decrypted (timestamp = "
            f"{timestamp_string}). This message will be skipped, and the file it came "
            "from cannot be processed from the stream until it is decryptable. Please "
            "rerun with a new Consumer ID to consume these messages again."
        )
        self.logger.warning(warnmsg)

    def _mismatched_hash_callback(self, datafile, lock):
        """
        Called when a file reconstructed in memory doesn't match the hash of its contents
        originally on disk, providing an opportunity for fallback/backup processing
        in the case of failure.

        Does nothing in the base class.

        :param datafile: A :class:`~.data_file_io.DownloadDataFileToMemory` object that has received
            all of its messages from the topic
        :type datafile: :class:`~.data_file_io.DownloadDataFileToMemory`
        :param lock: Acquiring this :class:`threading.Lock` object would ensure that only one
            instance of :func:`~_mismatched_hash_callback` is running at once
        :type lock: :class:`threading.Lock`
        """

    @classmethod
    def _get_auto_output_dir(cls):
        return pathlib.Path() / f"{cls.__name__}_output"

    @classmethod
    def get_command_line_arguments(cls):
        superargs, superkwargs = super().get_command_line_arguments()
        args = [*superargs, "mode", "delete_on_disk_mode"]
        kwargs = {
            **superkwargs,
            "optional_output_dir": cls._get_auto_output_dir(),
        }
        return args, kwargs

    @classmethod
    def get_init_args_kwargs(cls, parsed_args):
        superargs, superkwargs = super().get_init_args_kwargs(parsed_args)
        print("inside data file stream handler")
        print(superargs)
        print(superkwargs)
        kwargs = {
            **superkwargs,
            "mode": parsed_args.mode,
            "output_dir": parsed_args.output_dir,
        }
        return superargs, kwargs
