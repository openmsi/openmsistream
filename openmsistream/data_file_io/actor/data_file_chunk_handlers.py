"""Anything that receives DataFileChunk messages from a topic and does something with them"""

# imports
from abc import ABC, abstractmethod
from threading import Lock

from kafkacrypto import KafkaCryptoMessage
from openmsitoolbox import LogOwner
from ...kafka_wrapper.controlled_message_processor import ControlledMessageProcessor
from ...kafka_wrapper.controlled_message_reproducer import ControlledMessageReproducer
from ..config import DATA_FILE_HANDLING_CONST
from ..entity.data_file_chunk import DataFileChunk
from ..entity.download_data_file import DownloadDataFile


class DataFileChunkHandler(LogOwner, ABC):
    """
    Performs some generic handling of DataFileChunk objects read from a topic.
    """

    #################### CONSTANTS ####################

    # Up to this many filepaths will be held onto after they've been processed
    N_RECENT_FILES = 50

    #################### PROPERTIES ####################

    @property
    def other_datafile_kwargs(self):
        """
        Overload this in child classes to define additional keyword arguments
        that should go to the specific datafile constructor
        """
        return {}

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self, *args, datafile_type, **kwargs):
        """
        datafile_type = the type of datafile that the consumed messages will be used to create
            (must be a subclass of DownloadDataFile)
        """
        super().__init__(*args, **kwargs)
        self.datafile_type = datafile_type
        if not issubclass(self.datafile_type, DownloadDataFile):
            errmsg = (
                f"ERROR: {self.__class__.__name__} requires a datafile_type that is "
                f"a subclass of DownloadDataFile but {self.datafile_type} was given!"
            )
            raise ValueError(errmsg)
        self.files_in_progress_by_path = {}
        self.locks_by_fp = {}
        self.recent_processed_filepaths = []
        self.n_processed_files = 0

    #################### PRIVATE HELPER FUNCTIONS ####################

    @abstractmethod
    def _process_message(self, lock, msg, rootdir_to_set):
        """
        Make sure message values are of the expected DataFileChunk type with no root directory set,
        and then add the chunk to the data file object. If the file is in progress this function
        returns True. Otherwise the code from DownloadDataFile.add_chunk will be returned.

        If instead the message was encrypted and could not be successfully decrypted, this will
        return the raw Message object with KafkaCryptoMessages as its key and/or value

        lock = the Thread Lock object to use when processing the file this message comes from
        msg = the actual message object from a call to consumer.poll
        rootdir_to_set = root directory for the new DataFileChunk

        Child classes should call self._process_message() before doing anything else with
        the message to perform these checks
        """
        # If the message has KafkaCryptoMessages as its key and/or value, then decryption failed.
        # Return the message object instead of a code.
        if (
            hasattr(msg, "key")
            and hasattr(msg, "value")
            and (
                isinstance(msg.key, KafkaCryptoMessage)
                or isinstance(msg.value, KafkaCryptoMessage)
            )
        ):
            return msg
        # get the DataFileChunk from the message value
        try:
            dfc = msg.value()  # from a regular Kafka Consumer
        except TypeError:
            dfc = msg.value  # from KafkaCrypto
        # make sure the chunk is of the right type
        if not isinstance(dfc, DataFileChunk):
            errmsg = f"ERROR: expected DataFileChunk messages but received a {type(dfc)} message!"
            self.logger.error(errmsg, exc_type=ValueError)
        # make sure the chunk doesn't already have a rootdir set
        if dfc.rootdir is not None:
            warnmsg = (
                f"WARNING: message with key {dfc.message_key} has rootdir={dfc.rootdir} "
                "(should be None as it was just consumed)! "
                "Will ignore this message and continue."
            )
            self.logger.warning(warnmsg)
        # set the chunk's root directory
        dfc.rootdir = rootdir_to_set
        # add the chunk's data to the file that's being reconstructed
        with lock:
            if dfc.relative_filepath not in self.files_in_progress_by_path:
                new_datafile = self.datafile_type(
                    dfc.filepath,
                    logger=self.logger,
                    **self.other_datafile_kwargs,
                )
                self.files_in_progress_by_path[dfc.relative_filepath] = new_datafile
                self.locks_by_fp[dfc.relative_filepath] = Lock()
        retval = self.files_in_progress_by_path[dfc.relative_filepath].add_chunk(
            dfc,
            self.locks_by_fp[dfc.relative_filepath],
        )
        # If the file is just in progress, return True
        if retval in (
            DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS,
            DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE,
        ):
            return True
        # otherwise just return the code from add_chunk
        return retval


class DataFileChunkProcessor(DataFileChunkHandler, ControlledMessageProcessor):
    """
    Combine template code in DataFileChunkHandler with specifics of processing messages
    """

    @property
    def progress_msg(self):
        """
        A string message describing the files that have had some chunks read
        """
        progress_msg = (
            f"Files recognized so far (up to {self.N_RECENT_FILES} most recent "
            "completed files shown):\n\t"
        )
        with self.lock:
            progress_msg += "\n\t".join(
                [
                    f"{df.relative_filepath} (in progress)"
                    for df in self.files_in_progress_by_path.values()
                ]
            )
            if len(self.files_in_progress_by_path) > 0:
                progress_msg += "\n\t"
            progress_msg += "\n\t".join(
                [f"{fp} (completed)" for fp in self.recent_processed_filepaths]
            )
        return progress_msg


class DataFileChunkReproducer(DataFileChunkHandler, ControlledMessageReproducer):
    """
    Combine template code in DataFileChunkHandler with processing messages from one topic
    and producing others to a different topic
    """

    @property
    def progress_msg(self):
        """
        A string message describing the files that have had some chunks read
        """
        progress_msg = (
            f"Files recognized so far (up to {self.N_RECENT_FILES} most recent "
            "completed files shown):\n\t"
        )
        progress_msg = "The following files have been recognized so far:\n"
        with self.lock:
            for datafile in self.files_in_progress_by_path.values():
                if datafile.relative_filepath not in self.recent_processed_filepaths:
                    progress_msg += f"\t{datafile.relative_filepath} (in progress)\n"
            for fp in self.recent_processed_filepaths:
                if fp not in self.recent_results_produced:
                    progress_msg += f"\t{fp} (fully read from topic)\n"
            for fp in self.recent_results_produced:
                progress_msg += f"\t{fp} (processing results produced)\n"
        return progress_msg

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.recent_results_produced = []
        self.n_results_produced_files = 0
