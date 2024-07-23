""" A watchdog FileSystemEventHandler for use with a DataFileUploadDirectory """

# imports
import pathlib, datetime
from dataclasses import dataclass
from threading import RLock
from watchdog.events import FileSystemEventHandler
from openmsitoolbox import LogOwner


@dataclass(eq=True, frozen=True)
class EventHandlerActiveFile:
    """
    A small dataclass to hold active files recognized by the event handler,
    along with a timestamp
    """

    filepath: pathlib.Path
    last_updated: datetime.datetime


class UploadDirectoryEventHandler(LogOwner, FileSystemEventHandler):
    """
    An extension of a watchdog FileSystemEventHandler to handle file events used by
    :class:`~DataFileUploadDirectory` objects. Keeps a dictionary of files with
    timestamps of when they were updated. That dictionary can be polled for active
    file objects by the upload directory.

    :param upload_regex: only files matching this regular expression will be uploaded
    :type upload_regex: :func:`re.compile`, optional
    :param logs_subdir: path to the owning upload directory's "logs" directory
    :type logs_subdir: :class:`pathlib.Path`
    :param watchdog_lag_time: Number of seconds that files must remain static (unmodified)
        before it's given in :func:`get_new_files`
    :type watchdog_lag_time: int
    """

    #################### NEW PUBLIC FUNCTIONS ####################

    def __init__(self, upload_regex, logs_subdir, lag_time, **other_kwargs):
        """
        Constructor method
        """
        super().__init__(**other_kwargs)
        # variables for determining files whose events should be dispatched
        self.__upload_regex = upload_regex
        self._logs_subdir = logs_subdir
        self.__rootdir = self._logs_subdir.parent
        self.__lag_time = lag_time
        # A thread lock
        self.__lock = RLock()
        # all currently active files
        self.__active_files_by_path = {}

    def get_new_files(self):
        """
        A generator of any new EventHandlerActiveFile objects that have persisted
        for at least a small amount of time. Called by the upload directory to get
        new files to upload (or re-upload modified or moved files)
        """
        keys_to_pop = []
        ref_timestamp = datetime.datetime.now()
        with self.__lock:
            for filepath, active_file in self.__active_files_by_path.items():
                if (
                    ref_timestamp - active_file.last_updated
                ).total_seconds() > self.__lag_time:
                    keys_to_pop.append(filepath)
            for key in keys_to_pop:
                yield self.__active_files_by_path.pop(key)

    def kick_back(self, handler_file):
        """
        Give back an :class:`EventHandlerActiveFile` so it can be processed later

        :param handler_file: The file to add back in as active
        :type handler_file: :class:`EventHandlerActiveFile`
        """
        self.__add_active_file(handler_file)

    def filepath_matched(self, filepath):
        """
        Returns True if a given filepath should be considered for event dispatch
        (or, eventually, upload). Only returns True for paths to files relative
        to the upload directory root dir, but not in the logs subdir, with names
        that don't start with "." and whose relative paths match the upload regex

        :param filepath: Filepath to test
        :type filepath: :class:`pathlib.Path`

        :raises TypeError: if `filepath` is not a path
        """
        if not isinstance(filepath, pathlib.PurePath):
            self.logger.error(
                f"ERROR: {filepath} passed to filepath_should_be_uploaded is not a Path!",
                exc_type=TypeError,
            )
        # only files count
        if not filepath.is_file():
            return False
        # must be relative to the upload directory
        try:
            if not filepath.is_relative_to(self.__rootdir):
                return False
        except AttributeError:  # "is_relative_to" was added after 3.7
            if not str(filepath).startswith(str(self.__rootdir)):
                return False
        # must be outside the logs subdirectory
        if self._logs_subdir in filepath.parents:
            return False
        # name shouldn't start with '.'
        if filepath.name.startswith("."):
            return False
        # must match the given regex
        if self.__upload_regex.match(str(filepath.relative_to(self.__rootdir))):
            return True
        return False

    #################### INHERITED FUNCTIONS ####################

    def dispatch(self, event):
        # Ignore directory events
        if event.is_directory:
            return
        # add the source and destination paths
        paths = []
        if event.src_path:
            paths.append(pathlib.Path(event.src_path))
        if hasattr(event, "dest_path"):
            paths.append(pathlib.Path(event.dest_path))
        # only dispatch events with at least one of their paths being relevant
        if any(self.filepath_matched(p) for p in paths):
            super().dispatch(event)

    def on_moved(self, event):
        super().on_moved(event)
        src_path = pathlib.Path(event.src_path)
        dest_path = pathlib.Path(event.dest_path)
        with self.__lock:
            self.__remove_active_file(src_path)
            self.__add_active_file(dest_path)

    def on_created(self, event):
        super().on_created(event)
        src_path = pathlib.Path(event.src_path)
        self.__add_active_file(src_path)

    def on_deleted(self, event):
        super().on_deleted(event)
        src_path = pathlib.Path(event.src_path)
        self.__remove_active_file(src_path)

    def on_modified(self, event):
        super().on_modified(event)
        src_path = pathlib.Path(event.src_path)
        self.__add_active_file(src_path)

    #################### PRIVATE HELPER FUNCTIONS ####################

    def __add_active_file(self, filepath_or_handler_file):
        if isinstance(filepath_or_handler_file, pathlib.PurePath):
            with self.__lock:
                self.__active_files_by_path[
                    filepath_or_handler_file
                ] = EventHandlerActiveFile(
                    filepath_or_handler_file, datetime.datetime.now()
                )
        elif isinstance(filepath_or_handler_file, EventHandlerActiveFile):
            with self.__lock:
                self.__active_files_by_path[
                    filepath_or_handler_file.filepath
                ] = filepath_or_handler_file
        else:
            self.logger.error(
                f"ERROR: unrecognized type {type(filepath_or_handler_file)}",
                exc_type=TypeError,
            )

    def __remove_active_file(self, filepath):
        with self.__lock:
            if filepath in self.__active_files_by_path:
                _ = self.__active_files_by_path.pop(filepath)
