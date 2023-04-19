# imports
import pathlib
from watchdog.events import FileSystemEventHandler
from ...utilities import LogOwner

class UploadDirectoryEventHandler(LogOwner,FileSystemEventHandler) :

    def __init__(self,upload_regex,logs_subdir,**other_kwargs) :
        """
        Constructor method
        """
        super().__init__(**other_kwargs)
        self.__upload_regex = upload_regex
        self.__logs_subdir = logs_subdir
        self.__rootdir = self.__logs_subdir.parent

    def dispatch(self, event):
        # Ignore directory events
        if event.is_directory:
            return
        paths = []
        if hasattr(event, "dest_path"):
            paths.append(pathlib.Path(event.dest_path))
        if event.src_path:
            paths.append(pathlib.Path(event.src_path))
        if any(self.filepath_should_be_uploaded(p) for p in paths):
            super().dispatch(event)

    def on_moved(self, event):
        super().on_moved(event)
        what = "directory" if event.is_directory else "file"
        self.logger.info(
            "Moved %s: from %s to %s", what, event.src_path, event.dest_path
        )

    def on_created(self, event):
        super().on_created(event)
        what = "directory" if event.is_directory else "file"
        self.logger.info("Created %s: %s", what, event.src_path)

    def on_deleted(self, event):
        super().on_deleted(event)
        what = "directory" if event.is_directory else "file"
        self.logger.info("Deleted %s: %s", what, event.src_path)

    def on_modified(self, event):
        super().on_modified(event)
        what = "directory" if event.is_directory else "file"
        self.logger.info("Modified %s: %s", what, event.src_path)

    def on_closed(self, event):
        super().on_closed(event)
        what = "directory" if event.is_directory else "file"
        self.logger.info("Closed %s: %s", what, event.src_path)

    def on_opened(self, event):
        super().on_opened(event)
        what = "directory" if event.is_directory else "file"
        self.logger.info("Opened %s: %s", what, event.src_path)

    def filepath_should_be_uploaded(self,filepath) :
        if not isinstance(filepath,pathlib.PurePath) :
            self.logger.error(
                f'ERROR: {filepath} passed to filepath_should_be_uploaded is not a Path!',
                exc_type=TypeError
            )
        # only files count
        if not filepath.is_file() :
            return False
        # must be relative to the upload directory
        if not filepath.is_relative_to(self.__rootdir) :
            return False
        # must be outside the logs subdirectory
        if self.__logs_subdir in filepath.parents :
            return False
        # name shouldn't start with '.'
        if filepath.name.startswith('.') :
            return False
        # must match the given regex
        if self.__upload_regex.match(str(filepath.relative_to(self.__rootdir))) :
            return True
        return False