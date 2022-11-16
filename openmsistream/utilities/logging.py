"""Classes for OpenMSIStream logging infrastructure"""

#imports
import pathlib, logging
from .has_arguments import HasArguments

class OpenMSIStreamFormatter(logging.Formatter) :
    """
    Very small extension of the usual logging.Formatter to allow modification of format based on message content
    """

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)

    def format(self, record):
        """
        If a message starts with a newline, start the actual logging line with the newline before any of the rest
        """
        formatted = ''
        if record.msg.startswith('\n') :
            record.msg = record.msg.lstrip('\n')
            formatted+='\n'
        formatted+=super().format(record)
        return formatted

class Logger :
    """
    Class for a general logger in OpenMSIStream format.

    :param logger_name: The name for the logger to use (automatically inferred from the running module if not given)
    :type logger_name: str, optional
    :param streamlevel: The level at/above which messages should be logged to the stream/console
    :type streamlevel: logging level int, optional
    :param logger_filepath: The path to a logger file to use or directory in which an automatically-named
        logger file should be created
    :type logger_filepath: :class:`pathlib.Path`, optional
    :param filelevel: The level at/above which messages should be written to the logfile
    :type filelevel: logging level int, optional
    """

    FORMATTER = OpenMSIStreamFormatter('[%(name)s at %(asctime)s] %(message)s','%Y-%m-%d %H:%M:%S')

    def __init__(self,logger_name=None,streamlevel=logging.INFO,logger_filepath=None,filelevel=logging.WARNING) :
        """
        name = the name for this logger to use (probably something like the top module that owns it)
        """
        self._name = logger_name
        if self._name is None :
            self._name = self.__class__.__name__
        self._logger_obj = logging.getLogger(self._name)
        self._logger_obj.setLevel(logging.DEBUG)
        self._streamhandler = logging.StreamHandler()
        self._streamhandler.setLevel(streamlevel)
        self._streamhandler.setFormatter(self.FORMATTER)
        self._logger_obj.addHandler(self._streamhandler)
        self._filehandler = None
        if logger_filepath is not None :
            self.add_file_handler(logger_filepath,level=filelevel)

    def set_level(self,level) :
        """
        Set the level of the entire underlying logger

        :param level: The level to set
        :type level: logging level int
        """
        self._logger_obj.setLevel(level)

    def set_stream_level(self,level) :
        """
        Set the level of the underlying logger's streamhandler

        :param level: The level to set
        :type level: logging level int
        """
        self._streamhandler.setLevel(level)

    def set_file_level(self,level) :
        """
        Set the level of the underlying logger's filehandler

        :param level: The level to set
        :type level: logging level int
        """
        if self._filehandler is None :
            errmsg = f'ERROR: Logger {self._name} does not have a filehandler set but set_file_level was called!'
            raise RuntimeError(errmsg)
        self._filehandler.setLevel(level)

    def add_file_handler(self,filepath,level=logging.INFO) :
        """
        Add an additional :class:`logging.FileHandler` to the logger

        :param filepath: The path to the new logger file
        :type filepath: :class:`pathlib.Path`
        :param level: The level to set
        :type level: logging level int
        """
        if not isinstance(filepath,pathlib.PurePath) :
            self.error(f'ERROR: {filepath} is a {type(filepath)} object, not a Path object!',exc_type=TypeError)
        if not filepath.is_file() :
            if not filepath.parent.is_dir() :
                filepath.parent.mkdir(parents=True)
            filepath.touch()
        self._filehandler = logging.FileHandler(filepath)
        self._filehandler.setLevel(level)
        self._filehandler.setFormatter(self.FORMATTER)
        self._logger_obj.addHandler(self._filehandler)

    #methods for logging different levels of messages

    def debug(self,msg,*args,**kwargs) :
        """
        Log a message at DEBUG level. Additional args/kwargs are sent to the underlying logger object's debug call.

        :param msg: the message to log
        :type msg: str
        """
        self._logger_obj.debug(msg,*args,**kwargs)

    def info(self,msg,*args,**kwargs) :
        """
        Log a message at INFO level. Additional args/kwargs are sent to the underlying logger object's info call.

        :param msg: the message to log
        :type msg: str
        """
        self._logger_obj.info(msg,*args,**kwargs)

    def warning(self,msg,*args,**kwargs) :
        """
        Log a message at WARNING level. Additional args/kwargs are sent to the underlying logger object's warning call.

        :param msg: the message to log (will have "WARNING: " prepended if it doesn't start with it)
        :type msg: str
        """
        if not msg.startswith('WARNING:') :
            msg = f'WARNING: {msg}'
        self._logger_obj.warning(msg,*args,**kwargs)

    #log an error message and optionally raise an exception with the same message, or raise a different exception
    def error(self,msg,*,exc_type=None,reraise=False,**kwargs) :
        """
        Log a message at ERROR level. Optionally raise an exception of a given type with the same message, or
        re-raise an Exception object passed through the `exc_info` kwarg (after logging its traceback).

        Additional kwargs are sent to the underlying logger object's error call.

        :param msg: the message to log
        :type msg: str
        :param exc_type: The type of Exception to raise with the same message as `msg`
        :type exc_type: :class:`Exception`, optional
        :param reraise: if True, any :class:`Exception` object passed through the `exc_info` will be re-raised
            after its traceback is logged.
        :type reraise: bool, optional
        """
        if not msg.startswith('ERROR:') :
            msg = f'ERROR: {msg}'
        self._logger_obj.error(msg,**kwargs)
        if reraise and ('exc_info' in kwargs) and isinstance(kwargs['exc_info'],Exception) :
            raise kwargs['exc_info']
        if exc_type is not None :
            raise exc_type(msg)

class LogOwner(HasArguments) :
    """
    Any subclasses extending this one will have access to a Logger defined by the first class in the MRO to extend it

    :param logger: a :class:`~.utilities.Logger` object that this class should have access to. If this
        parameter is given it will override any of the others provided.
    :type logger: :class:`~.utilities.Logger`, optional
    :param logger_name: The name for the logger to use (automatically inferred from the running module if not given)
    :type logger_name: str, optional
    :param streamlevel: The level at/above which messages should be logged to the stream/console
    :type streamlevel: logging level int, optional
    :param logger_filepath: The path to a logger file to use or directory in which an automatically-named
        logger file should be created
    :type logger_filepath: :class:`pathlib.Path`, optional
    :param filelevel: The level at/above which messages should be written to the logfile
    :type filelevel: logging level int, optional
    """

    @property
    def logger(self) :
        """
        The logger object that the class can use
        """
        return self.__logger

    @logger.setter
    def logger(self,logger) :
        if ( hasattr(self,'_LogOwner__logger') and
             self.__logger is not None and
             (not isinstance(self.__logger,type(logger))) ) :
            errmsg = f'ERROR: tried to reset a logger of type {type(self.__logger)} to a new logger of type {logger}!'
            self.__logger.error(errmsg,exc_type=ValueError)
        else :
            self.__logger = logger

    def __init__(self,*args,
                 logger=None,logger_name=None,streamlevel=logging.INFO,logger_file=None,filelevel=logging.WARNING,
                 **other_kwargs) :
        if logger is not None :
            self.__logger = logger
        else :
            if logger_name is None :
                logger_name = self.__class__.__name__
            logger_filepath = logger_file
            if logger_file is not None and logger_file.is_dir() :
                logger_filepath = logger_file / f'{self.__class__.__name__}.log'
            self.__logger = Logger(logger_name,streamlevel,logger_filepath,filelevel)
        super().__init__(*args,**other_kwargs)

    @classmethod
    def get_command_line_arguments(cls) :
        """
        Return the names of arguments for the logger stream and file levels.
        """
        superargs, superkwargs = super().get_command_line_arguments()
        return [*superargs,'logger_stream_level','logger_file_level'], superkwargs
