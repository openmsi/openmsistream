"""Custom argument parser and associated functions"""

#imports
import pathlib, math, re, logging
from argparse import ArgumentParser
from ..data_file_io.config import RUN_OPT_CONST
from .config import RUN_CONST

#################### MISC. FUNCTIONS ####################

def existing_file(argstring) :
    """
    convert a string or path argument into a path to a file, checking if it exists
    """
    filepath = pathlib.Path(argstring)
    if filepath.is_file() :
        return filepath.resolve()
    raise FileNotFoundError(f'ERROR: file {argstring} does not exist!')

def existing_dir(argstring) :
    """
    convert a string or path argument into a directory path, checking if it exists
    """
    dirpath = pathlib.Path(argstring)
    if dirpath.is_dir() :
        return dirpath.resolve()
    raise FileNotFoundError(f'ERROR: directory {argstring} does not exist!')

def create_dir(argstring) :
    """
    convert a string or path argument into a directory path, creating it if necessary
    """
    if argstring is None : #Then the argument wasn't given and nothing should be done
        return None
    dirpath = pathlib.Path(argstring)
    if dirpath.is_dir() :
        return dirpath.resolve()
    if dirpath.exists() :
        raise RuntimeError(f'ERROR: directory path {argstring} exists but is not a directory!')
    dirpath.mkdir(parents=True)
    return dirpath.resolve()

def config_path(configarg) :
    """
    convert a string or path argument into a config file path (raise an exception if the file can't be found)
    """
    if isinstance(configarg,str) and '.' not in configarg :
        configarg+=RUN_CONST.CONFIG_FILE_EXT
    configpath = pathlib.Path(configarg)
    if configpath.is_file() :
        return configpath.resolve()
    if (RUN_CONST.CONFIG_FILE_DIR/configpath).is_file() :
        return (RUN_CONST.CONFIG_FILE_DIR/configpath).resolve()
    raise ValueError(f'ERROR: config argument {configarg} is not a recognized config file!')

def detect_bucket_name(argstring) :
    """
    detects if the bucket name contains invalid characters
    """
    if argstring is None : #Then the argument wasn't given and nothing should be done
        return None
    illegal_charcters = ['#', '%', '&' ,'{', '}', '\\', '/', '<', '>', '*', '?', ' ',
                         '$', '!', '\'', '\"', ':', '@', '+', '`', '|', '=']
    if argstring in illegal_charcters:
        raise RuntimeError(f'ERROR: Illegal characters in bucket_name {argstring}')
    return argstring

def int_power_of_two(argval) :
    """
    make sure a given value is a nonzero integer power of two (or can be converted to one)
    """
    if not isinstance(argval,int) :
        argval=int(argval)
    if argval<=0 or math.ceil(math.log2(argval))!=math.floor(math.log2(argval)) :
        raise ValueError(f'ERROR: invalid argument: {argval} must be a (nonzero) power of two!')
    return argval

def positive_int(argval) :
    """
    make sure a given value is a positive integer
    """
    argval = int(argval)
    if (not isinstance(argval,int)) or (argval<1) :
        raise ValueError(f'ERROR: invalid argument: {argval} must be a positive integer!')
    return argval

def logger_string_to_level(argval) :
    """
    converts a given string representing a logger level to its corresponding integer
    """
    argval=argval.lower()
    if argval=='notset' :
        return logging.NOTSET
    if argval=='debug' :
        return logging.DEBUG
    if argval=='info' :
        return logging.INFO
    if argval=='warning' :
        return logging.WARNING
    if argval=='error' :
        return logging.ERROR
    if argval=='critical' :
        return logging.CRITICAL
    try :
        if int(argval)>=0 :
            return int(argval)
        raise ValueError(f'ERROR: logger argument {argval} is not valid!')
    except ValueError as exc :
        raise exc

#################### MYARGUMENTPARSER CLASS ####################

class OpenMSIStreamArgumentParser(ArgumentParser) :
    """
    Class to make it easier to get an ArgumentParser with some commonly-used arguments in it.

    All constructor arguments get passed to the underlying :class:`argparse.ArgumentParser` object.

    Arguments for the parser are defined in the :attr:`~OpenMSIStreamArgumentParser.ARGUMENTS` class variable,
    which is a dictionary. The keys are names of arguments, and the values are lists. The first entry in each list
    is a string reading "positional" or "optional" depending on the type of argument, and the second entry is a
    dictionary of keyword arguments to send to :func:`argparse.ArgumentParser.add_argument`.
    """

    ARGUMENTS = {
        'filepath':
            ['positional',{'type':existing_file,'help':'Path to the data file to use'}],
        'output_dir':
            ['positional',{'type':create_dir,'help':'Path to the directory to put output in'}],
        'upload_dir':
            ['positional',{'type':existing_dir,'help':'Path to the directory to watch for files to upload'}],
        'bucket_name':
            ['positional', {'type': detect_bucket_name,
                            'help': '''Name of the object store bucket to which consumed files should be transferred.
                                       Name should only contain valid characters'''}],
        'config':
            ['optional',{'default':RUN_OPT_CONST.DEFAULT_CONFIG_FILE,'type':config_path,
                         'help':f'''Name of config file to use in {RUN_CONST.CONFIG_FILE_DIR.resolve()},
                                    or path to a file in a different location'''}],
        'topic_name':
            ['optional',{'default':RUN_OPT_CONST.DEFAULT_TOPIC_NAME,
                         'help':'Name of the topic to produce to or consume from'}],
        'consumer_topic_name':
            ['optional',{'default':RUN_OPT_CONST.DEFAULT_TOPIC_NAME,
                         'help':'Name of the topic to consume from'}],
        'producer_topic_name':
            ['optional',{'default':RUN_OPT_CONST.DEFAULT_TOPIC_NAME,
                         'help':'Name of the topic to produce to'}],
        'n_threads':
            ['optional',{'default':RUN_CONST.DEFAULT_N_THREADS,'type':positive_int,
                         'help':'Maximum number of threads to use'}],
        'n_consumer_threads':
            ['optional',{'default':RUN_OPT_CONST.N_DEFAULT_DOWNLOAD_THREADS,'type':positive_int,
                         'help':'Number of consumer threads to use'}],
        'n_producer_threads':
            ['optional',{'default':RUN_OPT_CONST.N_DEFAULT_UPLOAD_THREADS,'type':positive_int,
                         'help':'Number of producer threads to use'}],
        'upload_regex':
            ['optional',{'default':RUN_OPT_CONST.DEFAULT_UPLOAD_REGEX,'type':re.compile,
                         'help':'Only files matching this regular expression will be uploaded'}],
        'chunk_size':
            ['optional',{'default':RUN_OPT_CONST.DEFAULT_CHUNK_SIZE,'type':int_power_of_two,
                         'help':'''Max size (in bytes) of chunks into which files should be broken
                                   as they are uploaded'''}],
        'queue_max_size':
            ['optional',{'default':RUN_OPT_CONST.DEFAULT_MAX_UPLOAD_QUEUE_MEGABYTES,'type':int,
                         'help':'''Maximum allowed size in MB of the internal upload queue.
                                 Use to adjust RAM usage if necessary.'''}],
        'update_seconds':
            ['optional',{'default':RUN_CONST.DEFAULT_UPDATE_SECONDS,'type':int,
                         'help':'''Number of seconds between printing a "." to the console
                                   to indicate the program is alive'''}],
        'upload_existing':
            ['optional',{'action':'store_true',
                         'help':'''Add this flag to upload files already existing in addition to those
                                   added to the directory after this code starts running (by default
                                   only files added after startup will be uploaded)'''}],
        'consumer_group_id':
            ['optional',{'default':'create_new',
                         'help':'ID to use for all consumers in the group'}],
        'optional_output_dir':
            ['optional',{'type':create_dir,
                         'help':'Optional path to directory to put output in'}],
        'service_name':
            ['positional',{'help':'The name of the service to work with'}],
        'optional_service_name':
            ['optional',{'help':'The customized name of the Service that will be installed to run the chosen class'}],
        'run_mode':
            ['positional',{'choices':['start','status','stop','remove','stop_and_remove',
                                      'reinstall','stop_and_reinstall','stop_and_restart'],
                           'help':'What to do with the service'}],
        'remove_env_vars':
            ['optional',{'action':'store_true',
                         'help':'''Add this flag to also remove username/password environment variables
                                   when removing a Service'''}],
        'remove_install_args':
            ['optional',{'action':'store_true',
                         'help':'Add this flag to also remove the install arguments file when removing a Service'}],
        'remove_nssm':
            ['optional',{'action':'store_true',
                         'help':'Add this flag to also remove the NSSM executable when removing a Service'}],
        'logger_stream_level':
            ['optional',{'type':logger_string_to_level,
                         'default':'info',
                         'help':"Messages below this level will not be processed by the logger's stream handler"}],
        'logger_file_level':
            ['optional',{'type':logger_string_to_level,
                         'default':'warning',
                         'help':"Messages below this level will not be processed by the logger's file handler"}],
    }

    #################### OVERLOADED FUNCTIONS ####################

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.__argnames_added = []
        self.__subparsers_action_obj = None
        self.__subparsers = {}
        self.__subparser_argnames_added = {}

    def add_subparsers(self,*args,**kwargs) :
        """
        Overloaded from base class; OpenMSIStreamArgumentParser actually owns its subparsers to simplify conflicting
        argument names
        """
        if self.__subparsers_action_obj is not None or self.__subparsers :
            raise RuntimeError('ERROR: add_subparsers called for an argument parser that already has subparsers!')
        self.__subparsers_action_obj = super().add_subparsers(*args,**kwargs)

    def parse_args(self,*args,**kwargs) :
        """
        Overloaded from base class to print usage when catching errors in parsing arguments
        """
        try :
            return super().parse_args(*args,**kwargs)
        except Exception :
            self.print_usage()
            raise

    #################### UNIQUE FUNCTIONS ####################

    def add_arguments(self,*args,**kwargs) :
        """
        Add a group of common arguments to the parser

        :param args: Names of arguments that should be added exactly as they're listed in
            :attr:`~OpenMSIStreamArgumentParser.ARGUMENTS`
        :type args: list
        :param kwargs: Dictionary whose keys are argument names like in `args` and whose values are new
            default argument values
        :type kwargs: dict

        :raises ValueError: if the name of an argument isn't recognized in
            :attr:`~OpenMSIStreamArgumentParser.ARGUMENTS`, or if the type of a new default argument is different
            than the type of its original default argument
        """
        if len(args)<1 and len(kwargs)<1 :
            raise ValueError('ERROR: must specify at least one desired argument to create an argument parser!')
        args_to_use = []
        for argname in args :
            if argname not in args_to_use :
                args_to_use.append(argname)
        for argname in kwargs :
            if argname in args_to_use :
                args_to_use.remove(argname)
        for argname in args_to_use :
            argname_to_add, kwargs_for_arg = self.__get_argname_and_kwargs(argname)
            if argname_to_add not in self.__argnames_added :
                self.add_argument(argname_to_add,**kwargs_for_arg)
                self.__argnames_added.append(argname_to_add)
        for argname,argdefault in kwargs.items() :
            argname_to_add, kwargs_for_arg = self.__get_argname_and_kwargs(argname,argdefault)
            if argname_to_add not in self.__argnames_added :
                self.add_argument(argname_to_add,**kwargs_for_arg)
                self.__argnames_added.append(argname_to_add)

    def add_subparser_arguments(self,subp_name,args_to_add=None,kwargs_to_add=None,**other_kwargs) :
        """
        Create a new subparser and add arguments to it.

        :param subp_name: the name of the subparser command
        :type subp_name: str
        :param args_to_add: additional arguments that should be added to the subparser
            (same format as `args` for :func:`~OpenMSIStreamArgumentParser.add_arguments`)
        :type args_to_add: list, optional
        :param kwargs_to_add: additional keyword arguments that should be added to the subparser
            (same format as `kwargs` for :func:`~OpenMSIStreamArgumentParser.add_arguments`)
        :type kwargs_to_add: dict, optional
        :param other_kwargs: any other keyword arguments are passed to the subparser's add_parser() method
        :type other_kwargs: dict, optional

        :raises ValueError: like in :func:`~OpenMSIStreamArgumentParser.add_arguments`
        """
        if self.__subparsers_action_obj is None :
            errmsg = 'ERROR: add_subparser_arguments called for an argument parser that '
            errmsg+= 'has not added subparsers!'
            raise RuntimeError(errmsg)
        if subp_name in self.__subparsers :
            errmsg = f'ERROR: subparser arguments for {subp_name} have already been added to this argument parser!'
            raise RuntimeError(errmsg)
        self.__subparsers[subp_name] = self.__subparsers_action_obj.add_parser(subp_name,**other_kwargs)
        self.__subparser_argnames_added[subp_name] = []
        if args_to_add is not None :
            for argname in args_to_add :
                argname_to_add, kwargs_for_arg = self.__get_argname_and_kwargs(argname)
                if argname_to_add not in self.__subparser_argnames_added[subp_name] :
                    self.__subparsers[subp_name].add_argument(argname_to_add,**kwargs_for_arg)
                    self.__subparser_argnames_added[subp_name].append(argname_to_add)
        if kwargs_to_add is not None :
            for argname,argdefault in kwargs_to_add.items() :
                argname_to_add,kwargs_for_arg = self.__get_argname_and_kwargs(argname,argdefault)
                if argname_to_add not in self.__subparser_argnames_added[subp_name] :
                    self.__subparsers[subp_name].add_argument(argname_to_add,**kwargs_for_arg)
                    self.__subparser_argnames_added[subp_name].append(argname_to_add)

    def add_subparser_arguments_from_class(self,class_to_add,*,
                                           subp_name=None,addl_args=None,addl_kwargs=None,**other_kwargs) :
        """
        Create a new subparser and add arguments from the given class to it.
        `class_to_add must` inherit from :class:`~Runnable` to be able to get its arguments.

        :param subp_name: an override for the name of the command the subparser should be registered under.
            (Default is the name of the class.)
        :type subp_name: str, optional
        :param addl_args: additional arguments that should be added to the subparser
            (same format as `args` for :func:`~OpenMSIStreamArgumentParser.add_arguments`)
        :type addl_args: list, optional
        :param addl_kwargs: additional keyword arguments that should be added to the subparser
            (same format as `kwargs` for :func:`~OpenMSIStreamArgumentParser.add_arguments`)
        :type addl_kwargs: dict, optional
        :param other_kwargs: any other keyword arguments are passed to subparsers' add_parser() method
        :type other_kwargs: dict, optional

        :raises ValueError: like in :func:`~OpenMSIStreamArgumentParser.add_arguments`
        """
        if subp_name is None :
            subp_name = class_to_add.__name__
        argnames, argnames_with_defaults = class_to_add.get_command_line_arguments()
        if addl_args is not None :
            argnames = [*argnames,*addl_args]
        if addl_kwargs is not None :
            argnames_with_defaults = {**argnames_with_defaults,**addl_kwargs}
        self.add_subparser_arguments(subp_name,argnames,argnames_with_defaults,**other_kwargs)

    def __get_argname_and_kwargs(self,argname,new_default=None) :
        """
        Return the name and kwargs dict for a particular argument

        argname = the given name of the argument, will be matched to a name in ARGUMENTS
        new_default = an optional parameter given to override the default already specified in ARGUMENTS
        """
        if argname in self.ARGUMENTS :
            if self.ARGUMENTS[argname][0]=='positional' :
                argname_to_add = argname
            else :
                if argname.startswith('optional_') :
                    argname_to_add = f'--{argname[len("optional_"):]}'
                else :
                    argname_to_add = f'--{argname}'
            kwargs = self.ARGUMENTS[argname][1].copy()
            if new_default is not None :
                if 'default' in kwargs.keys() and not isinstance(new_default,type(kwargs['default'])) :
                    errmsg = f'ERROR: new default value {new_default} for argument {argname} is of a different type '
                    errmsg+= f'than expected based on the old default ({self.ARGUMENTS[argname]["kwargs"]["default"]})!'
                    raise ValueError(errmsg)
                kwargs['default'] = new_default
            if 'default' in kwargs.keys() :
                if 'help' in kwargs.keys() :
                    kwargs['help']+=f" (default = {kwargs['default']})"
                else :
                    kwargs['help']=f"default = {kwargs['default']}"
            return argname_to_add, kwargs
        raise ValueError(f'ERROR: argument {argname} is not recognized as an option!')

    @property
    def actions(self) :
        """
        wrapper around parser._actions to make it publicly available
        """
        return self._actions
