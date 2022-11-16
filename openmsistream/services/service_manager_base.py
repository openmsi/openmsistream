"""Functionality common to both Windows Services and Linux daemons"""

#imports
import sys, pathlib, textwrap, importlib
from abc import abstractmethod
from subprocess import CalledProcessError
from ..utilities import LogOwner, HasArgumentParser, Runnable, OpenMSIStreamArgumentParser
from ..utilities.config_file_parser import ConfigFileParser
from .config import SERVICE_CONST
from .utilities import set_env_vars, remove_env_var

class ServiceManagerBase(LogOwner,HasArgumentParser) :
    """
    Base class for working with Services in general

    :param service_name: The name of the Service/daemon as installed
    :type service_name: str
    :param service_spec_string: A string specifying which code should be run as a Service.
        Could be the name of an OpenMSIStream Runnable class, or the path to a custom Python code.
        Custom Services can also specify a :class:`~Runnable` class name, and/or a function in the file
        using special formatting like [class_name]=[path.to.file]:[function_name].
        Only needed to initially install the Service/daemon.
    :type service_spec_string: str, optional
    :param argslist: The list of arguments (as from the command line) to pass to the :class:`~Runnable` class.
        Only needed to initially install the Service/daemon.
    :type argslist: list, optional
    :param interactive: if True, a few more messages/prompts will come up telling a user what to do
    :type interactive: bool, optional
    """

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,service_name,*,service_spec_string=None,argslist=None,interactive=None,**kwargs) :
        super().__init__(**kwargs)
        self.service_name = service_name
        self.service_spec_string = service_spec_string
        self.__set_service_dict()
        self.argslist = argslist
        self.interactive = interactive
        self.env_var_filepath = SERVICE_CONST.WORKING_DIR/f'{self.service_name}_env_vars.txt'
        self.install_args_filepath = SERVICE_CONST.WORKING_DIR/f'{self.service_name}_install_args.txt'
        self.exec_fp = SERVICE_CONST.WORKING_DIR/f'{self.service_name}{SERVICE_CONST.SERVICE_EXECUTABLE_NAME_STEM}'
        self.env_vars_needed = None

    def install_service(self) :
        """
        Install the Service
        child classes should call this method to get the setup done before they do anything specific
        """
        #make sure the necessary information was supplied
        if self.service_dict is None or self.argslist is None :
            errmsg = 'ERROR: newly installing a Service requires specifying what to install '
            errmsg+= 'and giving an argslist!'
            self.logger.error(errmsg,exc_type=RuntimeError)
        #set the environment variables
        must_rerun = set_env_vars(self.env_var_names,interactive=self.interactive)
        if must_rerun :
            msg = 'New values for environment variables have been set. '
            msg+= 'Please close this window and rerun InstallService so that their values get picked up.'
            self.logger.info(msg)
            sys.exit(0)
        #write out the environment variable file
        self.env_vars_needed = self._write_env_var_file()
        #write out the install arguments file
        self._write_install_args_file()
        #write out the executable file
        self._write_executable_file()
        #install the service
        msg = 'Installing '
        if self.service_dict['class_name'] is not None :
            msg+='a'
            if self.service_dict['class_name'].lower() in ('a','e','i','o','u','y') :
                msg+='n'
            msg+=f" {self.service_dict['class_name']} program "
        else :
            msg+=f"{self.service_dict['filepath']}"
            if self.service_dict['func_name'] is not None :
                msg+=f":{self.service_dict['func_name']}"
        msg+=f' as "{self.service_name}" from executable at {self.exec_fp}'
        self.logger.info(msg)

    def run_manage_command(self,run_mode,remove_env_vars=False,remove_install_args=False,remove_nssm=False) :
        """
        Run one of the other functions according to the action given

        :param run_mode: The string identifying the action(s) that should be taken
        :type run_mode: str
        :param remove_env_vars: if True, any environment variables needed by the Service/daemon will be removed.
            Only used when removing the Service/daemon.
        :type remove_env_vars: bool, optional.
        :param remove_install_args: if True, the file listing the arguments used to install the Service/daemon
            (to make it easier to re-install) will be removed. Only used when removing the Service/daemon.
        :type remove_install_args: bool, optional
        :param remove_nssm: if True, the NSSM executable will be removed. Only used when removing the Service/daemon.
        :type remove_nssm: bool, optional
        """
        if run_mode in ['start'] :
            self.start_service()
        if run_mode in ['status'] :
            self.service_status()
        if run_mode in ['stop','stop_and_remove','stop_and_reinstall','stop_and_restart'] :
            self.stop_service()
        if run_mode in ['remove','stop_and_remove','stop_and_reinstall','stop_and_restart'] :
            self.remove_service(remove_env_vars=remove_env_vars,
                                remove_install_args=remove_install_args,
                                remove_nssm=remove_nssm)
        if run_mode in ['reinstall','stop_and_reinstall','stop_and_restart'] :
            self.reinstall_service()
        if run_mode in ['stop_and_restart'] :
            self.start_service()

    @abstractmethod
    def start_service(self) :
        """
        start the Service
        not implemented in the base class
        """
        raise NotImplementedError

    @abstractmethod
    def service_status(self) :
        """
        get the status of the service
        not implemented in the base class
        """
        raise NotImplementedError

    @abstractmethod
    def stop_service(self) :
        """
        stop the Service
        not implemented in the base class
        """
        raise NotImplementedError

    def remove_service(self,remove_env_vars=False,remove_install_args=False,remove_nssm=False) :
        """
        Remove the Service. Child classes should call this after doing whatever they need to do to
        optionally remove environment variables.

        :param remove_env_vars: if True, any environment variables needed by the Service/daemon will be removed.
        :type remove_env_vars: bool, optional.
        :param remove_install_args: if True, the file listing the arguments used to install the Service/daemon
            (to make it easier to re-install) will be removed.
        :type remove_install_args: bool, optional
        :param remove_nssm: if True, the NSSM executable will be removed. Only used on Windows.
        :type remove_nssm: bool, optional
        """
        #remove the executable file
        if self.exec_fp.is_file() :
            self.exec_fp.unlink()
        #remove the environment variables that were set when the service was installed
        if remove_env_vars :
            try :
                for env_var_name in self.env_var_names :
                    remove_env_var(env_var_name)
                    self.logger.info(f'{env_var_name} environment variable successfully removed')
            except CalledProcessError :
                warnmsg = 'WARNING: failed to remove environment variables. You should remove any username/password '
                warnmsg+= 'environment variables manually even though the service is uninstalled!'
                self.logger.info(warnmsg)
        else :
            self.logger.info('Environment variables will be retained')
        #remove the actual environment variable file
        if self.env_var_filepath.is_file() :
            self.env_var_filepath.unlink()
        #remove the installation arguments file
        if remove_install_args :
            if self.install_args_filepath.is_file() :
                self.install_args_filepath.unlink()
                self.logger.info(f'Installation arguments file {self.install_args_filepath} deleted.')
        else :
            if self.install_args_filepath.is_file() :
                self.logger.info(f'Installation arguments file {self.install_args_filepath} will be retained')
        self.logger.info(f'Done removing {self.service_name}')
        _ = remove_nssm # appease pyflakes / pylint

    def reinstall_service(self) :
        """
        Install the service using the arguments defined in the service arguments file
        """
        if not self.install_args_filepath.is_file() :
            errmsg =  'ERROR: cannot reinstall service without an installation arguments file '
            errmsg+= f'at {self.install_args_filepath}!'
            self.logger.error(errmsg,exc_type=FileNotFoundError)
        with open(self.install_args_filepath,'r') as fp :
            lines = fp.readlines()
        if not len(lines)>0 :
            errmsg = f'ERROR: installation arguments file {self.install_args_filepath} does not contain enough entries!'
            self.logger.error(errmsg,exc_type=RuntimeError)
        self.service_spec_string = lines[0].strip()
        self.__set_service_dict()
        self.argslist = [line.strip() for line in lines[1:]] if len(lines)>1 else []
        if self.interactive :
            msg = 'Running this reinstall would be like running the following from the command line:\n'
            msg+= f'InstallService {self.service_spec_string} '
            for arg in self.argslist :
                msg+=f'{arg} '
            msg+=f'--service_name {self.service_name}\n'
            msg+='Does everything above look right? [(y)/n]: '
            check = input(msg)
            if check.lower().startswith('n') :
                msg = f'{self.service_name} will NOT be reinstalled. You can install it again by rerunning '
                msg+= 'InstallService on its own.'
                self.logger.info(msg)
                sys.exit(0)
            else :
                self.install_service()

    #################### PRIVATE HELPER FUNCTIONS ####################

    @abstractmethod
    def _write_env_var_file(self) :
        """
        Write and set permissions for the file holding the values of the environment variables needed by the Service
        returns True if there are environment variables referenced, False if not

        Not implemented in base class
        """
        raise NotImplementedError

    def _write_install_args_file(self) :
        """
        Write out a file storing the arguments used to install the Service so it can be
        reinstalled using the same configurations/setting if desired
        """
        if self.service_spec_string is None or self.argslist is None :
            errmsg = "ERROR: can't write the installation arguments file without a service_spec_string and argslist!"
            self.logger.error(errmsg,exc_type=RuntimeError)
        with open(self.install_args_filepath,'w') as fp :
            for arg in [self.service_spec_string,*self.argslist] :
                fp.write(f'{arg}\n')

    def _write_executable_file(self,filepath=None) :
        """
        write out the executable python file that the service will actually be running
        """
        error_log_path = pathlib.Path().resolve()/f'{self.service_name}{SERVICE_CONST.ERROR_LOG_STEM}'
        code = '''\
            if __name__=='__main__' :
                try :'''
        if self.service_dict['func_name'] is not None :
            code+=f'''
                    from {self.service_dict['filepath']} import {self.service_dict['func_name']}
                    {self.service_dict['func_name']}({self.argslist})'''
        else :
            code+=f'''
                    from {self.service_dict['filepath']} import {self.service_dict['class_name']}
                    {self.service_dict['class_name']}.run_from_command_line({self.argslist})'''
        code+=f'''
                except Exception :
                    import pathlib, traceback, datetime
                    output_filepath = pathlib.Path(r"{error_log_path}")
                    with open(output_filepath,'a') as fp :'''
        code+=r'''
                        fp.write(f'{(datetime.datetime.now()).strftime("Error on %Y-%m-%d at %H:%M:%S")}. Exception:\n{traceback.format_exc()}')
                    import sys
                    sys.exit(1)
        '''
        if filepath is not None :
            warnmsg = f'WARNING: Services executable will be written to {filepath} instead of {self.exec_fp}'
            self.logger.warning(warnmsg)
            exec_fp = filepath
        else :
            exec_fp = self.exec_fp
        with open(exec_fp,'w') as fp :
            fp.write(textwrap.dedent(code))

    def __set_service_dict(self) :
        """
        Set the service dict with information about the code that should be run based on the service_spec_string
        """
        if self.service_spec_string is not None :
            service_dict = [sd for sd in SERVICE_CONST.available_services
                            if sd['class_name']==self.service_spec_string]
            if len(service_dict)==1 :
                self.service_dict = service_dict[0]
            elif len(service_dict)==0 :
                f_p, c_n, r_c, f_n = self.__parse_custom_service_string(self.service_spec_string,self.logger)
                self.service_dict = {'filepath':f_p,
                                     'class_name':c_n,
                                     'class':r_c,
                                     'func_name':f_n}
            else :
                errmsg = f'ERROR: could not find the Service dictionary for {self.service_name} '
                errmsg+= f'(a {self.service_spec_string} program)! service_dict = {service_dict}'
                self.logger.error(errmsg,exc_type=RuntimeError)
        else :
            self.service_dict = None

    @staticmethod
    def __parse_custom_service_string(service_spec_string,logger=SERVICE_CONST.logger) :
        """
        Get the filepath and optional class/function names from the custom Service string
        """
        filepath = None
        class_name = None
        run_class = None
        func_name = None
        try :
            #at minimum need a path to a file containing a class or function to run
            if '=' in service_spec_string :
                equals_split = service_spec_string.split('=')
                assert len(equals_split)==2
                class_name = equals_split[0]
                for_path_and_func_name = equals_split[1]
            else :
                class_name = None
                for_path_and_func_name = service_spec_string
            if ':' in service_spec_string :
                colon_split = for_path_and_func_name.split(':')
                assert len(colon_split)==2
                filepath = colon_split[0]
                func_name = colon_split[1]
            else :
                filepath = for_path_and_func_name
            assert filepath is not None
            #make sure the path is valid
            module = importlib.import_module(filepath)
            assert module is not None
            #if the function name was specified, make sure that can be imported from the file, too
            if func_name is not None :
                function = getattr(module,func_name)
                assert function is not None
            #If the class name was given without a function name, make sure the class can be imported from the file
            elif class_name is not None and func_name is None :
                run_class = getattr(module,class_name)
                #and make sure the class extends Runnable, since we'll be calling its run_from_command_line function
                assert issubclass(run_class,Runnable)
            #make sure at least one of the function/class names was given
            assert ((func_name is not None) or ((class_name is not None) and (run_class is not None)))
        except Exception as exc :
            errmsg = f'ERROR: service specification string {service_spec_string} is not valid! '
            errmsg+= 'Will re-raise specific Exception.'
            logger.error(errmsg,exc_info=exc,reraise=True)
        return filepath, class_name, run_class, func_name

    #################### CLASS METHODS ####################

    @classmethod
    def get_argument_parser(cls,install_or_manage=None,class_name_or_spec_string=None) :
        """
        Return the command line argument parser that should be used

        :param install_or_manage: Whether the parser used should accept commands for "install"-ing
            or "manage"-ing the Service/daemon
        :type install_or_manage: str
        :param class_name_or_spec_string: the first argument to the install script, if that's being run.
            Used to add subparsers for known Runnable classes or for custom Runnables defined at the time
            the Service/daemon is being installed.
        :type class_name_or_spec_string: str, optional

        :return: the :class:`~.utilities.OpenMSIStreamArgumentParser` object that should be used
        :rtype: :class:`~.utilities.OpenMSIStreamArgumentParser`

        :raises ValueError: if `install_or_manage` is neither "install" nor "manage"
        """
        parser = OpenMSIStreamArgumentParser()
        if install_or_manage=='install' :
            #subparsers from the classes that could be run
            subp_desc = 'The name of a "Runnable" class (or the specification for a custom Service code) to install '
            subp_desc = 'as a service must be given as the first argument. Adding the name (or spec string) of a class '
            subp_desc = 'that extends "Runnable" to the command line along with "-h" will show additional help.'
            if class_name_or_spec_string in ('--help','-h') :
                return parser
            parser.add_subparsers(description=subp_desc,required=True,dest='service_spec_string')
            if class_name_or_spec_string is None :
                for service_dict in SERVICE_CONST.available_services :
                    parser.add_subparser_arguments_from_class(service_dict['class'],addl_args=['optional_service_name'])
            elif class_name_or_spec_string in [d['class_name'] for d in SERVICE_CONST.available_services] :
                sds = [sd for sd in SERVICE_CONST.available_services if sd['class_name']==class_name_or_spec_string]
                s_d = sds[0]
                parser.add_subparser_arguments_from_class(s_d['class'],addl_args=['optional_service_name'])
            else :
                _, _, run_class, _ = cls.__parse_custom_service_string(class_name_or_spec_string)
                if run_class is not None :
                    parser.add_subparser_arguments_from_class(run_class,
                                                              subp_name=class_name_or_spec_string,
                                                              addl_args=['optional_service_name'])
                else :
                    parser.add_subparser_arguments(class_name_or_spec_string,['optional_service_name'])
        elif install_or_manage=='manage' :
            parser.add_arguments('service_name','run_mode','remove_env_vars','remove_install_args','remove_nssm')
        else :
            errmsg =  'ERROR: must call get_argument_parser with either "install" or "manage", '
            errmsg+= f'not "{install_or_manage}"!'
            raise ValueError(errmsg)
        return parser

    #################### PROPERTIES ####################

    @property
    def env_var_names(self) :
        """
        Names of the environment variables used by the service
        (any environment variables in the command line arguments or config file)
        """
        #get the names of environment variables from the command line and config file
        env_var_names = set()
        if self.argslist is not None and self.service_dict is not None :
            for arg in self.argslist :
                if arg.startswith('$') :
                    env_var_names.add(arg)
            if self.service_dict['class'] is not None :
                parser = self.service_dict['class'].get_argument_parser()
                argsdests = [action.dest for action in parser.actions]
                if 'config' in argsdests :
                    pargs = parser.parse_args(args=self.argslist)
                    cfp = ConfigFileParser(pargs.config,logger=SERVICE_CONST.logger)
                    for evn in cfp.env_var_names :
                        env_var_names.add(evn)
        return env_var_names
