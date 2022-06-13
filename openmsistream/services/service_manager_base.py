#imports
import sys, pathlib, textwrap
from abc import abstractmethod
from subprocess import CalledProcessError
from ..shared.argument_parsing import MyArgumentParser
from ..shared.config_file_parser import ConfigFileParser
from ..shared.logging import LogOwner
from ..shared.has_argument_parser import HasArgumentParser
from .config import SERVICE_CONST
from .utilities import set_env_vars, remove_env_var

class ServiceManagerBase(LogOwner,HasArgumentParser) :
    """
    Base class for working with Services in general
    """

    #################### CLASS METHODS ####################
    
    @classmethod
    def get_argument_parser(cls,install_or_manage) :
        parser = MyArgumentParser()
        if install_or_manage=='install' :
            #subparsers from the classes that could be run
            subp_desc = 'The name of a runnable class to install as a service must be given as the first argument. '
            subp_desc = 'Adding one of these class names to the command line along with "-h" will show additional help.'
            parser.add_subparsers(description=subp_desc,required=True,dest='service_class_name')
            for service_dict in SERVICE_CONST.AVAILABLE_SERVICES :
                parser.add_subparser_arguments_from_class(service_dict['class'],addl_args=['optional_service_name'])
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
        """
        #get the names of environment variables from the command line and config file
        if self.argslist is not None and self.service_dict is not None :
            for arg in self.argslist :
                if arg.startswith('$') :
                    yield arg
            p = self.service_dict['class'].get_argument_parser()
            argsdests = [action.dest for action in p._actions]
            if 'config' in argsdests :
                pargs = p.parse_args(args=self.argslist)
                cfp = ConfigFileParser(pargs.config,logger=SERVICE_CONST.LOGGER)
                for evn in cfp.env_var_names :
                    yield evn

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,service_name,*args,service_class_name=None,argslist=None,interactive=None,**kwargs) :
        super().__init__(*args,**kwargs)
        self.service_name = service_name
        self.service_class_name = service_class_name
        self.__set_service_dict_from_class_name()
        self.argslist = argslist
        self.interactive = interactive
        self.env_var_filepath = SERVICE_CONST.WORKING_DIR/f'{self.service_name}_env_vars.txt'
        self.install_args_filepath = SERVICE_CONST.WORKING_DIR/f'{self.service_name}_install_args.txt'
        self.exec_fp = SERVICE_CONST.WORKING_DIR/f'{self.service_name}{SERVICE_CONST.SERVICE_EXECUTABLE_NAME_STEM}'
    
    def install_service(self) :
        """
        Install the Service
        child classes should call this method to get the setup done before they do anything specific
        """
        #make sure the necessary information was supplied
        if self.service_class_name is None or self.service_dict is None or self.argslist is None :
            errmsg = 'ERROR: newly installing a Service requires that the class name and argslist are supplied!'
            self.logger.error(errmsg,RuntimeError)
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
        msg='Installing a'
        if self.service_class_name[0].lower() in ('a','e','i','o','u','y') :
            msg+='n'
        msg+=f' {self.service_class_name} program as "{self.service_name}" from executable at {self.exec_fp}...'
        self.logger.info(msg)

    def run_manage_command(self,run_mode,remove_env_vars=False,remove_install_args=False,remove_nssm=False) :
        """
        Run one of the functions below according to the action given
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
        pass

    @abstractmethod
    def service_status(self) :
        """
        get the status of the service
        not implemented in the base class
        """
        pass

    @abstractmethod
    def stop_service(self) :
        """
        stop the Service
        not implemented in the base class
        """
        pass

    def remove_service(self,remove_env_vars=False,remove_install_args=False) :
        """
        remove the Service
        child classes should call this after doing whatever they need to do to possibly remove environment variables
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
    
    def reinstall_service(self) :
        """
        Install the service using the arguments defined in the service arguments file
        """
        if not self.install_args_filepath.is_file() :
            errmsg =  'ERROR: cannot reinstall service without an installation arguments file '
            errmsg+= f'at {self.install_args_filepath}!'
            self.logger.error(errmsg,FileNotFoundError)
        with open(self.install_args_filepath,'r') as fp :
            lines = fp.readlines()
        if not len(lines)>0 :
            errmsg = f'ERROR: installation arguments file {self.install_args_filepath} does not contain enough entries!'
            self.logger.error(errmsg,RuntimeError)
        self.service_class_name = lines[0].strip()
        self.__set_service_dict_from_class_name()
        self.argslist = [line.strip() for line in lines[1:]] if len(lines)>1 else []
        if self.interactive :
            msg = 'Running this reinstall would be like running the following from the command line:\n'
            msg+= f'InstallService {self.service_class_name} '
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
        not implemented in base class
        """
        pass

    def _write_install_args_file(self) :
        """
        Write out a file storing the arguments used to install the Service so it can be 
        reinstalled using the same configurations/setting if desired
        """
        if self.service_class_name is None or self.argslist is None :
            errmsg = "ERROR: can't write the installation arguments file without a class name and argslist!"
            self.logger.error(errmsg,RuntimeError)
        with open(self.install_args_filepath,'w') as fp :
            for arg in [self.service_class_name,*self.argslist] :
                fp.write(f'{arg}\n')
    
    def _write_executable_file(self,filepath=None) :
        """
        write out the executable python file that the service will actually be running
        """
        error_log_path = pathlib.Path().resolve()/f'{self.service_name}{SERVICE_CONST.ERROR_LOG_STEM}'
        code = f'''\
            if __name__=='__main__' :
                try :
                    from {self.service_dict['filepath']} import {self.service_dict['func_name']}
                    {self.service_dict['func_name']}({self.argslist})
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

    def __set_service_dict_from_class_name(self) :
        """
        If a service class name has been set, set the service dict with information about the code that should be run
        """
        if self.service_class_name is not None :
            #set the dictionary with details about the program that will run
            service_dict = [sd for sd in SERVICE_CONST.AVAILABLE_SERVICES if sd['script_name']==self.service_class_name]
            if len(service_dict)!=1 :
                errmsg = f'ERROR: could not find the Service dictionary for {self.service_name} '
                errmsg+= f'(a {self.service_class_name} program)! service_dict = {service_dict}'
                self.logger.error(errmsg,RuntimeError)
            self.service_dict = service_dict[0]
        else :
            self.service_dict = None
